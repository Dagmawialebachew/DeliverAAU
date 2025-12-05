import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List
from aiogram import Bot
from database.db import Database
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
# Assuming db is initialized externally and passed as a path or instance
log = logging.getLogger(__name__)

# --- Core DG Metrics ---


async def increment_skip(self, dg_id: int, max_skips: int = 3) -> int:
        """
        Increments the skip count for a delivery guy, updates last_skip_at,
        and returns the new skipped_requests count. Also records the skip in daily_stats.
        """
        import datetime
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        
        async with self._open_connection() as conn:
            # Note: We assume 'total_requests' incrementation is handled elsewhere
            # or should be handled here alongside 'accepted_requests'. 
            # Given the original included 'total_requests', we'll update both dg and daily_stats.

            # 1. Update delivery_guys table (also increments total_requests)
            await conn.execute(
                """
                UPDATE delivery_guys 
                SET skipped_requests = skipped_requests + 1, 
                    total_requests = total_requests + 1,
                    last_skip_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                dg_id
            )

            # 2. Update daily_stats table (UPSERT: increment skipped, assigned, and total requests)
            await conn.execute(
                """
                INSERT INTO daily_stats (dg_id, date, skipped, assigned)
                VALUES ($1, $2, 1, 1) -- Set both skipped and assigned to 1 for the first time
                ON CONFLICT(dg_id, date) DO UPDATE SET
                skipped = daily_stats.skipped + 1,
                assigned = daily_stats.assigned + 1,
                updated_at = CURRENT_TIMESTAMP
                """,
                dg_id, today_str
            )

            # 3. Fetch the new skip count
            new_skips = await conn.fetchval(
                "SELECT skipped_requests FROM delivery_guys WHERE id = $1", dg_id
            )
            return int(new_skips) if new_skips is not None else 0


async def reset_skips_daily(self) -> None:
        """Resets the skipped_requests count for all DGs to 0."""
        async with self._open_connection() as conn:
            # We use the existing method, which already uses Postgres:
            # The previous snippet only updated 'skipped_requests', 
            # but usually this task would reset the count for *all* DGs.
            await conn.execute("UPDATE delivery_guys SET skipped_requests = 0")


async def calc_acceptance_rate(self, dg_id: int) -> float:
    """
    Calculates the Delivery Guy's acceptance rate:
    (accepted_requests / total_requests) * 100
    
    - accepted_requests: how many offers the DG accepted
    - total_requests: how many offers were sent to the DG
    
    Returns a percentage between 0.0 and 100.0.
    If no requests exist, defaults to 100.0 (neutral baseline).
    """
    async with self._open_connection() as conn:
        row = await conn.fetchrow(
            "SELECT total_requests, accepted_requests FROM delivery_guys WHERE id = $1",
            dg_id
        )

        if not row:
            return 100.0  # DG not found → treat as neutral baseline

        total_requests = int(row["total_requests"] or 0)
        accepted_requests = int(row["accepted_requests"] or 0)

        if total_requests <= 0:
            return 100.0  # No requests yet → full acceptance by definition

        acceptance_rate = (accepted_requests / total_requests) * 100.0

        # Clamp to [0, 100] in case of data anomalies
        acceptance_rate = max(0.0, min(acceptance_rate, 100.0))

        return round(acceptance_rate, 2)

    # -------------------- Order Retrieval (Postgres/asyncpg) --------------------

async def get_all_active_orders_for_dg(self, dg_id: int) -> List[Dict[str, Any]]:
    """Fetches all non-delivered orders assigned to the DG."""
    async with self._open_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT * 
            FROM orders 
            WHERE delivery_guy_id = $1 
                AND status != 'delivered'
                AND status != 'cancelled'

            ORDER BY created_at DESC
            """,
            dg_id,
        )

        # Debug print
        print(f"[DEBUG] get_all_active_orders_for_dg for DG {dg_id}: {rows}")

        # Or use logging for better control
        import logging
        logging.getLogger(__name__).debug("Active orders for DG %s: %s", dg_id, rows)

        return [self._row_to_dict(r) for r in rows]

async def get_latest_active_order_for_dg(self, dg_id: int) -> Optional[Dict[str, Any]]:
        """Fetches the latest non-delivered order assigned to the DG."""
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE delivery_guy_id = $1 AND status != 'delivered' ORDER BY created_at DESC LIMIT 1",
                dg_id,
            )
            return self._row_to_dict(row) if row else None
# --- Daily Stats (Section 5) ---

async def record_daily_stat(self, dg_id: int, delivery_fee: float, delivered: int = 0) -> None:
        """
        Records or updates daily statistics for the DG using UPSERT.
        This records delivery completion (delivered count and earnings).
        """
        date_str = date.today().strftime('%Y-%m-%d')
        
        # We use PostgreSQL's UPSERT (INSERT ... ON CONFLICT) for atomicity
        async with self._open_connection() as conn:
            await conn.execute(
                """
                INSERT INTO daily_stats (dg_id, date, deliveries, earnings, updated_at)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                ON CONFLICT (dg_id, date) DO UPDATE SET
                    deliveries = daily_stats.deliveries + $3,
                    earnings = daily_stats.earnings + $4,
                    updated_at = CURRENT_TIMESTAMP
                """,
                dg_id, date_str, delivered, delivery_fee
            )
        # Note: Exception handling should wrap the calling code or rely on the class structure


async def check_thresholds_and_notify(
    self,
    bot: Bot,
    dg_id: int,
    admin_group_id: int,
    max_skips: int = 3
):
    async with self._open_connection() as conn:
        dg_info = await conn.fetchrow(
            "SELECT name, skipped_requests FROM delivery_guys WHERE id = $1",
            dg_id
        )
        if not dg_info:
            return

        name = dg_info["name"]
        skips = int(dg_info["skipped_requests"] or 0)

        if skips >= max_skips:
            admin_message = (
                f"🚨 **Reliability Alert!**\n"
                f"Delivery Partner **{name}** (ID: `{dg_id}`) has reached the maximum skip threshold ({max_skips} skips today).\n"
                f"**ACTION REQUIRED**: Review their performance and block if necessary."
            )
            await self.notify_admin(bot, admin_group_id, admin_message)

    # NOTE: The original `notify_admin` is a simple wrapper for bot.send_message 
    # and doesn't use the DB, so it should stay as a standalone or a helper method.
    # We include it here for completeness, though it doesn't strictly belong to DB ops.
async def notify_admin(self, bot: Any, admin_group_id: int, message: str):
        """Sends a message to the admin group (non-DB operation)."""
        # ... (implementation remains the same, assuming bot is passed in)
        pass # Placeholder: Implement sending logic here
        
        
async def get_student_chat_id(self, order: Dict[str, Any]) -> Optional[int]:
        """
        Resolve the student's Telegram chat_id from an order record.
        This uses the existing database method `get_user_by_id`.
        """
        user_id = order.get("user_id")
        if not user_id:
            return None

        user = await self.get_user_by_id(user_id) # Assumes this method exists and returns a user dict
        return user["telegram_id"] if user and user.get("telegram_id") is not None else None


logging

async def add_dg_to_blacklist(self, order_id: int, dg_id: int) -> None:
    """
    Add a delivery guy's internal ID to the order's rejection blacklist.
    Ensures breakdown_json is parsed from string to dict before manipulation.
    """
    async with self._open_connection() as conn:
        # 1. Fetch current breakdown_json
        row = await conn.fetchrow(
            "SELECT breakdown_json FROM orders WHERE id = $1",
            order_id
        )
        if not row:
            logging.warning("[BLACKLIST] Order %s not found when adding DG %s", order_id, dg_id)
            return

        breakdown_raw = row["breakdown_json"]

        # Parse JSON string into dict
        if isinstance(breakdown_raw, str):
            try:
                breakdown = json.loads(breakdown_raw)
            except Exception:
                logging.error("[BLACKLIST] Failed to parse breakdown_json for order %s", order_id)
                breakdown = {}
        elif isinstance(breakdown_raw, dict):
            breakdown = breakdown_raw
        else:
            breakdown = {}

        # 2. Update rejected list
        rejected = breakdown.get("rejected_by_dg_ids", [])
        if dg_id not in rejected:
            rejected.append(dg_id)
            breakdown["rejected_by_dg_ids"] = rejected

            # 3. Save back to DB
            await conn.execute(
                "UPDATE orders SET breakdown_json = $1 WHERE id = $2",
                json.dumps(breakdown), order_id
            )
            logging.info("[BLACKLIST] Added DG %s to order %s blacklist.", dg_id, order_id)
        else:
            logging.debug("[BLACKLIST] DG %s already in order %s blacklist", dg_id, order_id)
    # -------------------- Vendor Summaries (Postgres/asyncpg) --------------------
async def calc_vendor_day_summary(self, vendor_id: int, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns a daily summary dict for a vendor.
        Uses PostgreSQL's date and aggregate functions.
        """
        date_to_use = date.today()
        
        async with self._open_connection() as conn:
            # 1. Orders totals
            order_summary = await conn.fetchrow(
                """
                SELECT
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    COALESCE(SUM(CASE WHEN status = 'delivered' THEN food_subtotal ELSE 0 END), 0.0) AS food_revenue,                    
                    COALESCE(SUM(CASE WHEN status = 'delivered' THEN delivery_fee ELSE 0 END), 0.0) AS delivery_fees,
                    COALESCE(SUM(CASE WHEN status IN ('accepted','in_progress','delivered') THEN 1 ELSE 0 END), 0) AS progressed
                FROM orders
                WHERE vendor_id = $1 AND DATE(created_at) = $2
                """, 
                vendor_id, date_to_use
            )

            delivered = int(order_summary["delivered_count"])
            cancelled = int(order_summary["cancelled_count"])
            food_rev = float(order_summary["food_revenue"])
            delivery_fees = float(order_summary["delivery_fees"])
            progressed = int(order_summary["progressed"])

            # 2. Vendor ratings snapshot
            vendor_info = await conn.fetchrow(
                "SELECT name, rating_avg, rating_count FROM vendors WHERE id = $1", vendor_id
            )

            vendor_name = (vendor_info["name"] if vendor_info else "Unknown")
            rating_avg = float(vendor_info["rating_avg"] if vendor_info and vendor_info.get("rating_avg") is not None else 0.0)
            rating_count = int(vendor_info["rating_count"] if vendor_info and vendor_info.get("rating_count") is not None else 0)

            # 3. Reliability calc
            denom = progressed + cancelled
            reliability_pct = 0 if denom == 0 else round(100.0 * progressed / denom)

        return {
            "date": date_to_use,
            "vendor_name": vendor_name,
            "delivered": delivered,
            "cancelled": cancelled,
            "food_revenue": food_rev,
            "delivery_fees": delivery_fees,
            "total_payout": food_rev + delivery_fees,
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "reliability_pct": reliability_pct,
        }


async def calc_vendor_week_summary(self, vendor_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Weekly summary: totals and per-day breakdown across a provided range."""
        if not start_date or not end_date:
            today = date.today()
            start = today - timedelta(days=today.weekday())
            end = start + timedelta(days=6)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")

        async with self._open_connection() as conn:
            # 1. Total summary for the range
            total_summary = await conn.fetchrow(
                """
                SELECT
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    COALESCE(SUM(food_subtotal), 0.0) AS food_revenue,
                    COALESCE(SUM(delivery_fee), 0.0) AS delivery_fees
                FROM orders
                WHERE vendor_id = $1 AND DATE(created_at) BETWEEN $2 AND $3
                """, 
                vendor_id, start_date, end_date
            )

            delivered = int(total_summary["delivered_count"] or 0)
            cancelled = int(total_summary["cancelled_count"] or 0)
            food_rev = float(total_summary["food_revenue"] or 0.0)
            delivery_fees = float(total_summary["delivery_fees"] or 0.0)

            # 2. Daily breakdown for the range
            daily_rows = await conn.fetch(
                """
                SELECT DATE(created_at) AS d,
                        SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                        COALESCE(SUM(food_subtotal), 0.0) AS food_revenue,
                        COALESCE(SUM(delivery_fee), 0.0) AS delivery_fees
                FROM orders
                WHERE vendor_id = $1 AND DATE(created_at) BETWEEN $2 AND $3
                GROUP BY DATE(created_at)
                ORDER BY d ASC
                """, 
                vendor_id, start_date, end_date
            )

        days = []
        for r in daily_rows:
            day_food_rev = float(r["food_revenue"])
            day_delivery_fees = float(r["delivery_fees"])
            days.append({
                "date": r["d"].strftime("%Y-%m-%d"),
                "delivered": int(r["delivered_count"]),
                "cancelled": int(r["cancelled_count"]),
                "food_revenue": day_food_rev,
                "delivery_fees": day_delivery_fees,
                "total_payout": day_food_rev + day_delivery_fees,
            })

        return {
            "start_date": start_date,
            "end_date": end_date,
            "delivered": delivered,
            "cancelled": cancelled,
            "food_revenue": food_rev,
            "delivery_fees": delivery_fees,
            "total_payout": food_rev + delivery_fees,
            "days": days,
        }
        


# --- Telegram Notifications (Non-DB operations) ---
# NOTE: Assume 'Bot' and 'TelegramForbiddenError' etc. are imported correctly.

# The original `notify_admin` is now an internal method of Database or remains a standalone helper.
# async def notify_admin(bot, admin_group_id: int, message: str): ... 

async def notify_student_prepared(bot: Bot, chat_id: int, vendor_name: str, order_id: int) -> None:
    await bot.send_message(
        chat_id,
        f"🍴 Your order #{order_id} has been prepared by {vendor_name}.\n🚴 A delivery partner will pick it up soon."
    )

async def notify_student_cancelled(bot: Bot, chat_id: int, order_id: int) -> None:
    await bot.send_message(
        chat_id,
        f"⚠️ Sorry, your order #{order_id} is unavailable.\nPlease choose another menu option."
    )

async def notify_dg_pickup_ready(
    bot: Bot,
    chat_id: int,
    vendor_name: str,
    order_id: int,
    pickup: str
) -> None:
    text = f"🚴 Order #{order_id} is ready for pickup at {vendor_name}.\n📍 Pickup: {pickup}"
    try:
        await bot.send_message(chat_id, text)
        # log.info("Sent pickup ready notification to DG chat_id=%s for order %s", chat_id, order_id)
    except Exception:
        # log.exception("Unexpected error notifying DG %s for order %s", chat_id, order_id)
        pass # Handle Telegram exceptions

async def notify_dg_cancelled(bot: Bot, chat_id: int, order_id: int) -> None:
    await bot.send_message(
        chat_id,
        f"⚠️ Order #{order_id} has been cancelled by the vendor."
    )

async def notify_admin_log(bot: Bot, admin_group_id: int, text: str) -> None:
    await bot.send_message(admin_group_id, text)

# --- Formatting Functions ---

def format_admin_daily_summary(summary: Dict[str, Any]) -> str:
    return (
        f"📊 Daily Summary — {summary.get('vendor_name','Vendor')} ({summary['date']})\n\n"
        f"📦 Orders: {summary['delivered'] + summary['cancelled']} "
        f"(Prepared/Delivered {summary['delivered']} | Cancelled {summary['cancelled']})\n"
        f"💵 Food Revenue: {int(summary['food_revenue'])} birr\n"
        f"🚚 Delivery Fees: {int(summary['delivery_fees'])} birr\n"
        f"💰 Total Payout: {int(summary['total_payout'])} birr\n\n"
        f"⭐ Average Rating: {summary['rating_avg']:.1f} ({summary['rating_count']} reviews)\n"
        f"⚡ Reliability: {summary['reliability_pct']}%"
    )

def format_vendor_daily_summary_amharic(summary: Dict[str, Any]) -> str:
    return (
        f"📊 የዕለቱ ሪፖርት — {summary.get('vendor_name','ሱቅ')}\n\n"
        f"📦 ትዕዛዞች: {summary['delivered'] + summary['cancelled']} (✅ {summary['delivered']} | ❌ {summary['cancelled']})\n"
        f"💵 ገቢ: {int(summary['food_revenue'] + summary['delivery_fees'])} ብር\n"
        f"⭐ አማካይ ደረጃ: {summary['rating_avg']:.1f} ({summary['rating_count']} አስተያየት)\n"
        f"⚡ ታማኝነት: {summary['reliability_pct']}%"
    )