import json
import aiosqlite
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from aiogram import Bot
from database.db import Database
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
# Assuming db is initialized externally and passed as a path or instance
log = logging.getLogger(__name__)

# --- Core DG Metrics ---


async def increment_skip(db_path: str, dg_id: int, max_skips: int = 3) -> int:
    """Increments the skip count for a delivery guy and returns the new count."""
    try:
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET skipped_requests = skipped_requests + 1, total_requests = total_requests + 1 WHERE id = ?",
                (dg_id,)
            )
            await conn.commit()
            
            async with conn.execute("SELECT skipped_requests FROM delivery_guys WHERE id = ?", (dg_id,)) as cur:
                result = await cur.fetchone()
                new_skips = result[0] if result else 0
                log.info("DG %s incremented skip count to %s", dg_id, new_skips)
                return new_skips
    except Exception:
        log.exception("Failed to increment skip for DG %s", dg_id)
        return -1


async def reset_skips_daily(db_path: str):
    """Resets the skipped_requests count for all DGs to 0."""
    try:
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("UPDATE delivery_guys SET skipped_requests = 0")
            await conn.commit()
            log.info("Daily skip counts reset for all delivery guys.")
    except Exception:
        log.exception("Failed to reset daily skips")

async def calc_acceptance_rate(db_path: str, dg_id: int) -> float:
    """Calculates the DG's acceptance rate (accepted / total requests)."""
    try:
        async with aiosqlite.connect(db_path) as conn:
            async with conn.execute(
                "SELECT total_requests, total_deliveries FROM delivery_guys WHERE id = ?",
                (dg_id,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    total_requests, total_deliveries = row
                    # Total requests includes skips and accepted (delivered)
                    if total_requests == 0:
                        return 100.0 # Perfect rate if no requests yet
                    
                    acceptance_rate = (total_deliveries / total_requests) * 100
                    return round(acceptance_rate, 2)
                return 100.0
    except Exception:
        log.exception("Failed to calculate acceptance rate for DG %s", dg_id)
        return 0.0

# --- Order Retrieval ---

async def get_all_active_orders_for_dg(db_path: str, dg_id: int) -> List[Dict[str, Any]]:
    """Fetches all non-delivered orders assigned to the DG."""
    try:
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM orders WHERE delivery_guy_id = ? AND status != 'delivered' ORDER BY created_at DESC",
                (dg_id,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]
    except Exception:
        log.exception("Failed fetching active orders for dg %s", dg_id)
        return []

async def get_latest_active_order_for_dg(db_path: str, dg_id: int) -> Optional[Dict[str, Any]]:
    """Fetches the latest non-delivered order assigned to the DG."""
    try:
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM orders WHERE delivery_guy_id = ? AND status != 'delivered' ORDER BY created_at DESC LIMIT 1",
                (dg_id,),
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
    except Exception:
        log.exception("Failed fetching latest active order for dg %s", dg_id)
        return None

# --- Daily Stats (Section 5) ---

async def record_daily_stat(db_path: str, dg_id: int, delivery_fee: float, delivered: int = 0) -> None:
    """Records or updates daily statistics for the DG."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    try:
        async with aiosqlite.connect(db_path) as conn:
            # Check if record exists for today
            async with conn.execute(
                "SELECT * FROM daily_stats WHERE dg_id = ? AND date = ?", 
                (dg_id, date_str)
            ) as cur:
                exists = await cur.fetchone()

            if exists:
                # Update existing record
                await conn.execute(
                    """
                    UPDATE daily_stats SET 
                        deliveries = deliveries + ?,
                        earnings = earnings + ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE dg_id = ? AND date = ?
                    """,
                    (delivered, delivery_fee, dg_id, date_str)
                )
            else:
                # Insert new record
                await conn.execute(
                    """
                    INSERT INTO daily_stats (dg_id, date, deliveries, earnings)
                    VALUES (?, ?, ?, ?)
                    """,
                    (dg_id, date_str, delivered, delivery_fee)
                )
            await conn.commit()
            log.info("Daily stat recorded for DG %s on %s", dg_id, date_str)
    except Exception:
        log.exception("Failed to record daily stat for DG %s", dg_id)


# --- Admin Notifications / Thresholds (Section 2) ---

async def notify_admin(bot, admin_group_id: int, message: str):
    """Sends a message to the admin group."""
    try:
        await bot.send_message(admin_group_id, message, parse_mode="Markdown")
    except Exception:
        log.exception("Failed to notify admin group %s", admin_group_id)

async def check_thresholds_and_notify(bot, db_path: str, dg_id: int, admin_group_id: int, max_skips: int = 3):
    """
    Checks if the DG hit the skip threshold and notifies the admin.
    NOTE: Blocking/unblocking logic is handled by the admin post-notification.
    """
    try:
        async with aiosqlite.connect(db_path) as conn:
            async with conn.execute("SELECT name, skipped_requests FROM delivery_guys WHERE id = ?", (dg_id,)) as cur:
                dg_info = await cur.fetchone()
                if not dg_info:
                    return

                name, skips = dg_info
                
                if skips >= max_skips:
                    # Notify admin
                    admin_message = (
                        f"🚨 **Reliability Alert!**\n"
                        f"Delivery Partner **{name}** (ID: `{dg_id}`) has reached the maximum skip threshold ({max_skips} skips today).\n"
                        f"**ACTION REQUIRED**: Review their performance and block if necessary."
                    )
                    await notify_admin(bot, admin_group_id, admin_message)
                
    except Exception:
        log.exception("Error checking skip thresholds for DG %s", dg_id)



async def get_student_chat_id(db: Database, order: Dict[str, Any]) -> Optional[int]:
    """
    Resolve the student's Telegram chat_id from an order record.
    - order["user_id"] is the internal DB id of the user.
    - This helper fetches the user row and returns user["telegram_id"].
    """
    user_id = order.get("user_id")
    if not user_id:
        return None

    # db must be an instance of Database, not the module
    user = await db.get_user_by_id(user_id)
    return user["telegram_id"] if user else None



async def add_dg_to_blacklist(db_path: str, order_id: int, dg_id: int) -> None:
    """
    Add a delivery guy's internal ID to the order's rejection blacklist.
    Ensures consistency across skip, expiry, and block flows.
    """
    async with aiosqlite.connect(db_path) as conn:
        async with conn.execute("SELECT breakdown_json FROM orders WHERE id = ?", (order_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            log.warning("[BLACKLIST] Order %s not found when adding DG %s", order_id, dg_id)
            return

        breakdown = json.loads(row[0] or "{}")
        rejected = breakdown.get("rejected_by_dg_ids", [])
        log.debug("[DEBUG] Blacklist before update for order %s: %s", order_id, rejected)

        if dg_id not in rejected:
            rejected.append(dg_id)
            breakdown["rejected_by_dg_ids"] = rejected
            await conn.execute(
                "UPDATE orders SET breakdown_json = ? WHERE id = ?",
                (json.dumps(breakdown), order_id)
            )
            await conn.commit()
            log.info("[BLACKLIST] Added DG %s to order %s blacklist. Now: %s", dg_id, order_id, rejected)
        else:
            log.debug("[BLACKLIST] DG %s already in order %s blacklist", dg_id, order_id)
            
            





async def calc_vendor_day_summary(db_path: str, vendor_id: int, date: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a daily summary dict for a vendor: delivered/cancelled counts,
    food revenue, delivery fees, total payout, ratings, reliability %.
    """
    date = date or datetime.date.today().strftime("%Y-%m-%d")
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        # Orders totals
        async with conn.execute(
            """
            SELECT
              SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
              SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
              SUM(food_subtotal) AS food_revenue,
              SUM(delivery_fee) AS delivery_fees
            FROM orders
            WHERE vendor_id = ? AND DATE(created_at) = ?
            """, (vendor_id, date)
        ) as cur:
            s = await cur.fetchone()
        delivered = int((s["delivered_count"] or 0))
        cancelled = int((s["cancelled_count"] or 0))
        food_rev = float((s["food_revenue"] or 0.0))
        delivery_fees = float((s["delivery_fees"] or 0.0))

        # Vendor ratings snapshot
        async with conn.execute("SELECT name, rating_avg, rating_count FROM vendors WHERE id = ?", (vendor_id,)) as cur:
            v = await cur.fetchone()
        vendor_name = (v["name"] if v else "Unknown")
        rating_avg = float((v["rating_avg"] if v else 0.0))
        rating_count = int((v["rating_count"] if v else 0))

        # Reliability calc (progressed vs cancelled)
        async with conn.execute(
            """
            SELECT
              SUM(CASE WHEN status IN ('accepted','in_progress','delivered') THEN 1 ELSE 0 END) AS progressed,
              SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
            FROM orders
            WHERE vendor_id = ? AND DATE(created_at) = ?
            """, (vendor_id, date)
        ) as cur:
            r = await cur.fetchone()
        progressed = int((r["progressed"] or 0))
        cancelled2 = int((r["cancelled"] or 0))
        denom = progressed + cancelled2
        reliability_pct = 0 if denom == 0 else round(100.0 * progressed / denom)

    return {
        "date": date,
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


async def calc_vendor_week_summary(db_path: str, vendor_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Weekly summary: totals and per-day breakdown across an ISO week or provided range.
    """
    if not start_date or not end_date:
        today = datetime.date.today()
        start = today - datetime.timedelta(days=today.weekday())
        end = start + datetime.timedelta(days=6)
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")

    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row

        async with conn.execute(
            """
            SELECT
              SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
              SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
              SUM(food_subtotal) AS food_revenue,
              SUM(delivery_fee) AS delivery_fees
            FROM orders
            WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?
            """, (vendor_id, start_date, end_date)
        ) as cur:
            t = await cur.fetchone()

        delivered = int((t["delivered_count"] or 0))
        cancelled = int((t["cancelled_count"] or 0))
        food_rev = float((t["food_revenue"] or 0.0))
        delivery_fees = float((t["delivery_fees"] or 0.0))

        async with conn.execute(
            """
            SELECT DATE(created_at) AS d,
                   SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                   SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                   SUM(food_subtotal) AS food_revenue,
                   SUM(delivery_fee) AS delivery_fees
            FROM orders
            WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?
            GROUP BY DATE(created_at)
            ORDER BY d ASC
            """, (vendor_id, start_date, end_date)
        ) as cur:
            rows = await cur.fetchall()

        days = [{
            "date": r["d"],
            "delivered": int((r["delivered_count"] or 0)),
            "cancelled": int((r["cancelled_count"] or 0)),
            "food_revenue": float((r["food_revenue"] or 0.0)),
            "delivery_fees": float((r["delivery_fees"] or 0.0)),
            "total_payout": float((r["food_revenue"] or 0.0)) + float((r["delivery_fees"] or 0.0)),
        } for r in rows]

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
        log.info("Sent pickup ready notification to DG chat_id=%s for order %s", chat_id, order_id)
    except TelegramForbiddenError:
        log.warning("Cannot notify DG %s for order %s: bot blocked or chat not available", chat_id, order_id)
    except TelegramBadRequest as e:
        log.error("Bad request sending to DG %s for order %s: %s", chat_id, order_id, e)
    except Exception:
        log.exception("Unexpected error notifying DG %s for order %s", chat_id, order_id)

async def notify_dg_cancelled(bot: Bot, chat_id: int, order_id: int) -> None:
    await bot.send_message(
        chat_id,
        f"⚠️ Order #{order_id} has been cancelled by the vendor."
    )

async def notify_admin_log(bot: Bot, admin_group_id: int, text: str) -> None:
    await bot.send_message(admin_group_id, text)
    



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