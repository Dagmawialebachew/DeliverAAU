import logging
import json 
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError 
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery,
)
from database.db import AnalyticsService, Database
from config import settings
from handlers.delivery_guy import _db_get_delivery_guy_by_user
from utils.db_helpers import calc_acceptance_rate, reset_skips_daily, add_dg_to_blacklist
from utils.globals import PENDING_OFFERS # IMPORTANT: Ensure utils/globals.py exists
from utils.vendor_scheduler import VendorJobs
ADMIN_IDS = settings.ADMIN_IDS
ADMIN_GROUP_ID = settings.ADMIN_SUMMRY_GROUP_ID
log = logging.getLogger(__name__)

    
def go_online_keyboard(ticket_id: str = None):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🟢 Go Online", callback_data="dg:go_online")]
        ])
class BotScheduler:
    """Bot background job scheduler."""

    def __init__(self, db: Database, bot: Bot):
        """
        Initialize scheduler.
        """
        self.db = db
        self.bot = bot
        self.scheduler = AsyncIOScheduler()

    # -----------------------------------------------
    # 🆕 NEW: Live Offer Countdown Job (Phase 2)
    # -----------------------------------------------
    async def update_order_offers(self) -> None:
        """
        Periodically updates pending DG offers:
        - Updates countdown on active offers
        - Expires offers → resets order.delivery_guy_id and status to 'pending'
        - Blacklists rejecting/blocked DG
        - Re-offers to next candidate
        - Notifies admin group with clear attribution (order + DG info)
        """
        offers_to_check = list(PENDING_OFFERS.items())
        log.debug("[DEBUG] Checking %d pending offers", len(offers_to_check))

        for order_id, offer in offers_to_check:
            chat_id = offer["chat_id"]
            message_id = offer["message_id"]
            assigned_at = offer["assigned_at"]
            expiry_seconds = offer["expiry_seconds"]

            elapsed_time = (datetime.now() - assigned_at).total_seconds()
            remaining_seconds = max(0, int(expiry_seconds - elapsed_time))
            minutes, seconds = divmod(remaining_seconds, 60)
            countdown = f"{minutes:02d}:{seconds:02d}"

            log.debug("[DEBUG] Order %s for DG %s: remaining=%s (%s)", order_id, chat_id, remaining_seconds, countdown)

            if remaining_seconds > 120:
                icon = "⏳"
            elif remaining_seconds > 30:
                icon = "⚠️"
            else:
                icon = "❌"

            from handlers.delivery_guy import order_offer_keyboard, send_new_order_offer
            from utils.helpers import find_next_candidate
            from utils.db_helpers import  notify_admin_log# ensure import exists
            from utils.db_helpers import add_dg_to_blacklist
               # ensure import exists

            async def _admin_notify(event: str, dg: dict | None, extra: str = ""):
                # Minimal, resilient admin notifier with DG context
                if not settings.ADMIN_DAILY_GROUP_ID:
                    return
                dg_name = dg.get("name") if dg else "Unknown DG"
                dg_id = dg.get("id") if dg else "N/A"
                dg_campus = dg.get("campus") if dg else "N/A"
                dg_user = dg.get("telegram_id") if dg else chat_id
                msg = (
                    f"{event}\n"
                    f"📦 Order #{order_id}\n"
                    f"🚴 DG: {dg_name} (id={dg_id}, campus={dg_campus}, chat_id={dg_user})\n"
                    f"{extra}".strip()
                )
                try:
                    await notify_admin_log(self.bot, settings.ADMIN_DAILY_GROUP_ID, msg, parse_mode="HTML")
                except Exception:
                    log.exception("[ADMIN LOG] Failed to send admin notification for order %s", order_id)

            try:
                # --- Offer expired ---
                if remaining_seconds <= 0:
                    log.warning("Order offer %s expired for DG %s.", order_id, chat_id)

                    expired_text = (
                        "⏰ **Offer Expired!**\n\n"
                        f"📦 Order #{order_id} has been automatically returned to the pool."
                    )
                    # Update the offer message to expired
                    try:
                        await self.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=expired_text,
                            reply_markup=None,
                            parse_mode="Markdown"
                        )
                    except Exception:
                        log.debug("Failed to edit expired message for order %s to DG chat %s", order_id, chat_id)

                    # Clear DB assignment and mark pending (do NOT rely on status for DG availability logic)
                    async with self.db._open_connection() as conn:
                            await conn.execute(
                                """
                                UPDATE orders
                                SET delivery_guy_id = $1,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = $2
                                """,
                                None, order_id
                            )


                    # Persist rejection using DG internal ID (blacklist)
                    dg = await _db_get_delivery_guy_by_user(chat_id)
                    if dg:
                        try:
                            await add_dg_to_blacklist(self.db, order_id, dg["id"])
                        except Exception:
                            log.exception("[BLACKLIST] Failed to add DG %s to blacklist for order %s", dg["id"], order_id)

                    # Stop tracking this offer
                    PENDING_OFFERS.pop(order_id, None)

                    # Admin notification (expired)
                    await _admin_notify(
                        event="⏰ Offer expired (ignored until timeout)",
                        dg=dg,
                        extra=f"🕒 Last countdown seen: {countdown}\n↩️ Returned to pool."
                    )

                    # Re-offer to next candidate (do not assign)
                    order = await self.db.get_order(order_id)
                    if order:
                        log.debug("[REOFFER] Finding next candidate for order %s", order_id)
                        next_dg = await find_next_candidate(self.db, order_id, order)
                        if next_dg:
                            try:
                                await send_new_order_offer(self.bot, next_dg, order)
                                log.info("[REOFFER] Offered order %s to DG %s", order_id, next_dg["id"])
                                # Admin notification (re-offered)
                                await _admin_notify(
                                    event="🔁 Re-offered to next candidate",
                                    dg=next_dg,
                                    extra="Awaiting DG response."
                                )
                            except Exception:
                                log.exception("[REOFFER] Failed to send offer for order %s to DG %s", order_id, next_dg.get("id"))
                                await _admin_notify(
                                    event="❗ Re-offer failed",
                                    dg=next_dg,
                                    extra="Check logs for details."
                                )
                        else:
                            log.warning("[REOFFER] No eligible DG found for order %s", order_id)
                            await _admin_notify(
                                event="⚠️ No eligible DG found",
                                dg=None,
                                extra="Manual admin assignment may be required."
                            )

                # --- Offer still active: update countdown display ---
                else:
                    if countdown == offer.get("last_countdown"):
                        continue
                    offer["last_countdown"] = countdown

                    order = await self.db.get_order(order_id)
                    if not order:
                        log.debug("[DEBUG] Order %s not found in DB, removing from tracker", order_id)
                        PENDING_OFFERS.pop(order_id, None)
                        await _admin_notify(
                            event="⚠️ Offer tracking stopped (order missing)",
                            dg=None,
                            extra="Order not found in DB."
                        )
                        continue

                    pickup_loc = order.get("pickup")
                    dropoff_loc = order.get("dropoff")
                    delivery_fee = order.get("delivery_fee", 0.0)

                    breakdown = json.loads(order.get("breakdown_json") or "{}")
                    drop_lat = breakdown.get("drop_lat")
                    drop_lon = breakdown.get("drop_lon")
                    dropoff_display = (
                        f"Live location ({drop_lat:.6f},{drop_lon:.6f})"
                        if drop_lat and drop_lon else dropoff_loc
                    )

                    new_message_text = (
                        "🚴‍♂️ **New Order Incoming!**\n\n"
                        f"📍 **Pickup**: {pickup_loc}\n"
                        f"🏠 **Drop-off**: {dropoff_display}\n"
                        f"💰 **Delivery Fee**: {int(delivery_fee)} birr\n"
                        f"{icon} **Expires in**: {countdown}\n"
                    )

                    kb = order_offer_keyboard(order_id, expiry_min=expiry_seconds // 60)

                    try:
                        await self.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=new_message_text,
                            reply_markup=kb,
                            parse_mode="Markdown"
                        )
                        log.debug("[DEBUG] Countdown updated for order %s", order_id)
                    except TelegramBadRequest as e:
                        if "message is not modified" not in str(e):
                            log.warning("Offer message for order %s failed to edit: %s", order_id, e)
                            await _admin_notify(
                                event="❗ Failed to edit DG offer message",
                                dg=None,
                                extra=str(e)
                            )
                        PENDING_OFFERS.pop(order_id, None)

            except TelegramForbiddenError:
                log.warning("DG %s blocked the bot. Removing offer %s.", chat_id, order_id)
                PENDING_OFFERS.pop(order_id, None)

                # Persist rejection using DG internal ID
                dg = await _db_get_delivery_guy_by_user(chat_id)
                if dg:
                    try:
                        await add_dg_to_blacklist(self.db, order_id, dg["id"])
                    except Exception:
                        log.exception("[BLACKLIST] Failed to add DG %s to blacklist for order %s", dg["id"], order_id)

                # Admin notification (blocked)
                await _admin_notify(
                    event="🚫 DG blocked the bot",
                    dg=dg,
                    extra="Order returned to pool."
                )
                await self.db.increment_skip(dg["id"])
                # Re-offer to next candidate
                order = await self.db.get_order(order_id)
                if order:
                    next_dg = await find_next_candidate(self.db, order_id, order)
                    if next_dg:
                        try:
                            await send_new_order_offer(self.bot, next_dg, order)
                            log.info("[REOFFER] Offered order %s to DG %s after block", order_id, next_dg["id"])
                            await _admin_notify(
                                event="🔁 Re-offered to next candidate",
                                dg=next_dg,
                                extra="Awaiting DG response."
                            )
                        except Exception:
                            log.exception("[REOFFER] Failed to send offer for order %s to DG %s", order_id, next_dg.get("id"))
                            await _admin_notify(
                                event="❗ Re-offer failed after block",
                                dg=next_dg,
                                extra="Check logs for details."
                            )

            except Exception:
                log.exception("Unexpected error updating offer %s", order_id)
                PENDING_OFFERS.pop(order_id, None)
                await _admin_notify(
                    event="❗ Unexpected error in offer update",
                    dg=None,
                    extra="Offer tracking stopped for this order."
                )

    #----------------------------------
    # 🆕 NEW: Core System Jobs (Section 7)
    # -----------------------------------------------
    
        
        
    async def expire_stale_orders(self):
        """Auto-cancel orders that expired without vendor acceptance."""
        try:
            async with self.db._open_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, user_id, vendor_id
                    FROM orders
                    WHERE status = 'pending'
                    AND expires_at IS NOT NULL
                    AND expires_at < NOW()
                    """
                )

            for r in rows:
                order_id = r["id"]

                # Cancel order
                async with self.db._open_connection() as conn:
                    await conn.execute(
                        """
                        UPDATE orders
                        SET status = 'cancelled',
                            cancel_reason = 'expired_vendor_no_accept',
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        order_id
                    )

                # Notify student
                student = await self.db.get_user_by_id(r["user_id"])
                if student and student.get("telegram_id"):
                    try:
                        await self.bot.send_message(
                            student["telegram_id"],
                            f"❌ **Order {order_id} Expired**\n"
                            "──────────────────────\n"
                            "⏱ Vendor did not accept in time.\n"
                            "💵 No charges applied.\n"
                            "✨ Please place a new order."
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass

                # Notify vendor
                vendor = await self.db.get_vendor(r["vendor_id"])
                if vendor and vendor.get("telegram_id"):
                    try:
                        await self.bot.send_message(
                            vendor["telegram_id"],
                            f"⚠️ ትዕዛዝ #{order_id} በ 45 ደቂቃ ውስጥ ተቀባይነት ሳላላገኘ ከትእዛዞች ተወግዷል።\n"
                            "እባክዎ ትዕዛዞችን በፍጥነት ይቀበሉ ወይም ይሰርዙ።"
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass

            if rows:
                log.info("Expired %d stale orders", len(rows))

        except Exception as e:
            log.exception("Failed to expire stale orders: %s", e)
            
    async def reset_delivery_guys_and_send_summary(self) -> None:
        """
        Daily job that:
        - Archives yesterday's daily_stats into daily_stats_archive and clears them
        - Resets all delivery_guys to offline and stamps last_offline_at
        - Clears in-memory pending offers to avoid ghost offers overnight
        - Sends each DG a daily recap enriched with active_orders_count and acceptance_rate
        - Sends an admin report with top performers, low-acceptance alerts, vendor cancels, and engagement metrics
        """
        log.info("Running daily DG reset + summary task")
        try:
            from datetime import date, timedelta
            today = date.today()
            yesterday = today - timedelta(days=1)
            today_str = today.strftime("%Y-%m-%d")
            yesterday_str = yesterday.strftime("%Y-%m-%d")

            async with self.db._pool.acquire() as conn:
                # 1) Archive yesterday's daily_stats and clear them (idempotent)
                try:
                    await conn.execute(
                        """
                        INSERT INTO daily_stats (dg_id, date, deliveries, earnings, skipped, assigned, acceptance_rate)
                        SELECT dg_id, date, deliveries, earnings, skipped, assigned, acceptance_rate
                        FROM daily_stats
                        WHERE date = $1
                        ON CONFLICT (dg_id, date) DO UPDATE
                        SET deliveries = EXCLUDED.deliveries,
                            earnings = EXCLUDED.earnings,
                            skipped = EXCLUDED.skipped,
                            assigned = EXCLUDED.assigned,
                            acceptance_rate = EXCLUDED.acceptance_rate,
                            updated_at = CURRENT_TIMESTAMP;                        
                        """,
                        yesterday_str
                    )
                    await conn.execute("DELETE FROM daily_stats WHERE date = $1", yesterday_str)
                    log.debug("Archived and cleared daily_stats for %s", yesterday_str)
                except Exception:
                    log.exception("Failed to archive/clear yesterday's daily_stats")

                # 2) Reset all DGs to offline and stamp last_offline_at
                try:
                    await conn.execute("UPDATE delivery_guys SET active = FALSE, last_offline_at = NOW()")
                    log.info("All delivery guys set to offline")
                except Exception:
                    log.exception("Failed to reset delivery_guys active flags")

                # 3) Clear in-memory pending offers
                try:
                    PENDING_OFFERS.clear()
                    log.debug("Cleared PENDING_OFFERS")
                except Exception:
                    log.exception("Failed to clear PENDING_OFFERS")

                # 4) Fetch DG list to message them
                try:
                    dgs = await conn.fetch("SELECT id, telegram_id, name FROM delivery_guys")
                except Exception:
                    log.exception("Failed to fetch delivery_guys")
                    dgs = []

                total_dgs = len(dgs)
                sent_count = 0
                failed_count = 0
                failed_ids: list[int] = []

                # safe_send helper
                async def safe_send(bot, chat_id, text, reply_markup=None, parse_mode="Markdown"):
                    try:
                        await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
                        return True
                    except Exception as e:
                        log.warning("safe_send failed for chat %s: %s", chat_id, e)
                        return False

                # 5) Send per-DG summaries
                for row in dgs:
                    dg_id = row["id"]
                    tg_id = row["telegram_id"]
                    dg_name = row.get("name") or f"DG #{dg_id}"

                    # Fetch today's stats (fallback to zeros)
                    try:
                        stats = await self.db.get_daily_stats_for_dg(dg_id, today)
                    except Exception:
                        log.exception("Failed to fetch daily stats for DG %s", dg_id)
                        stats = {"deliveries": 0, "earnings": 0.0, "xp": 0, "coins": 0.0}

                    # Enrich with active orders count
                    try:
                        active_orders = await self.db.get_active_orders_for_dg(dg_id)
                        active_count = len(active_orders)
                    except Exception:
                        log.exception("Failed to compute active orders for DG %s", dg_id)
                        active_count = 0

                    # Acceptance rate
                    try:
                        acceptance_rate = await calc_acceptance_rate(self.db, dg_id)
                    except Exception:
                        log.exception("Failed to compute acceptance rate for DG %s", dg_id)
                        acceptance_rate = 100.0

                    # Build message
                    text = (
                        f"🔥 **Your DAILY RECAP — {today_str}** 🔥\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🚀 **Today’s Performance**\n"
                        f"   • 🚚 Deliveries: **{int(stats.get('deliveries', 0))}**\n"
                        f"   • 💵 Earnings: **{int(stats.get('earnings', 0))} birr**\n"
                        f"   • 🎁 Rewards: **+{int(stats.get('xp', 0))} XP** • **+{float(stats.get('coins', 0.0)):.2f} Coins**\n\n"
                        f"📊 **Workload**: Active Orders: **{active_count}** • Acceptance: **{acceptance_rate:.1f}%**\n\n"
                        f"🧊 **Status:** `OFFLINE`\n"
                        "You are offline and will not receive orders. Use the button below to go online.\n"
                    )

                    try:
                        ok = await safe_send(self.bot, tg_id, text, reply_markup=go_online_keyboard(), parse_mode="Markdown")
                        if ok:
                            sent_count += 1
                            log.info("Sent daily recap to DG %s (%s)", dg_id, dg_name)
                        else:
                            failed_count += 1
                            failed_ids.append(dg_id)
                    except Exception:
                        log.exception("Unexpected error sending recap to DG %s", dg_id)
                        failed_count += 1
                        failed_ids.append(dg_id)

                # 6) Build admin report

                # Top 3 drivers (today)
                try:
                    top_drivers = await self.db.get_top_drivers(today)
                except Exception:
                    log.exception("Failed to fetch top drivers")
                    top_drivers = []

                

                # Low acceptance alerts (scan drivers with stats today)
                driver_alerts = []
                try:
                    low_accept_rows = await conn.fetch(
                        """
                        SELECT DISTINCT ds.dg_id, dg.name
                        FROM daily_stats ds
                        LEFT JOIN delivery_guys dg ON dg.id = ds.dg_id
                        WHERE ds.date = $1
                        """,
                        today_str
                    )
                    for r in low_accept_rows:
                        try:
                            rate = await calc_acceptance_rate(self.db, r["dg_id"])
                        except Exception:
                            rate = 100.0
                        if rate < 80.0:  # alert threshold
                            dg_id = r["dg_id"]
                            name = r["name"] or f"DG #{dg_id}"
                            driver_alerts.append(f"⚠️ {name} • {rate:.1f}% acceptance")
                except Exception:
                    log.exception("Failed to compute driver alerts")

                # Vendor cancels
                vendor_alerts = []
                try:
                    vendor_cancel_rows = await conn.fetch(
                        """
                        SELECT v.id AS vendor_id, v.name AS vendor_name, COUNT(*) AS cancels
                        FROM orders o
                        JOIN vendors v ON o.vendor_id = v.id
                        WHERE o.status = 'cancelled' AND o.created_at::DATE = $1
                        GROUP BY v.id, v.name
                        HAVING COUNT(*) > 0
                        ORDER BY cancels DESC
                        LIMIT 5
                        """,
                        today
                    )
                    vendor_alerts = [
                        f"- {r['vendor_name']} • {int(r['cancels'])} cancels"
                        for r in vendor_cancel_rows
                    ]
                except Exception:
                    log.exception("Failed to fetch vendor cancels")

                # Engagement metric: drivers who came online in last 2 hours
                try:
                    reactivated_count = int(await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM delivery_guys
                        WHERE last_online_at IS NOT NULL
                        AND last_online_at > (NOW() - INTERVAL '2 hours')
                        """
                    ) or 0)
                except Exception:
                    log.exception("Failed to compute engagement metric")
                    reactivated_count = 0

                failed_ids_str = f" (IDs: {', '.join(map(str, failed_ids))})" if failed_ids else ""

                admin_lines = [
                    "━━━━━━━━━━━━━━━━━━━━━━",
                    f"📢 **DELIVERY OPERATIONS DASHBOARD — {today_str}**",
                    "━━━━━━━━━━━━━━━━━━━━━━",
                    f"👥 DGs Reset: **{total_dgs}**",
                    f"📤 Summaries Sent: **{sent_count}**",
                    f"⚠️ Failed: **{failed_count}**{failed_ids_str}",
                    "",
                    "🏆 **TOP PERFORMERS**:"
                ]
                print('here is top_drivers', top_drivers)

                if top_drivers:
                    for idx, td in enumerate(top_drivers, 1):
                        admin_lines.append(f"{idx}. **{td['name']}** — 🚚 {td['deliveries']} • 💵 {td['earnings']} birr")
                else:
                    admin_lines.append("No top performers today.")

                admin_lines.append("")
                admin_lines.append("🚨 **ALERTS**")
                admin_lines.extend(driver_alerts or ["- No driver alerts"])
                admin_lines.extend(vendor_alerts or ["- No vendor alerts"])
                admin_lines.append("")
                admin_lines.append("📈 **ENGAGEMENT METRIC**")
                admin_lines.append(f"⚡ {reactivated_count} drivers bounced back online within 2 hours.")
                admin_text = "\n".join(admin_lines)

                # Send admin report
                try:
                    await self.bot.send_message(ADMIN_GROUP_ID, admin_text, parse_mode="Markdown")
                    log.info("Sent admin daily report")
                except Exception:
                    log.exception("Failed to send admin report")

        except Exception:
            log.exception("Error in DG reset + summary task")

    async def expire_unpicked_ready_orders(self):
        """Auto-cancel orders that were ready but not picked up within 3 hours."""
        try:
            async with self.db._open_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, user_id, vendor_id
                    FROM orders
                    WHERE status = 'ready'
                    AND ready_at IS NOT NULL
                    AND ready_at < NOW() - INTERVAL '3 hours'
                    """
                )

            for r in rows:
                order_id = r["id"]

                # Cancel order
                async with self.db._open_connection() as conn:
                    await conn.execute(
                        """
                        UPDATE orders
                        SET status = 'cancelled',
                            cancel_reason = 'expired_not_picked_up',
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        order_id
                    )

                # Notify student
                student = await self.db.get_user_by_id(r["user_id"])
                if student and student.get("telegram_id"):
                    try:
                        await self.bot.send_message(
                            student["telegram_id"],
                            f"❌ **Order {order_id} Cancelled**\n"
                            "──────────────────────\n"
                            "⏱ Your order was ready but not picked up within 3 hours.\n"
                            "💵 No charges applied.\n"
                            "✨ Please place a new order when convenient."
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass

                # Notify vendor
                
                if settings.ADMIN_DAILY_GROUP_ID:
                    try:
                        await self.bot.send_message(
                            settings.ADMIN_DAILY_GROUP_ID,
                            f"⚠️ Order #{order_id} auto‑cancelled: student did not pick up within 3 hours."
                        )
                    except (TelegramBadRequest, TelegramForbiddenError):
                        pass
                    
                
                vendor = await self.db.get_vendor(r["vendor_id"])
                if vendor and vendor.get("telegram_id"):
                        try:
                            await self.bot.send_message(
                                vendor["telegram_id"],
                                f"⚠️ ትዕዛዝ #{order_id} ተዘርዙዋል: በሶስት ሰአት ውስጥ ወደ አዘዘው ሰው መድረስ አልቻለም"
                            )
                        except (TelegramBadRequest, TelegramForbiddenError):
                            pass

            if rows:
                log.info("Expired %d unpicked ready orders", len(rows))

        except Exception as e:
            log.exception("Failed to expire unpicked ready orders: %s", e)

        from datetime import datetime, timedelta


    async def auto_reassign_unaccepted_orders(self) -> None:
        """
        Periodically checks for orders that have a delivery_guy_id but were not accepted
        within the timeout window. Resets them to 'pending' and re-offers to next candidate.
        """
        log.info("Running auto-reassign unaccepted orders task.")
        try:
            cutoff_time = datetime.now() - timedelta(minutes=5)

            async with self.db._open_connection() as conn:
        # Fetch orders that are unassigned, older than cutoff, and still in valid statuses
                stuck_orders = await conn.fetch(
                    """
                    SELECT id, delivery_guy_id, status
                    FROM orders
                    WHERE delivery_guy_id IS NULL
                    AND updated_at < $1
                    AND status = ANY($2)
                    """,
                    cutoff_time,
                    ["preparing", "ready"]
                )
            if not stuck_orders:
                log.info("No unaccepted orders found to reassign.")
                return

            for row in stuck_orders:
                order_id, dg_id, status = row["id"], row["delivery_guy_id"], row["status"]


                # 2. Mark DG back to active
                if dg_id:
                    async with self.db._open_connection() as conn:
                        await conn.execute(
                            """
                            UPDATE delivery_guys
                            SET active = TRUE
                            WHERE id = $1
                            """,
                            dg_id
                        )

                log.warning("Reassigned timed-out order %s from DG %s.", order_id, dg_id)

                # 3. Notify admin group
                try:
                    from utils.db_helpers import notify_admin_log
                    msg = (
                        f"⏰ Auto-reassign triggered\n"
                        f"📦 Order #{order_id} was stuck with DG {dg_id} (status={status}).\n"
                        f"↩️ Reset to pending for re-offer."
                    )
                    await notify_admin_log(self.bot, ADMIN_GROUP_ID, msg)
                except Exception:
                    log.exception("[ADMIN LOG] Failed to notify for order %s", order_id)

                # 4. Re-offer to next candidate
                try:
                    from utils.helpers import find_next_candidate
                    from handlers.delivery_guy import send_new_order_offer

                    order = await self.db.get_order(order_id)
                    if order:
                        next_dg = await find_next_candidate(self.db, order_id, order)
                        if next_dg:
                            await send_new_order_offer(self.bot, next_dg, order)
                            log.info("[REOFFER] Offered order %s to DG %s", order_id, next_dg["id"])
                            await notify_admin_log(
                                self.bot,
                                ADMIN_GROUP_ID,
                                f"🔁 Order #{order_id} re-offered to DG {next_dg['id']}."
                            )
                        else:
                            log.warning("[REOFFER] No eligible DG found for order %s", order_id)
                            await notify_admin_log(
                                self.bot,
                                ADMIN_GROUP_ID,
                                f"⚠️ No eligible DG found for order #{order_id}. Manual assignment required."
                            )
                except Exception:
                    log.exception("[REOFFER] Failed to re-offer order %s", order_id)

        except Exception:
            log.exception("Error during auto-reassign task")

    async def reset_skips_daily_job(self) -> None:
        """Job wrapper for the daily skip reset function."""
        await reset_skips_daily(self.db.db_path)
        log.info("Scheduled job: Daily skips reset successfully.")


    # -----------------------------------------------
    # Existing Jobs
    # -----------------------------------------------
    async def daily_leaderboard_reset(self) -> None:
        """Reset daily leaderboard stats (optional feature)."""
        log.info("Running daily leaderboard reset task")
        # Placeholder for future leaderboard reset logic
    async def send_admin_summary(self) -> None:
        """Send daily summary to admins."""
        log.info("Sending admin summary")

        try:
            # Use AnalyticsService to compute everything
            analytics = AnalyticsService(self.db)
            summary = await analytics.summary_text()

            # Send to all admins (group or individual IDs)
            try:
                    await self.bot.send_message(
                        ADMIN_GROUP_ID,
                        summary,
                        parse_mode="Markdown"
                    )
                    log.info(f"✅ Sent summary to admin {ADMIN_GROUP_ID}")
            except Exception as e:
                    log.error(f"❌ Failed to send summary to admin {ADMIN_GROUP_ID}: {e}")

        except Exception as e:
            log.error(f"Error in admin summary task: {e}")

    async def cleanup_inactive_sessions(self) -> None:
        """Clean up inactive user sessions (placeholder)."""
        log.info("Running inactive session cleanup")
        # Placeholder for cleaning up stale FSM states, etc.


    def start(self) -> None:
        """Start scheduler with all jobs."""
        
        # Daily leaderboard reset at midnight
        self.scheduler.add_job(
            self.daily_leaderboard_reset,
            CronTrigger(hour=0, minute=0),
            id="daily_leaderboard_reset"
        )
        
        # 🆕 NEW JOB 1: Reset daily skips at midnight
        self.scheduler.add_job(
            self.reset_skips_daily_job,
            CronTrigger(hour=0, minute=0, second=10), # Slight offset to avoid conflict
            id="reset_daily_skips_job"
        )
        
        # 🆕 NEW JOB 2: Auto-reassign orders every 5 minutes (Fallback)
        self.scheduler.add_job(
            self.auto_reassign_unaccepted_orders,
            'interval',
            minutes=10,
            id='auto_reassign_unaccepted_orders'
        )
        
        # 🆕 NEW JOB 3: Update live offers every 5 seconds
        self.scheduler.add_job(
            self.update_order_offers,
            'interval',
            seconds=20,
            id='update_order_offers'
        )
        
        self.scheduler.add_job(
            self.expire_stale_orders,
            'interval',
            minutes=5,
            id='expire_stale_orders'
        )
        self.scheduler.add_job(
            self.expire_unpicked_ready_orders,
            'interval',
            hours = 2,
            id='expire_unpicked_ready_orders'
        )

        # Admin summary at 23:00 daily
        print('Scheduling admin summary job', ADMIN_IDS)
        if ADMIN_GROUP_ID:
            self.scheduler.add_job(
                self.send_admin_summary,
                CronTrigger(hour=23, minute=0),
                id="admin_summary"
            )
            # self.scheduler.add_job(self.send_admin_summary, "interval", minutes=0.2)  # run every 1 minute for testing

        # Cleanup every 6 hours
        self.scheduler.add_job(
            self.cleanup_inactive_sessions,
            CronTrigger(hour="*/6"),
            id="cleanup_sessions"
        )
        
        #cleanup the inactive delivery guys and send them summary at 23:59
        self.scheduler.add_job(
            self.reset_delivery_guys_and_send_summary,
            CronTrigger(hour=23, minute=5),
            id="dg_daily_summary"
        )


        
        # Vendor jobs
        vj = VendorJobs(self.db, self.bot)

        # Nightly vendor daily summaries (e.g., 21:00)
        self.scheduler.add_job(
            vj.send_daily_summary,
            CronTrigger(hour=21, minute=0),
            id="vendor_daily_summary"
        )

        # Weekly vendor summary (Sunday 21:10)
        self.scheduler.add_job(
            vj.send_weekly_summary,
            CronTrigger(day_of_week="sun", hour=21, minute=10),
            id="vendor_weekly_summary"
        )

        # Daily reliability alerts (e.g., 17:00)
        self.scheduler.add_job(
            vj.reliability_alerts,
            CronTrigger(hour=17, minute=0),
            id="vendor_reliability_alerts"
        )

        self.scheduler.start()
        log.info("Scheduler started with all jobs")

    def shutdown(self) -> None:
        """Shutdown scheduler."""
        self.scheduler.shutdown()
        log.info("Scheduler shut down")
        
    
    