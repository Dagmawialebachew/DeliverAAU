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
            from utils.db_helpers import add_dg_to_blacklist

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

                    # Clear DB assignment and mark pending
                    async with self.db._open_connection() as conn:
                        await conn.execute(
                            "UPDATE orders SET status = $1, delivery_guy_id = $2 WHERE id = $3",
                            "pending", None, order_id
                        )

                    # Persist rejection using DG internal ID
                    dg = await _db_get_delivery_guy_by_user(chat_id)
                    if dg:
                        try:
                            await add_dg_to_blacklist(self.db, order_id, dg["id"])
                        except Exception:
                            log.exception("[BLACKLIST] Failed to add DG %s to blacklist for order %s", dg["id"], order_id)

                    # Stop tracking this offer
                    PENDING_OFFERS.pop(order_id, None)

                    # Re-offer to next candidate (do not assign)
                    order = await self.db.get_order(order_id)
                    if order:
                        log.debug("[REOFFER] Finding next candidate for order %s", order_id)
                        next_dg = await find_next_candidate(self.db, order_id, order)
                        if next_dg:
                            try:
                                await send_new_order_offer(self.bot, next_dg, order)
                                log.info("[REOFFER] Offered order %s to DG %s", order_id, next_dg["id"])
                            except Exception:
                                log.exception("[REOFFER] Failed to send offer for order %s to DG %s", order_id, next_dg.get("id"))
                        else:
                            log.warning("[REOFFER] No eligible DG found for order %s", order_id)

                # --- Offer still active: update countdown display ---
                else:
                    if countdown == offer.get("last_countdown"):
                        continue
                    offer["last_countdown"] = countdown

                    order = await self.db.get_order(order_id)
                    if not order:
                        log.debug("[DEBUG] Order %s not found in DB, removing from tracker", order_id)
                        PENDING_OFFERS.pop(order_id, None)
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

                # Re-offer to next candidate
                order = await self.db.get_order(order_id)
                if order:
                    next_dg = await find_next_candidate(self.db, order_id, order)
                    if next_dg:
                        try:
                            await send_new_order_offer(self.bot, next_dg, order)
                            log.info("[REOFFER] Offered order %s to DG %s after block", order_id, next_dg["id"])
                        except Exception:
                            log.exception("[REOFFER] Failed to send offer for order %s to DG %s", order_id, next_dg.get("id"))

            except Exception:
                log.exception("Unexpected error updating offer %s", order_id)
                PENDING_OFFERS.pop(order_id, None)

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
                            "❌ **Order Expired**\n"
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
        """Reset all delivery guys to offline, send them daily summary, and notify admins with a report."""
        log.info("Running daily DG reset + summary task")
        try:
            async with self.db._pool.acquire() as conn:

                # 1) Reset all DGs to offline
                await conn.execute("UPDATE delivery_guys SET active = FALSE, last_offline_at = NOW()")
                log.info("All delivery guys set to offline")

                # 2) Fetch DGs
                dgs = await conn.fetch("SELECT id, telegram_id FROM delivery_guys")
                total_dgs = len(dgs)

                from datetime import date
                today_date = date.today()
                today_date_str = today_date.strftime("%Y-%m-%d")

                sent_count = 0
                failed_count = 0
                failed_ids: list[int] = []

                # 3) HYPED summary to each DG
                for dg in dgs:
                    dg_id = dg["id"]
                    tg_id = dg["telegram_id"]

                    try:
                        stats = await self.db.get_daily_stats_for_dg(dg_id, today_date)
                    except Exception:
                        stats = {"deliveries": 0, "earnings": 0.0, "xp": 0, "coins": 0.0}

                    # 🔥 ULTRA-HYPE CREATOR DASHBOARD STYLE
                    text = (
                        f"🔥 **Your DAILY RECAP is READY — {today_date_str}** 🔥\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🚀 **Today’s Performance**\n"
                        f"   • 🚚 Delivers: **{stats.get('deliveries', 0)}**\n"
                        f"   • 💵 Earnings: **{int(stats.get('earnings', 0))} birr**\n"
                        f"   • 🎁 Rewards: **+{stats.get('xp', 0)} XP** • **+{stats.get('coins', 0.0):.2f} Coins**\n\n"
                        f"🧊 **Status:** `OFFLINE`\n"
                        "You’re offline. You are not receiving orders.\n\n"
                        "⚡ **Go Online anytime to jump back into the game.**"
                    )

                    try:
                        await self.bot.send_message(
                            tg_id,
                            text,
                            reply_markup=go_online_keyboard(),
                            parse_mode="Markdown"
                        )
                        sent_count += 1
                        log.info("🔥 Sent hype summary to DG %s", dg_id)
                    except Exception as e:
                        failed_count += 1
                        failed_ids.append(dg_id)
                        log.error("❌ Failed to send summary to DG %s: %s", dg_id, e)

                # -------------------------
                # ADMIN REPORT SECTIONS
                # -------------------------

                # A) Top 3 drivers
                top_drivers_rows = await conn.fetch(
                    """
                    SELECT ds.dg_id, ds.deliveries, ds.earnings, dg.name
                    FROM daily_stats ds
                    LEFT JOIN delivery_guys dg ON dg.id = ds.dg_id
                    WHERE ds.date = $1
                    ORDER BY ds.deliveries DESC, ds.earnings DESC
                    LIMIT 3
                    """,
                    today_date_str
                )

                top_drivers = []
                for r in top_drivers_rows:
                    name = r["name"] or f"DG #{r['dg_id']}"
                    top_drivers.append({
                        "id": r["dg_id"],
                        "name": name,
                        "deliveries": int(r["deliveries"] or 0),
                        "earnings": int(r["earnings"] or 0.0)
                    })

                # B) Drivers low acceptance
                            # Fetch all drivers for today
                low_accept_rows = await conn.fetch(
                    """
                    SELECT ds.dg_id, dg.name
                    FROM daily_stats ds
                    LEFT JOIN delivery_guys dg ON dg.id = ds.dg_id
                    WHERE ds.date = $1
                    """,
                    today_date_str
                )

                driver_alerts = []
                for r in low_accept_rows:
                    # Pass the db instance explicitly, just like in delivery_guy.py
                    acceptance_rate = await calc_acceptance_rate(self.db, r["dg_id"])
                    
                    driver_alerts.append(
                        f"⚠️ {r['name'] or f'DG #{r['dg_id']}'} • {acceptance_rate:.1f}% acceptance"
                    )


                # C) Vendor cancels
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
                    today_date
                )

                vendor_alerts = [
                    f"- {r['vendor_name']} • {int(r['cancels'])} cancels"
                    for r in vendor_cancel_rows
                ]

                # D) Engagement metric
                reactivated_count = int(await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM delivery_guys
                    WHERE last_online_at IS NOT NULL
                    AND last_online_at > (NOW() - INTERVAL '2 hours')
                    """
                ) or 0)

                # -------------------------------------
                # ADMIN REPORT — ULTRA HYPE CREATOR MODE
                # -------------------------------------

                failed_ids_str = f" (IDs: {', '.join(map(str, failed_ids))})" if failed_ids else ""

                admin_lines = [
        "",  # breaker line to separate from previous messages
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📢 **DELIVERY OPERATIONS DASHBOARD — {today_date_str}**",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"👥 DGs Reset: **{total_dgs}**",
        f"📤 Summaries Sent: **{sent_count}**",
        f"⚠️ Failed: **{failed_count}**{failed_ids_str}",
        "",
        "🏆 **TOP PERFORMERS**:"
    ]


                if top_drivers:
                    for idx, td in enumerate(top_drivers, 1):
                        admin_lines.append(
                            f"{idx}. **{td['name']}** — 🚚 {td['deliveries']} • 💵 {td['earnings']} birr"
                        )
                else:
                    admin_lines.append("No top performers today.")

                admin_lines.append("")
                admin_lines.append("🚨 **ALERTS**")
                admin_lines.extend(driver_alerts or ["- No driver alerts"])
                admin_lines.extend(vendor_alerts or ["- No vendor alerts"])

                admin_lines.append("")
                admin_lines.append("📈 **ENGAGEMENT METRIC**")
                admin_lines.append(
                    f"⚡ {reactivated_count} drivers bounced back online within 2 hours."
                )

                admin_text = "\n".join(admin_lines)

                # Send admin report
                try:
                    await self.bot.send_message(
                        ADMIN_GROUP_ID,
                        admin_text,
                        parse_mode="Markdown"
                    )
                    log.info("🔥 Sent admin creator dashboard report")
                except Exception as e:
                    log.error("❌ Failed to send admin report: %s", e)

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

        
    async def auto_reassign_unaccepted_orders(self) -> None:
        """
        Periodically checks for 'assigned' orders that were not accepted
        (e.g., timed out) and sets them back to 'pending' for re-assignment.
        """
        log.info("Running auto-reassign unaccepted orders task.")
        try:
            # Reassign orders older than 5 minutes that are still in 'assigned' status
            cutoff_time = datetime.now() - timedelta(minutes=5)

            async with self.db._pool.acquire() as conn:                # Fetch unaccepted orders
                unaccepted_orders = await conn.fetch(
                    """
                    SELECT id, delivery_guy_id
                    FROM orders
                    WHERE status = 'assigned' AND created_at < $1
                    """,
                    cutoff_time
                )

                if not unaccepted_orders:
                    log.info("No unaccepted orders found to reassign.")
                    return

                for row in unaccepted_orders:
                    order_id, dg_id = row["id"], row["delivery_guy_id"]

                    # 1. Reassign order back to pending
                    await conn.execute(
                        """
                        UPDATE orders
                        SET status = 'pending', delivery_guy_id = NULL
                        WHERE id = $1
                        """,
                        order_id
                    )

                    # 2. Set the DG back to active (if they were inactive due to assignment)
                    if dg_id:
                        await conn.execute(
                            """
                            UPDATE delivery_guys
                            SET active = TRUE
                            WHERE id = $1
                            """,
                            dg_id
                        )

                    log.warning("Reassigned timed-out order %s from DG %s.", order_id, dg_id)

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
            minutes=5,
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
        # self.scheduler.add_job(self.reset_delivery_guys_and_send_summary, "interval", minutes=0.2)  # run every 1 minute for testing


        
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
        
    
    