import logging
import json 
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError 

from database.db import Database
from config import settings
from handlers.delivery_guy import _db_get_delivery_guy_by_user
from utils.db_helpers import reset_skips_daily, add_dg_to_blacklist
from utils.globals import PENDING_OFFERS # IMPORTANT: Ensure utils/globals.py exists
from utils.vendor_scheduler import VendorJobs
ADMIN_IDS = settings.ADMIN_IDS
log = logging.getLogger(__name__)


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
        Periodically checks PENDING_OFFERS, updates countdown in the message,
        and automatically cancels/reassigns expired or blocked offers.
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

            # Emoji urgency levels
            if remaining_seconds > 120:
                icon = "⏳"
            elif remaining_seconds > 30:
                icon = "⚠️"
            else:
                icon = "❌"

            from handlers.delivery_guy import order_offer_keyboard
            from utils.helpers import assign_delivery_guy, notify_student_reassignment
            from utils.db_helpers import add_dg_to_blacklist

            try:
                # --- Case 1: Offer expired ---
                if remaining_seconds <= 0:
                    log.warning("Order offer %s expired for DG %s.", order_id, chat_id)

                    expired_text = (
                        "⏰ **Offer Expired!**\n\n"
                        f"📦 Order #{order_id} has been automatically returned to the pool."
                    )
                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=expired_text,
                        reply_markup=None,
                        parse_mode="Markdown"
                    )

                    # Reset order
                    async with self.db._open_connection() as conn: 
                        await conn.execute(
                            # Use positional parameters ($1, $2, etc.) for PostgreSQL
                            # NULL is handled the same way, but the parameter must be explicitly passed or hardcoded.
                            "UPDATE orders SET status = $1, delivery_guy_id = $2 WHERE id = $3",
                            'pending', None, order_id
                        )

                    # Persist rejection using DG internal ID
                    dg = await _db_get_delivery_guy_by_user(chat_id)
                    if dg:
                        await add_dg_to_blacklist(self.db.db_path, order_id, dg["id"])

                    PENDING_OFFERS.pop(order_id, None)  # stop scheduler tracking

                    # 🔄 Immediate reassignment
                    order = await self.db.get_order(order_id)
                    if order:
                        log.debug("[DEBUG] Reassigning expired order %s", order_id)
                        chosen = await assign_delivery_guy(self.db.db_path, order_id, bot=self.bot, current_order_data=order)
                        if chosen:
                            log.debug("[DEBUG] Order %s reassigned to DG %s", order_id, chosen["id"])
                            await notify_student_reassignment(self.bot, self.db, order, chosen)

                # --- Case 2: Offer active ---
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

                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=new_message_text,
                        reply_markup=kb,
                        parse_mode="Markdown"
                    )
                    log.debug("[DEBUG] Countdown updated for order %s", order_id)

            except TelegramForbiddenError:
                log.warning("DG %s blocked the bot. Removing offer %s.", chat_id, order_id)
                PENDING_OFFERS.pop(order_id, None)

                # Persist rejection using DG internal ID
                dg = await _db_get_delivery_guy_by_user(chat_id)
                if dg:
                    await add_dg_to_blacklist(self.db.db_path, order_id, dg["id"])

                # 🔄 Immediate reassignment
                order = await self.db.get_order(order_id)
                if order:
                    log.debug("[DEBUG] Reassigning blocked order %s", order_id)
                    chosen = await assign_delivery_guy(self.db.db_path, order_id, bot=self.bot, current_order_data=order)
                    if chosen:
                        log.debug("[DEBUG] Order %s reassigned to DG %s", order_id, chosen["id"])
                        await notify_student_reassignment(self.bot, self.db, order, chosen)

            except TelegramBadRequest as e:
                if "message is not modified" not in str(e):
                    log.warning("Offer message for order %s failed to edit: %s", order_id, e)
                PENDING_OFFERS.pop(order_id, None)

            except Exception:
                log.exception("Unexpected error updating offer %s", order_id)
                PENDING_OFFERS.pop(order_id, None)

    #----------------------------------
    # 🆕 NEW: Core System Jobs (Section 7)
    # -----------------------------------------------
        
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
            # Assuming db.get_leaderboard exists
            users = await self.db.get_leaderboard(limit=100)
            total_users = len(users)

            # Get active users (last 24 hours)
            cutoff = (datetime.now() - timedelta(days=1)).isoformat()
            active_count = sum(
                1 for u in users
                if u.get("last_active", "") >= cutoff
            )

            summary = f"""
📊 **Daily Summary** - {datetime.now().strftime("%Y-%m-%d")}

👥 Total Users: {total_users}
✅ Active (24h): {active_count}
🏆 Top Deliverer: {users[0].get('first_name', 'N/A') if users else 'None'} ({users[0].get('xp', 0)} XP)

🚀 Deliver AAU Bot
"""
            # Send to all admins
            for admin_id in ADMIN_IDS:
                try:
                    await self.bot.send_message(
                        admin_id,
                        summary,
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    log.error(f"Failed to send summary to admin {admin_id}: {e}")

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
            seconds=180,
            id='update_order_offers'
        )

        # Admin summary at 23:00 daily
        if ADMIN_IDS:
            self.scheduler.add_job(
                self.send_admin_summary,
                CronTrigger(hour=23, minute=0),
                id="admin_summary"
            )

        # Cleanup every 6 hours
        self.scheduler.add_job(
            self.cleanup_inactive_sessions,
            CronTrigger(hour="*/6"),
            id="cleanup_sessions"
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
        
    
    