# scheduler/vendor_jobs.py
import logging
import datetime
from aiogram import Bot
from database.db import Database
from config import settings

from utils.db_helpers import (
    calc_vendor_day_summary,
    calc_vendor_week_summary,
    format_admin_daily_summary,
    format_vendor_daily_summary_amharic,
    notify_admin_log
)

log = logging.getLogger(__name__)
ADMIN_GROUP_ID = settings.ADMIN_DAILY_GROUP_ID

class VendorJobs:
    def __init__(self, db: Database, bot: Bot):
        self.db = db
        self.bot = bot

    async def send_daily_summary(self) -> None:
        """
        Send Amharic daily summaries to each vendor,
        and English daily summaries to admin group.
        """
        vendors = await self.db.list_vendors()
        today = datetime.date.today().strftime("%Y-%m-%d")

        for v in vendors:
            vid = v["id"]
            summary = await calc_vendor_day_summary(self.db.db_path, vid, date=today)

            # Vendor (Amharic)
            try:
                await self.bot.send_message(
                    v["telegram_id"],
                    format_vendor_daily_summary_amharic(summary)
                )
            except Exception:
                log.exception("Failed to send daily summary to vendor %s", v["name"])

            # Admin (English)
            try:
                await self.bot.send_message(
                    ADMIN_GROUP_ID,
                    format_admin_daily_summary(summary)
                )
            except Exception:
                log.exception("Failed to send admin daily summary for vendor %s", v["name"])

    async def send_weekly_summary(self) -> None:
        """
        Send weekly Amharic summaries to vendors and English summaries to admin group.
        """
        vendors = await self.db.list_vendors()
        for v in vendors:
            vid = v["id"]
            ws = await calc_vendor_week_summary(self.db.db_path, vid)

            # Vendor (Amharic) compact weekly
            vendor_text = (
                f"📅 የሳምንቱ ሪፖርት — {v['name']}\n"
                f"🗓 ከ{ws['start_date']} እስከ {ws['end_date']}\n\n"
                f"📦 ትዕዛዞች: {ws['delivered'] + ws['cancelled']} (✅ {ws['delivered']} | ❌ {ws['cancelled']})\n"
                f"💵 ጠቅላላ ገቢ: {int(ws['total_payout'])} ብር\n"
            )
            try:
                await self.bot.send_message(v["telegram_id"], vendor_text)
            except Exception:
                log.exception("Failed to send weekly summary to vendor %s", v["name"])

            # Admin (English) with per-day breakdown
            admin_text = (
                f"📊 Weekly Summary — {v['name']}\n"
                f"{ws['start_date']} → {ws['end_date']}\n\n"
                f"📦 Orders: {ws['delivered'] + ws['cancelled']} (Delivered {ws['delivered']} | Cancelled {ws['cancelled']})\n"
                f"💵 Food Revenue: {int(ws['food_revenue'])} birr\n"
                f"🚚 Delivery Fees: {int(ws['delivery_fees'])} birr\n"
                f"💰 Total Payout: {int(ws['total_payout'])} birr\n\n"
                + "\n".join([
                    f"• {d['date']}: {d['delivered'] + d['cancelled']} orders — {int(d['total_payout'])} birr"
                    for d in ws['days']
                ])
            )
            try:
                await self.bot.send_message(ADMIN_GROUP_ID, admin_text)
            except Exception:
                log.exception("Failed to send admin weekly summary for vendor %s", v["name"])

    async def reliability_alerts(self, min_reliability_pct: int = 70) -> None:
        """
        If vendor reliability drops below threshold for today, notify admin group (English)
        and warn vendor in Amharic.
        """
        vendors = await self.db.list_vendors()
        today = datetime.date.today().strftime("%Y-%m-%d")

        for v in vendors:
            vid = v["id"]
            s = await calc_vendor_day_summary(self.db.db_path, vid, date=today)
            if s["reliability_pct"] < min_reliability_pct:
                # Admin alert
                try:
                    await notify_admin_log(
                        self.bot,
                        ADMIN_GROUP_ID,
                        f"⚠️ Reliability Alert — {v['name']}: {s['reliability_pct']}% today."
                    )
                except Exception:
                    log.exception("Failed to notify admin reliability for vendor %s", v["name"])

                # Vendor warning (Amharic)
                try:
                    await self.bot.send_message(
                        v["telegram_id"],
                        f"⚠️ ታማኝነት ዝቅ ነው: {s['reliability_pct']}% ዛሬ.\nእባክዎ ትዕዛዞችን በጊዜ ያቀርቡ።"
                    )
                except Exception:
                    log.exception("Failed to send reliability warning to vendor %s", v["name"])
