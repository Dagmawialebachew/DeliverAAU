import asyncio
from collections import Counter
import contextlib
import json
import logging
import math
import random
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timedelta
from aiogram import Bot
from aiogram import Router, F, types
from config import settings
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.exceptions import TelegramBadRequest
from datetime import date
from app_context import db
from handlers.delivery_guy import COIN_RATIO, ENABLE_COINS, ENABLE_XP, XP_PER_DELIVERY, _db_get_delivery_guy_by_user, accepted_order_actions
from handlers.delivery_guy import notify_student
from utils.db_helpers import calc_acceptance_rate
from utils.helpers import calculate_commission


async def post_accept_updates(call: CallbackQuery, order_id: int, dg: Dict[str, Any]):
    try:
        # Refresh order with latest info
        order = await db.get_order(order_id)
        if not order:
            return

        subtotal = order.get("food_subtotal", 0)
        delivery_fee = order.get("delivery_fee", 0)
        total_payable = subtotal + delivery_fee
        order["delivery_guy_name"] = dg["name"]
        order["campus"] = dg.get("campus")

        # Notify student
        await notify_student(call.bot, order, "assigned")

        # Build items string
        try:
            items = json.loads(order.get("items_json", "[]")) or []
            from collections import Counter
            names = [i.get("name", "") if isinstance(i, dict) else str(i) for i in items]
            counts = Counter(names)
            items_str = ", ".join(
                f"{name} x{count}" if count > 1 else name
                for name, count in counts.items()
            )
        except Exception:
            items_str = "Items unavailable"

        # Dropoff + campus
        dropoff = order.get("dropoff", "N/A")
        campus_text = await db.get_user_campus_by_order(order["id"])
        dropoff = f"{dropoff} • {campus_text}" if campus_text else dropoff

        # Message text
        message_text = (
            f"📦 Order #{order_id}\n"
            f"📌 Status: {'✅ Ready for pickup' if order['status']=='ready' else '👨‍🍳 The meal is being prepared...'}\n\n"
            "──────────────────────\n"
            f"🏠 Pickup: {order.get('pickup')}\n"
            f"📍 Drop-off: {dropoff}\n"
            f"{('📝 Notes: ' + order.get('notes', '') + '\n') if order.get('notes') else ''}"
            f"💰 Subtotal Fee: {subtotal} birr\n"
            f"🚚 Delivery fee: {delivery_fee} birr\n"
            "──────────────────────\n"
            f"💵 Total Payable: {total_payable} birr\n\n"
            f"🛒 Items: {items_str}\n\n"
            "⚡ Manage this order below.\n\n"
            "For robust and fast use My Orders in the dashboard."
        )


        # ✅ Notify daily admin group
        if settings.ADMIN_DAILY_GROUP_ID:
            admin_msg = (
                f"🚴 *Delivery Guy Accepted Order #{order_id}*\n"
                f"👤 DG: {dg.get('name','Unknown')} ({dg.get('phone','N/A')})\n"
            )
            try:
                await call.bot.send_message(
                    settings.ADMIN_DAILY_GROUP_ID,
                    admin_msg,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logging.warning(f"Failed to notify admin group for accepted order {order_id}: {e}")

    

        # Edit vendor message
        try:
            status = order["status"]
            action_key = "ready" if status == "ready" else "accepted"
            await call.message.edit_text(
                message_text,
                reply_markup=accepted_order_actions(order_id, action_key),
                parse_mode="Markdown"
            )
            await db.increment_accepted_requests(dg["id"])
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
               logging.warning("Failed to edit message after acceptance: %s", str(e))
               await call.message.answer(
                    message_text,
                    reply_markup=accepted_order_actions(order_id, "accepted"),
                    parse_mode="Markdown"
                )
    except Exception as e:
       logging.exception("post_accept_updates failed for order %s: %s", order_id, e)

async def post_delivered_updates(call: CallbackQuery, dg: Dict[str, Any], order: Dict[str, Any]):
    try:
        # Commission breakdown (use platform_share instead of delivery_fee)
        breakdown = calculate_commission(order.get("items_json", "[]"))
        platform_profit = breakdown.get("platform_share", 0)

        xp_gained = XP_PER_DELIVERY if ENABLE_XP else 0
        coins_gained = platform_profit * COIN_RATIO if ENABLE_COINS else 0.0

        today = date.today()

        await db.record_daily_stat_delivery(
            dg["id"],
            today,
            platform_profit,
            xp_gained if xp_gained > 0 else None,
            coins_gained if coins_gained > 0 else None
        )

        if xp_gained > 0:
            updated_dg = await _db_get_delivery_guy_by_user(call.from_user.id)
            if updated_dg and hasattr(db, "auto_compute_level"):
                new_level = await db.auto_compute_level(updated_dg["xp"])
                if new_level != updated_dg.get("level"):
                    await db.update_delivery_guy_level(dg["id"], new_level)

        # Notify student
        await notify_student(call.bot, order, "delivered")

        # Daily summary for DG
        today_stats = await db.get_daily_stats(dg["id"], datetime.now().date()) or {}
        stats = await db.get_daily_stats_for_dg(dg["id"], today)

        deliveries_today = today_stats.get("deliveries", 0)
        acceptance_rate = await calc_acceptance_rate(db, dg["id"])
        reliability = "Excellent 🚀" if acceptance_rate >= 90 else ("Good 👍" if acceptance_rate >= 80 else "Fair")

        summary_text = (
            f"🎉 **Delivery #{order['id']} Complete!**\n"
            "──────────────────────\n"
            f"📦 Status: Delivered successfully\n\n"
            "📊 **Your Daily Progress**\n"
            f"🚚 Deliveries today: *{deliveries_today}*\n"
            f"💵 Earnings: *{int(stats['earnings'])} birr*\n"
            f"⚖️ Acceptance Rate: *{int(acceptance_rate)}%* ({reliability})\n\n"
            "🎁 **Rewards Earned**\n"
            f"✨ +{xp_gained} XP\n"
            f"💰 +{coins_gained:.2f} Coins\n\n"
            "⚡ Keep going strong! Use the menu below to head back to your dashboard."
        )

        try:
            await call.message.edit_text(summary_text, parse_mode="Markdown")
        except TelegramBadRequest:
            await call.message.answer(summary_text, parse_mode="Markdown")
            
        today_stats = await db.get_daily_stats(dg["id"], datetime.now().date()) or {}
        # But this is per DG. For total platform profit, add a helper:
        platform_today = await db.get_platform_total(today)

        # --- Playful admin notification ---
        
        admin_group_id = -1003640931988  # replace with your actual group ID
        profit_msg = build_profit_message(order['id'], platform_profit, platform_today)        
        await call.bot.send_message(admin_group_id, profit_msg, parse_mode="Markdown")

    except Exception as e:
        logging.exception("post_delivered_updates failed for order %s: %s", order["id"], e)

def build_profit_message(order_id: int, platform_profit: float, total_profit: float) -> str:
    templates = [
        f"🔥 Yo fam, *order #{order_id}* just dropped *{platform_profit} birr* in the bag 💸\n\n"
        f"Total profit today: *{total_profit} birr* — we cookin’ like soul food 🌶️🚀",

        f"💃🏾 Order *#{order_id}* came through with *{platform_profit} birr* profit!\n\n"
        f"Running tally: *{total_profit} birr* stacked — stay fly 😎✨",

        f"🎤 Order *#{order_id}* hit the stage — *{platform_profit} birr* profit!\n\n"
        f"Campus hustle smoother than jazz \n\n🎷🔥 Total today: *{total_profit} birr*",

        f"🚀 Nigaaee, order *#{order_id}* just blessed us with *{platform_profit} birr* 💰\n\n"
        f"We flexin’ — sauce dripping 🍗💦 \n\nTotal profit today: *{total_profit} birr*",

        f"😏 Order *#{order_id}* slid in with *{platform_profit} birr* profit!\n\n"
        f"Sexy hustle, spicy vibes 🌶️💃🏾 \n\nTotal stacked: *{total_profit} birr*"
    ]
    return random.choice(templates)

async def post_referral_updates(bot, new_user_id: int, inviter_id: int | None, new_user_name: str, inviter_name: str):
    try:
        async with db._open_connection() as conn:
            new_user_bites = await conn.fetchval(
                "SELECT bites FROM leaderboards WHERE user_id=$1", new_user_id
            ) or 0
            new_user_rank = await conn.fetchval(
                """
                SELECT r FROM (
                    SELECT user_id, RANK() OVER (ORDER BY bites DESC) AS r
                    FROM leaderboards
                ) t WHERE user_id=$1
                """,
                new_user_id
            ) or "—"

            inviter_stats = ""
            if inviter_id:
                inviter_bites = await conn.fetchval(
                    "SELECT bites FROM leaderboards WHERE user_id=$1", inviter_id
                ) or 0
                inviter_rank = await conn.fetchval(
                    """
                    SELECT r FROM (
                        SELECT user_id, RANK() OVER (ORDER BY bites DESC) AS r
                        FROM leaderboards
                    ) t WHERE user_id=$1
                    """,
                    inviter_id
                ) or "—"
                inviter_stats = f"\n🍪 Inviter Bites: {inviter_bites} | Rank: #{inviter_rank}"

        message_text = (
            f"👥 *Referral Event*\n"
            f"──────────────────────\n"
            f"🆕 New User: {new_user_name} (id={new_user_id})\n"
            f"🎯 Invited By: {inviter_name if inviter_id else 'No referral'}\n\n"
            f"🍪 New User Bites: {new_user_bites} | Rank: #{new_user_rank}"
            f"{inviter_stats}\n\n"
            "✅ Referral bonus applied (+1 Bite welcome)\n"
            "──────────────────────\n"
            "📡 Logged for admin tracking."
        )

        if settings.ADMIN_REFERRAL_GROUP_ID:
            await bot.send_message(
                settings.ADMIN_REFERRAL_GROUP_ID,
                message_text,
                parse_mode="Markdown"
            )
    except Exception as e:
        logging.exception("post_referral_updates failed: %s", e)



async def notify_admin_spin(
    bot,
    user_id: int,
    telegram_id: int,
    prize: str,
    bites: int,
    rank: int,
    tg_first_name: str,
    tg_username: Optional[str] = None
):
    """
    Background task: log prize to DB and notify admin group.
    """
    # Persist prize
    await db.record_spin_prize(user_id, prize)

    # Fetch DB user info (phone, campus)
    async with db._open_connection() as conn:
        row = await conn.fetchrow(
            "SELECT phone, campus FROM users WHERE id=$1",
            user_id
        )
    phone = row["phone"] if row else "—"
    campus = row["campus"] if row else "—"

    # Build admin message
    text = (
        f"🎡 Spin Result\n"
        f"────────────────────\n"
        f"📛 Name: {tg_first_name}\n"
        f"🔗 Username: @{tg_username if tg_username else '—'}\n"
        f"💬 Telegram ID: {telegram_id}\n"
        f"📱 Phone: {phone}\n"
        f"🏫 Campus: {campus}\n"
        f"🏆 Rank: #{rank}\n"
        f"🔥 Bites: {bites}\n"
        f"🎁 Prize: {prize}\n"
        f"🕒 Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    )

    # Send to admin group
    await bot.send_message(settings.ADMIN_REFERRAL_GROUP_ID, text)


async def process_order_rewards(order: dict, db, bot, admin_chat_id: int):
    student_id = order.get("user_id")
    if not student_id:
        return

    async with db._open_connection() as conn:
        # --- Always award +3 bites to the student ---
        # fetch telegram_id for student
        student_tg = await conn.fetchval("SELECT telegram_id FROM users WHERE id=$1", student_id)
        display_name = f"User{student_id}"
        if student_tg:
            try:
                chat = await bot.get_chat(student_tg)
                first = chat.first_name or ""
                last = chat.last_name or ""
                display_name = (first + " " + last).strip() or display_name
            except Exception:
                pass

        await conn.execute(
            """
            INSERT INTO leaderboards (user_id, display_name, bites, last_updated)
            VALUES ($1, $2, 3, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id)
            DO UPDATE SET bites = leaderboards.bites + 3,
                          display_name = EXCLUDED.display_name,
                          last_updated = CURRENT_TIMESTAMP
            """,
            student_id, display_name
        )

        # --- Check if this is their first order ---
        order_count = await conn.fetchval(
            "SELECT COUNT(*) FROM orders WHERE user_id=$1",
            student_id
        )
        if order_count == 1:
            # Find referrer
            referrer_id = await conn.fetchval(
                "SELECT referred_by FROM users WHERE id=$1",
                student_id
            )
            if referrer_id:
                # fetch telegram_id for referrer
                referrer_tg = await conn.fetchval("SELECT telegram_id FROM users WHERE id=$1", referrer_id)
                ref_display_name = f"User{referrer_id}"
                if referrer_tg:
                    try:
                        chat = await bot.get_chat(referrer_tg)
                        first = chat.first_name or ""
                        last = chat.last_name or ""
                        ref_display_name = (first + " " + last).strip() or ref_display_name
                    except Exception:
                        pass

                await conn.execute(
                    """
                    INSERT INTO leaderboards (user_id, display_name, bites, last_updated)
                    VALUES ($1, $2, 2, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id)
                    DO UPDATE SET bites = leaderboards.bites + 2,
                                  display_name = EXCLUDED.display_name,
                                  last_updated = CURRENT_TIMESTAMP
                    """,
                    referrer_id, ref_display_name
                )

                # Notify referrer
                if referrer_tg:
                    asyncio.create_task(
                        bot.send_message(
                            referrer_tg,
                            f"🎉 Your friend {order.get('student_name','—')} placed their first order!\n"
                            f"✨ You earned +2 Bites referral bonus."
                        )
                    )

                # Log to admin
                msg_admin = (
                    f"👥 Referral Bonus Applied\n"
                    f"🆕 New User: {order.get('student_name','—')} (id={student_id})\n"
                    f"🎯 Referrer: {ref_display_name} (id={referrer_id})\n"
                    f"🍪 Bonus: +2 Bites to referrer"
                )
                await bot.send_message(admin_chat_id, msg_admin, parse_mode="Markdown")
