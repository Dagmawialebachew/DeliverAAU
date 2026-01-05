# handlers/rating.py
import contextlib
import logging
from typing import Dict
from unittest.mock import call
from aiogram import Bot, Router, F
import aiogram
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters import Command
from config import settings
from app_context import db
from database.db import Database
from utils.task_scheduler import notify_admin_spin
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)

from handlers.onboarding import build_profile_card, main_menu
router = Router()





#-----------------🎄 ገና Specials-----------------#
@router.message(F.text == "🎄 ገና Specials 🎄")
async def show_genna_specials(message: Message):
    user_id = await db.get_user_id_by_telegram(message.from_user.id)
    if not user_id:
        await message.answer("⚠️ You’re not registered yet.")
        return

    stats = await db.get_genna_progress(user_id)
    text = (
        "🎄 UniBites *ገና Specials*\n"
        "────────────────────────\n\n"
        "🔥 Your ገና Progress\n"
        "────────────────────\n"
        f"🍪 Bites this week: *{stats['bites_this_week']}*\n"
        f"🏆 Weekly rank: *#{stats['rank_position']}*\n"
        f"🎯 Next rank in: *{stats['remaining']}* Bites\n"
        f"⏳ Time left this week: *{stats['days_left']}* days\n\n"
        "*🎡 Spin* to win instant prizes\n"
        "🏆 Top users win the *🍽️ገና Combo* weekly\n\n"
        "Stay active to collect more Bites."
    )
    await message.answer(text, reply_markup=genna_specials_menu())

def genna_specials_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🎡 Spin & Rewards"),
                KeyboardButton(text="🍽️ ገና Combo"),
            ],
            [
                KeyboardButton(text="📊 Leaderboard"),
                KeyboardButton(text="👥 Referrals"),
            ],
            [
                KeyboardButton(text="⬅️ Back to Main Menu"),
            ],
           
        ],
        resize_keyboard=True,
        input_field_placeholder="ገና Specials 👇",
    )
def build_spin_dashboard_text(bites: int, spins_available: int, progress: int) -> str:
    spin_wheel = [
        "🍽️ Free Meal (up to 500 birr)",
        "🎁 100 birr Gift Card",
        "🚚 Free Delivery ×5",
        "🚚 Free Delivery ×2",
        "🥤 Free Soft Drink",
        "🍿2 Free Snacks",
        "💸 30 birr Discount",
        "😅 Try Again",
    ]
    return (
        "🎡 UniBites Spin\n"
        "──────────────────\n"
        f"🔥 Bites: *{bites}*\n"
        f"🎰 Spins available: *{spins_available}*\n"
        f"📊 Progress: *{progress}/25 bites to next spin*\n\n"
        "💡 Ways to *earn more spins*:\n"
        "  _🎯 Order daily (+3 bites per order)_\n"
        "  _🎯 Invite friends with your referral link (+1 bite per order, +2 bites when your friend orders)_\n\n"
        "🎁 Rewards waiting on the wheel:\n\n"
        f"{chr(10).join(spin_wheel)}\n\n"
        "Tap below to spin or sync your bites 👇"
    )


def build_spin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎯 Spin Now", callback_data="spin_now"),
                InlineKeyboardButton(text="🔄 Sync Bites", callback_data="sync_bites"),
            ]
        ]
    )


@router.message(F.text == "🎡 Spin & Rewards")
async def spin_dashboard(message: Message):
    user_id = await db.get_user_id_by_telegram(message.from_user.id)
    if not user_id:
        await message.answer("⚠️ You’re not registered yet.")
        return

    spins_available, bites, progress = await db.get_user_spins_and_bites(user_id)
    
    text = build_spin_dashboard_text(bites, spins_available, progress)
    await message.answer(text, reply_markup=build_spin_keyboard(), parse_mode="Markdown")


@router.callback_query(F.data == "sync_bites")
async def sync_bites_callback(call: CallbackQuery):
    user_id = await db.get_user_id_by_telegram(call.from_user.id)
    if not user_id:
        await call.answer("⚠️ You’re not registered yet.", show_alert=True)
        return

    # Sync spins with latest bites
    await db.sync_spins_for_user(user_id)
    spins_available, bites, progress = await db.get_user_spins_and_bites(user_id)

    text = build_spin_dashboard_text(bites, spins_available, progress)
    try:
        await call.message.edit_text(text, reply_markup=build_spin_keyboard(), parse_mode="Markdown")
    except aiogram.exceptions.TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await call.answer("Already up to date ✅", show_alert=True)
        else:
            raise


import random
import asyncio
from datetime import datetime, timedelta
from aiogram.types import CallbackQuery

SPIN_COOLDOWN = timedelta(hours=0)
last_spin = {}

# --- Wheel + descriptions + weights ---
# constants/spin.py

spin_wheel = [
    "🍽️ Free Meal (up to 500 birr)",  # ultra rare
    "🎁 100 birr Gift Card",
    "🚚 Free Delivery ×5",
    "🚚 Free Delivery ×2",
    "🥤 Free Soft Drink",
    "🍿 2 Free Snacks",
    "💸 30 birr Discount",
    "😅 Try Again",
]

spin_descriptions = {
    "💸 30 birr Discount": "Save 30 birr instantly on your next order.",
    "🚚 Free Delivery ×2": "Two orders with delivery completely free.",
    "🚚 Free Delivery ×5": "Five orders with delivery completely free.",
    "🥤 Free Soft Drink": "A refreshing soft drink with your next meal.",
    "🍿 2 Free Snacks": "Two tasty snacks to enjoy with your order.",
    "🎁 100 birr Gift Card": "100 birr credit to spend on UniBites.",
    "😅 Try Again": "No prize this time, but you earn +2 bites for the leaderboard.",
    "🍽️ Free Meal (up to 500 birr)": "Jackpot! A full free meal worth up to 500 birr."
}

# Adjust weights to match the wheel order above
prizes_weights = [
    0.3,  # 🍽️ Free Meal (ultra rare)
    5,    # 🎁 Gift Card (tier 3)
    3,    # 🚚 Free Delivery ×5 (tier 3)
    20,   # 🚚 Free Delivery ×2 (tier 1)
    15,   # 🥤 Free Soft Drink (tier 2)
    15,   # 🍿 2 Free Snacks (tier 2)
    20,   # 💸 30 birr Discount (tier 1)
    3     # 😅 Try Again (tier 4)
]



# --- Helpers (replace with real DB calls) ---



# --- Easing curve generator (accelerate then decelerate) ---
def easing_delays(total_steps):
    accel = max(1, int(total_steps * 0.28))
    steady = max(1, int(total_steps * 0.44))
    decel = max(1, total_steps - accel - steady)
    delays = []
    # accelerate (decreasing delay)
    for i in range(accel):
        t = 0.06 - 0.03 * (i / accel)  # 0.06 -> 0.03
        delays.append(max(0.02, t))
    # steady
    for i in range(steady):
        delays.append(0.03 + 0.005 * (i / steady))
    # decelerate (increasing delay)
    for i in range(decel):
        t = 0.035 + 0.12 * (i / decel)  # 0.035 -> ~0.155
        delays.append(t)
    # ensure length
    if len(delays) < total_steps:
        delays += [delays[-1]] * (total_steps - len(delays))
    return delays[:total_steps]

# --- Build 3-column, 3-row vertical reels display ---
def build_1col_display(pointer, reel, jackpot, phase_text, spins_left, bites_total, rank, progress_pct):
    """
    pointer: index of the center item in the reel
    reel: list of prize strings
    """
    top = reel[(pointer - 1) % len(reel)]
    center = reel[pointer % len(reel)]
    bottom = reel[(pointer + 1) % len(reel)]

    # header: bites, spins, rank, progress bar
    header = (
        f"🏆 Rank #{rank} | Bites: **{bites_total}** | Spins left: **{spins_left}**\n"
    )

    display = (
        f"{header}\n"
        f"🎰 **{phase_text}**   Jackpot: {int(jackpot)} 💰\n\n"
        f"  {top}\n"
        f"**▶ {center} ◀**\n"
        f"  {bottom}\n\n"
        f"_Tip: big prizes are ultra rare — good luck!_"
    )
    return display

# --- Utility: determine if prize is "rare" for confetti ---
def is_rare_prize(prize: str):
    return prize == "🍽️ Free Meal (up to 500 birr)" or "🎁" in prize

# --- Main handler (UX-focused, no DB writes) ---
@router.callback_query(F.data == "spin_now")
async def spin_process_inline(call: CallbackQuery):
    telegram_id = call.from_user.id
    user_id = await db.get_user_id_by_telegram(telegram_id)
    now = datetime.utcnow()

    # cooldown
    if telegram_id in last_spin:        
        remaining = SPIN_COOLDOWN - (now - last_spin[telegram_id])
        if remaining.total_seconds() > 0:
            mins = int(remaining.total_seconds() // 60)
            await call.answer(f"⏳ Spin cooldown! Try again in {mins} minutes.", show_alert=True)
            return
    last_spin[telegram_id] = now
    await call.answer()
    
    consumed = await db.consume_spin(user_id)
    if not consumed:
        await call.answer("You don't have any spins available. Earn more bites to unlock spins.", show_alert=True) 
        await call.message.reply(
            "⚠️ You currently have 0 spins.\n\n"
            "💡 Earn spins by:\n"
            "• Ordering meals (+3 bites per order)\n"
            "• Inviting friends (+1 bite per order, +2 when they order)\n\n"
            "Each 25 bites unlocks 1 spin!"
            )
        return

    # fetch dynamic UX values (replace with real DB calls)
    spins_left, bites_total, progress = await db.get_user_spins_and_bites(user_id)
    rank, progress_pct = await db.get_user_rank_and_progress(user_id)

    # lock final prize early for fairness
    final_prize = random.choices(spin_wheel, weights=prizes_weights, k=1)[0]

    # initial message
    msg = await call.message.edit_text("🎰 **Pulling the lever...**\n\n🔊 clack!", parse_mode="Markdown")

    REEL = spin_wheel[:]
    REEL_LEN = len(REEL)

    # 3-column pointers (center index for each column)
    start_pointers = [random.randint(0, REEL_LEN - 1) for _ in range(3)]
    final_index = REEL.index(final_prize)
    # stagger final indices so columns stop sequentially but all show final prize at reveal
    final_pointers = [
        (final_index + offset) % REEL_LEN for offset in (0, 0, 0)
    ]

    # total steps: ensure each column cycles several times; we'll stop columns staggered
    base_cycles = random.randint(5, 8)
    total_steps = base_cycles * REEL_LEN + ((final_pointers[0] - start_pointers[0]) % REEL_LEN)

    delays = easing_delays(total_steps)

    pointers = start_pointers[:]
    jackpot = 1000.0
    jackpot_increment = 30.0

    last_text = None

    # determine when each column should stop (staggered)
    stop_offsets = [0, int(REEL_LEN * 0.9), int(REEL_LEN * 1.8)]  # column 0 stops first, then 1, then 2
    stop_steps = [total_steps - off for off in stop_offsets]

    for step in range(total_steps):
        jackpot += jackpot_increment * random.random()
        phase = "⚡ Accelerating…" if step < total_steps * 0.25 else \
                "🎶 Rolling steady…" if step < total_steps * 0.6 else \
                "🌀 Slowing down…" if step < total_steps * 0.95 else "✨ Final stop…"

        # advance pointers unless their stop step has passed
        for col in range(3):
            if step < stop_steps[col]:
                pointers[col] = (pointers[col] + 1) % REEL_LEN
            else:
                # gently nudge to final pointer when in final stopping window
                # compute distance to final and step towards it
                target = final_pointers[col]
                if pointers[col] != target:
                    pointers[col] = (pointers[col] + 1) % REEL_LEN

        # dynamic header values (refresh each frame)
        # If you have real-time values, fetch them here or update from cached values
        spins_display = max(0, spins_left - 1) if step == 0 else max(0, spins_left - 1)
        # show bites_total unchanged in this UX-only flow
        text = build_1col_display(pointers[0], REEL, jackpot, phase, spins_display, bites_total, rank, progress_pct)

        # add subtle confetti for rare prize when we are in final deceleration window
        if is_rare_prize(final_prize) and step > total_steps * 0.85:
            text = "🎉✨💥  " + text  # prefix confetti to the frame

        if text != last_text:
            await msg.edit_text(text, parse_mode="Markdown")
            last_text = text

        await asyncio.sleep(delays[step])

    # --- Near-miss flare (ultra rare) ---
    if final_prize == "🍽️ Free Meal (up to 500 birr)":
        # show a single-message near-miss + confetti flash before final reveal
        near_center = random.choice([p for p in REEL if p != final_prize])
        near_display = (
            f"😱 **ALMOST JACKPOT!**   🎉✨\n\n"
            f"  {near_center}   {near_center}   {near_center}\n"
            f"**▶ {final_prize} ◀**   **▶ {near_center} ◀**   **▶ {near_center} ◀**\n"
            f"  {near_center}   {near_center}   {near_center}\n\n"
            "💥 so close..."
        )
        await msg.edit_text(near_display, parse_mode="Markdown")
        await asyncio.sleep(1.0)

    # --- Final reveal in one message (confetti + description + progress) ---
    desc = spin_descriptions.get(final_prize, "")
    confetti = "🎉✨💥 " if is_rare_prize(final_prize) else ""
    final_header = (
        f"🏆 Rank #{rank}   |   Bites: **{bites_total}**   |   Spins left: **{max(0, spins_left-1)}**\n"
    )
    final_display = (
        f"{confetti}🎉 **RESULT** 🎉 {confetti}\n\n"
       f"➡️ **{final_prize}**\n\n{desc}\n\n"
        f"_Claim rules: prizes valid as described. Check Genna Specials for redemption._"
    )

    await msg.edit_text(final_header + final_display, parse_mode="Markdown")
    asyncio.create_task( notify_admin_spin( call.bot, user_id, telegram_id, final_prize, bites_total, rank, tg_first_name=call.from_user.first_name, tg_username=call.from_user.username ) )    
    # short celebratory micro-sequence for ultra-rare
    if final_prize == "🍽️ Free Meal (up to 500 birr)":
        await asyncio.sleep(0.6)
        await msg.edit_text("🎊🎊🎊 JACKPOT! 🎊🎊🎊", parse_mode="Markdown")
        await asyncio.sleep(0.6)
        await msg.edit_text(final_header + final_display, parse_mode="Markdown")

    # final CTA (returns user to campaign keyboard)
    await asyncio.sleep(0.6)
    await msg.reply(
        "Want to spin again or check your progress?\nUse the Genna Specials menu to view spins, referrals, and leaderboard.",
        reply_markup=genna_specials_menu()
    )


@router.message(F.text == "🍽️ ገና Combo")
async def ገና_combo(message: Message):
    await message.answer(
    "🍽️ *ገና Combo Reward*\n"
    "────────────────────────\n"
    "✨ Weekly Top Reward ✨\n\n"
    "At the end of this week, the users with the most Bites\n"
    "unlock the exclusive *ገና Combo*!\n\n"
    "💡 Not everyone gets it — the more Bites you collect,\n"
    "the higher your chance to win.\n\n"
    "📊 Check your *leaderboard position and keep climbing*!\n\n"
    "Keep ordering, keep referring, and climb the leaderboard!\n"
    "Your next *combo could be yours! 🏆*"
)

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from urllib.parse import quote
from urllib.parse import quote
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

async def build_referral_hub(user_id: int, db):
    async with db._open_connection() as conn:
        internal_id = await conn.fetchval(
            "SELECT id FROM users WHERE telegram_id=$1",
            user_id
        )
        if not internal_id:
            return "⚠️ You’re not registered yet. Complete onboarding to unlock referrals.", None

        referral_code = await conn.fetchval(
            "SELECT referral_code FROM users WHERE telegram_id=$1",
            user_id
        ) or "sample123"

        referrals_count = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by=$1",
            internal_id
        ) or 0

        bites_earned = await conn.fetchval(
            "SELECT bites FROM leaderboards WHERE user_id=$1",
            internal_id
        ) or 0

        rank_position = await conn.fetchval(
            """
            SELECT r FROM (
                SELECT user_id, RANK() OVER (ORDER BY bites DESC) AS r
                FROM leaderboards
            ) t WHERE user_id=$1
            """,
            internal_id
        ) or "—"

    # Build referral link
    referral_link = f"https://t.me/unibites_deliverybot?start={referral_code}"

    # Prefilled share text (with link embedded)
    share_text = (
        "👀 Most people on campus will miss this today\n\n"
        "I’m using UniBites to order food straight to my dorm\n"
        "You can win:\n"
        "🍽️ Free meal (up to 500 birr)\n"
        "🎁 Discounts & free delivery\n"
        "🥤 Free Soft Drinks & Snacks!\n\n"
        f"👉 Join with my link and let’s unlock the ገና Combo 😄 together:\n\n{referral_link}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📤 Share & Earn Bites",
                    switch_inline_query=share_text
                ),
                InlineKeyboardButton(
                    text="🎯 My Progress",
                    callback_data="referral_progress"
                )
            ]
        ]
    )

    text = (
        "👥 *Referral Hub*\n"
        "─────────────────────\n"
        f"✨ You’ve invited *{referrals_count} friends* so far!\n"
        f"🍪 Total Bites earned: *{bites_earned}*\n"
        f"🏆 Current leaderboard position: *#{rank_position}*\n\n"
        "💡 Invite more friends to climb higher and unlock the weekly *ገና Combo* 🍽️\n\n"
        "Choose an option below to share, copy your link, or check your progress:"
    )

    return text, keyboard

@router.callback_query(F.data == "referrals_main")
async def referrals_main(call: CallbackQuery):
    text, keyboard = await build_referral_hub(call.from_user.id, db)
    await call.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)


import random
import string

def generate_referral_code(user_id: int) -> str:
    # UB + padded user_id + random 3 chars
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return f"UB{str(user_id).zfill(4)}{suffix}"

@router.message(F.text == "👥 Referrals")
async def my_referrals(message: Message):
    user_id = message.from_user.id

    async with db._open_connection() as conn:
        # check if referral_code exists
        referral_code = await conn.fetchval(
            "SELECT referral_code FROM users WHERE telegram_id=$1",
            user_id
        )
        if not referral_code:
            # fetch internal id
            internal_id = await conn.fetchval(
                "SELECT id FROM users WHERE telegram_id=$1",
                user_id
            )
            if internal_id:
                new_code = generate_referral_code(internal_id)
                await conn.execute(
                    """
                    UPDATE users
                    SET referral_code=$1
                    WHERE id=$2
                    """,
                    new_code, internal_id
                )
                referral_code = new_code

    # now build referral hub
    text, keyboard = await build_referral_hub(user_id, db)
    await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

    
    
from typing import Optional, Dict, Any
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

async def get_referral_stats(db, telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns a dictionary with referral and leaderboard stats for the given telegram_id.
    Returns None if the user is not registered.
    Keys:
      - internal_id: users.id (int)
      - referral_code: str
      - referrals_count: int
      - bites_earned: int
      - rank_position: int or "—"
      - referral_link: str
      - share_text: str
    """
    async with db._open_connection() as conn:
        internal_id = await conn.fetchval(
            "SELECT id FROM users WHERE telegram_id=$1",
            telegram_id
        )
        if not internal_id:
            return None

        # Use a single query to fetch referral_code, referrals_count, bites and rank
        row = await conn.fetchrow(
            """
            WITH u AS (
                SELECT id, referral_code
                FROM users
                WHERE telegram_id = $1
            ), referrals AS (
                SELECT COUNT(*) AS referrals_count
                FROM users
                WHERE referred_by = (SELECT id FROM u)
            ), bites AS (
                SELECT COALESCE(bites, 0) AS bites
                FROM leaderboards
                WHERE user_id = (SELECT id FROM u)
            ), r AS (
                SELECT user_id,
                       RANK() OVER (ORDER BY bites DESC, last_updated ASC) AS rnk
                FROM leaderboards
            )
            SELECT
                (SELECT id FROM u) AS internal_id,
                (SELECT referral_code FROM u) AS referral_code,
                (SELECT referrals_count FROM referrals) AS referrals_count,
                (SELECT bites FROM bites) AS bites_earned,
                COALESCE((SELECT rnk FROM r WHERE user_id = (SELECT id FROM u)), NULL) AS rank_position
            """,
            telegram_id
        )

    # Normalize results
    referral_code = row["referral_code"] or ""
    referrals_count = int(row["referrals_count"] or 0)
    bites_earned = int(row["bites_earned"] or 0)
    rank_position = row["rank_position"] if row["rank_position"] is not None else "—"
    internal_id = int(row["internal_id"])

    referral_link = f"https://t.me/unibites_deliverybot?start={referral_code}"

    share_text = (
        "👀 Most people on campus will miss this today\n\n"
        "I’m using UniBites to order food straight to my dorm\n"
        "You can win:\n"
        "🍽️ Free meal (up to 500 birr)\n"
        "🎁 Discounts & free delivery\n"
        "🥤 Free Soft Drinks & Snacks!\n\n"
        f"👉 Join with my link and let’s unlock the ገና Combo 😄 together:\n\n{referral_link}"
    )

    return {
        "internal_id": internal_id,
        "referral_code": referral_code,
        "referrals_count": referrals_count,
        "bites_earned": bites_earned,
        "rank_position": rank_position,
        "referral_link": referral_link,
        "share_text": share_text,
    }


def referral_progress_bar(current: int, goal: int = 25, length: int = 12) -> str:
    """
    Returns a simple text progress bar. Safe for goal == 0.
    """
    if goal <= 0:
        return "░" * length
    filled = min(length, int((current / goal) * length))
    filled = max(0, filled)
    empty = length - filled
    return "█" * filled + "░" * empty

def referral_goal_info(referrals_count: int, goal: int = 5) -> Dict[str, Any]:
    """
    Returns progress numbers for display.
    """
    bar = referral_progress_bar(referrals_count, goal)
    return {
        "goal": goal,
        "bar": bar,
        "current": referrals_count,
    }


from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

async def build_referral_hub_from_stats(db, telegram_id: int, goal: int = 25):
    """
    Returns (text, keyboard) ready to send to the user.
    Uses get_referral_stats and the progress helpers.
    """
    stats = await get_referral_stats(db, telegram_id)
    if stats is None:
        return "⚠️ You’re not registered yet. Complete onboarding to unlock referrals.", None

    progress = referral_goal_info(stats["referrals_count"], goal)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📤 Share & Earn Bites",
                    switch_inline_query=stats["share_text"]
                ),
                
                InlineKeyboardButton(
                    text="🎯 My Progress",
                    callback_data="referral_progress"
                )
            ]
        ]
    )

    text = (
        "👥 *Referral Hub*\n"
        "─────────────────────\n"
        f"✨ You’ve invited *{stats['referrals_count']} friends* so far!\n"
        f"🍪 Total Bites earned: *{stats['bites_earned']}*\n"
        f"🏆 Current leaderboard position: *#{stats['rank_position']}*\n\n"
        f"🎯 Goal: Invite {progress['goal']} friends to unlock the **UniBites Spin** 💫\n"
        f"Progress: {progress['bar']} {progress['current']}/{progress['goal']}\n\n"
        "💡 Invite more friends to climb higher and unlock the weekly *ገና Combo* 🍽️\n\n"
        "Choose an option below to share, copy your link, or check your progress:"
    )

    return text, keyboard


async def build_referral_progress(user_id: int, db):
    async with db._open_connection() as conn:
        referrals_count = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE referred_by=$1",
            user_id
        ) or 0
        bites_earned = await conn.fetchval(
            "SELECT bites FROM leaderboards WHERE user_id=$1",
            user_id
        ) or 0
        rank_position = await conn.fetchval(
            "SELECT rank FROM leaderboards WHERE user_id=$1",
            user_id
        ) or "—"

    # Example goal logic: unlock combo after 5 referrals
    next_goal = 25

    def progress_bar(current, goal, length=12):
        filled = int((current / goal) * length) if goal > 0 else 0
        empty = length - filled
        return "█" * filled + "░" * empty

    bar = progress_bar(referrals_count, next_goal)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔙 Back",
                    callback_data="referrals_main"
                )
            ]
        ]
    )

    text = (
        "🎯 **My Referral Progress**\n"
        "──────────────────────────\n"
        f"👥 Friends invited: **{referrals_count}**\n"
        f"🍪 Bites earned: **{bites_earned}**\n"
        f"🏆 Current leaderboard position: **#{rank_position}**\n\n"
        f"🎯 Goal: Invite {next_goal} friends to unlock the **UniBites Spin** 🍽️\n"
        f"Progress: {bar} {referrals_count}/{next_goal}\n\n"
        "Keep sharing your link to climb higher and unlock rewards!"
    )

    return text, keyboard



@router.callback_query(F.data == "referral_progress")
async def referral_progress(call: CallbackQuery):
    text, keyboard = await build_referral_hub_from_stats(db, call.from_user.id)
    # If you want a dedicated progress view, you can build a smaller text using the same stats
    await call.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)


@router.message(F.text == "📊 Leaderboard")
async def leaderboard(message: Message):
    user_id = await db.get_user_id_by_telegram(message.from_user.id)
    if not user_id:
        await message.answer("⚠️ You’re not registered yet.")
        return

    async with db._open_connection() as conn:
        # Fetch top 10 users ordered by bites
        rows = await conn.fetch(
    """
    SELECT DISTINCT ON (user_id) user_id, display_name, bites
    FROM leaderboards
    ORDER BY user_id, bites DESC, last_updated DESC
    LIMIT 10
    """
)


        # Fetch current user rank
        user_rank = await conn.fetchval(
            """
            SELECT rank FROM (
                SELECT user_id,
                       RANK() OVER (ORDER BY bites DESC, last_updated ASC) AS rank
                FROM leaderboards
            ) sub WHERE user_id=$1
            """,
            user_id
        )
        user_bites = await conn.fetchval(
            "SELECT bites FROM leaderboards WHERE user_id=$1", user_id
        ) or 0

    # Build leaderboard text
    text = "📊 UniBites Weekly Leaderboard\n"
    text += "──────────────────────────────\n"
    text += "🏆 Top Bites Collectors 🏆\n\n"

    if not rows:
        text += "No entries yet. Be the first to climb the board! 🚀\n\n"
    else:
        medals = ["🥇", "🥈", "🥉"]
        for idx, row in enumerate(rows, start=1):
            medal = medals[idx-1] if idx <= 3 else "🏅"
            text += f"{medal} {row['display_name']} — {row['bites']} Bites🍪\n"

    if user_rank:
        text += f"\n🔥 You’re currently #{user_rank} with {user_bites} Bites!\n"
    else:
        text += f"\n🔥 You’re not on the board yet. Collect bites to join the race!\n"

    text += "\nKeep ordering & referring to climb higher.\n"
    text += "The next ገና Combo could be yours! 🍽️"

    # Add refresh button
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh Leaderboard", callback_data="refresh_leaderboard")]
        ]
    )

    await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")


@router.callback_query(F.data == "refresh_leaderboard")
async def refresh_leaderboard(call: CallbackQuery):
    # Reuse the same logic as above
    user_id = await db.get_user_id_by_telegram(call.from_user.id)
    if not user_id:
        await call.answer("⚠️ You’re not registered yet.", show_alert=True)
        return

    async with db._open_connection() as conn:
        rows = await conn.fetch(
    """
    SELECT DISTINCT ON (user_id) user_id, display_name, bites
    FROM leaderboards
    ORDER BY user_id, bites DESC, last_updated DESC
    LIMIT 10
    """
)

        user_rank = await conn.fetchval(
            """
            SELECT rank FROM (
                SELECT user_id,
                       RANK() OVER (ORDER BY bites DESC, last_updated ASC) AS rank
                FROM leaderboards
            ) sub WHERE user_id=$1
            """,
            user_id
        )
        user_bites = await conn.fetchval(
            "SELECT bites FROM leaderboards WHERE user_id=$1", user_id
        ) or 0

    text = "📊 UniBites Weekly Leaderboard\n"
    text += "──────────────────────────────\n"
    text += "🏆 Top Bites Collectors 🏆\n\n"

    if not rows:
        text += "No entries yet. Be the first to climb the board! 🚀\n\n"
    else:
        medals = ["🥇", "🥈", "🥉"]
        for idx, row in enumerate(rows, start=1):
            medal = medals[idx-1] if idx <= 3 else "🏅"
            text += f"{medal} {row['display_name']} — {row['bites']} Bites🍪\n"

    if user_rank:
        text += f"\n🔥 You’re currently #{user_rank} with {user_bites} Bites!\n"
    else:
        text += f"\n🔥 You’re not on the board yet. Collect bites to join the race!\n"

    text += "\nKeep ordering & referring to climb higher.\n"
    text += "The next ገና Combo could be yours! 🍽️"

    try:
        await call.message.edit_text(text, reply_markup=call.message.reply_markup, parse_mode="Markdown")
    except aiogram.exceptions.TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await call.answer("Already up to date ✅", show_alert=True)
        else:
            raise



@router.message(F.text == "ℹ️ How It Works")
async def campaign_info(message: Message):
    await message.answer(
        "ℹ️ How ገና Specials Work\n"
        "────────────────────────\n"
        "• Earn Bites by ordering\n"
        "  and referring friends\n\n"
        "• Use Bites to spin\n"
        "  for instant rewards\n\n"
        "• Top users each week\n"
        "  win the ገና Combo\n\n"
        "ገና is competitive —\n"
        "stay active to win."
    )
