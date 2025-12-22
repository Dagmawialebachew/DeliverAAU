# -*- coding: utf-8 -*-
import asyncio
import logging
from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from config import settings
from database.db import Database
from utils.helpers import typing_pause, format_phone_number

# Handoff to vendor dashboard (Amharic UX)
from handlers import vendor as vendor_handler

router = Router()
# CHANGED: Use Database() that reads DATABASE_URL from environment
from app_context import db


class OnboardingStates(StatesGroup):
    share_phone = State()
    choose_campus = State()
    choose_gender = State()



# --- REUSABLE UI COMPONENTS ---

def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📲 Share my phone number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Share your phone number to continue"
    )


def gender_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👨 Male", callback_data="gender:male"),
                InlineKeyboardButton(text="👩 Female", callback_data="gender:female"),
            ]
        ]
    )


def campus_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏛 4kilo", callback_data="campus:4kilo"), InlineKeyboardButton(text="📚 5kilo", callback_data="campus:5kilo")],
            [InlineKeyboardButton(text="🎓 6kilo", callback_data="campus:6kilo"), InlineKeyboardButton(text="💹 FBE", callback_data="campus:FBE")],
        ]
    )


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🛒 Order"),
                KeyboardButton(text="📍 Track Order"),
            ],
            [
                KeyboardButton(text="🧑‍🍳 Need Help"),
                KeyboardButton(text="⚙️ More Options"),
            ],
        ],
        resize_keyboard=True,
        input_field_placeholder="Choose an option below 👇",
    )


def more_menu() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🎁 Redeem Coins"), KeyboardButton(text="🪙 Subscriptions")],
        [KeyboardButton(text="⚙️ Settings"), KeyboardButton(text="⬅️ Back")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)



@router.message(F.text == "🎁 Redeem Coins")
async def redeem_coins(message: Message):
    await message.answer(
        "🎬 Welcome to UniBites Delivery Rewards!\n\n"
        "✨ Redeem Coins feature is coming soon...\n"
        "Stay tuned for campus-first perks and surprises!"
    )

# Subscriptions
@router.message(F.text == "🪙 Subscriptions")
async def subscriptions(message: Message):
    await message.answer(
        "📦 Subscription plans are coming soon!\n"
        "You’ll be able to unlock premium campus delivery perks."
    )
   

 


import random

STUDENT_TIPS = [
    "🍔 Order anytime: Tap your favorite café and get food delivered fast.",
    "🛵 Track live: Watch your delivery guy move in real‑time on campus.",
    "🎉 Keep streaks alive: Daily orders = more XP, coins & badges.",
    "💰 Earn rewards: Coins unlock perks — save them for special treats.",
    "🏆 Level up: Higher levels = cooler badges & bragging rights.",
    "📊 Check progress: Your dashboard shows coins, XP & level at a glance.",
    "🤝 Support local: Every order helps student vendors & cafés grow.",
    "✨ Stay playful: Emojis, streaks & badges make ordering fun, not boring.",
    "📱 Quick actions: Use the menu buttons to jump straight to vendors or status.",
    "🎓 Campus ritual: UniBites isn’t just delivery — it’s part of student life."
]


def build_profile_card(user: dict, role: str = "student") -> str:
    """Reusable profile card for all roles."""
    coins = user.get("coins", 0)
    xp = user.get("xp", 0)
    level = user.get("level", 1)
    bar_length = 15
    filled = int(level)
    
    progress_bar = "▰" * filled + "▱" * (bar_length - filled)

    tip = random.choice(STUDENT_TIPS)

    if role == "delivery_guy":
        return (
            "🆔 **Delivery Partner Card**\n"
            f"👤 {user.get('name', user.get('first_name', 'Unknown'))}\n"
            f"🏛 {user.get('campus', 'N/A')}\n"
            f"📦 Deliveries: {user.get('total_deliveries', 0)}\n"
            # CHANGED: Postgres boolean
            f"⚡ Status: {'🟢 Online' if user.get('active', False) else '🔴 Offline'}\n\n"
            f"💰 Coins: {coins} • 🏆 XP: {xp} • 🔰 Level: {level}\n"
            f"{progress_bar}\n\n"
            "🚴 Keep hustling — every delivery powers your reputation ⚡"
        )

    badge = get_xp_badge(level)

    return (
        f"🎉 **Welcome, {user.get('first_name', 'User')}!**\n\n"
        f"📱 Phone: {user.get('phone', 'N/A')}\n"
        f"🏛 Campus: {user.get('campus', 'N/A')}\n"
        f"🎓 Role: {role.capitalize()}\n\n"
        f"💰 Coins: {coins}\n"
        f"🏆 XP: {xp} • 🔰 Level {level} • {badge}\n"
        f"{progress_bar}  \n({xp % 100}/100 XP to next level)\n\n"
        f"💡 Tip: {tip}"
    )

def get_xp_badge(level: int) -> str:
    if level >= 6:
        return "🟣 VIP"
    elif level >= 3:
        return "🔵 Regular"
    else:
        return "🟢 Newbie"



# --- MENUS ---

@router.message(F.text == "⚙️ More Options")
async def show_more_menu(message: Message):
    await message.answer("🔎 Explore more options 👇", reply_markup=more_menu())


@router.message(F.text == "⬅️ Back")
async def back_to_main(message: Message):
    user = await db.get_user(message.from_user.id)
    if user:
        await message.answer(build_profile_card(user), parse_mode="Markdown")
    await message.answer("⬅️ Back to your main dashboard 👇", reply_markup=main_menu())


# --- ONBOARDING FLOW ---

@router.message(CommandStart())
async def start(message: Message, state: FSMContext):
    telegram_id = message.from_user.id

    # --- VENDOR EXPERIENCE (added manually by admin in vendors table) ---
    vendor = await db.get_vendor_by_telegram(telegram_id)
    if vendor:
        await typing_pause(message, "🏪 እንኳን በደህና መጡ — የሱቅዎ ዳሽቦርድ ዝግጁ ነው!")
        await asyncio.sleep(0.4)
        await message.answer(
            f"⭐ አማካይ ደረጃ: {float(vendor.get('rating_avg', 3.00)):.1f} "
            f"({int(vendor.get('rating_count', 0))} አስተያየት)",
            parse_mode="HTML"
        )
        # Handoff to vendor’s Amharic dashboard
        await message.answer("እነዚን ቁልፎች ይጠቀሙ 👇", reply_markup=vendor_handler.vendor_dashboard_keyboard())
        await state.clear()
        return

    # --- DELIVERY GUY EXPERIENCE (added manually; delivery_guys.user_id = telegram_id) ---
    user = await db.get_user(telegram_id)
    delivery_guy = await db.get_delivery_guy_by_user_onboard(telegram_id)
    print('here is a delivery guy', delivery_guy)
    if delivery_guy or (user and (user.get("role") or "").lower() == "delivery_guy"):
        await typing_pause(message, "🚴‍♂️ Welcome back, Campus Star!")
        await asyncio.sleep(0.4)
        await typing_pause(message, "⚡ You keep the campus heartbeat alive — fast meals, real smiles, pure hustle.")

        await message.answer(build_profile_card(delivery_guy or user, role="delivery_guy"), parse_mode="Markdown")

        from handlers import delivery_guy as dg_handler
        is_online = bool(delivery_guy.get("active", False))

        await message.answer("🛠 Control Center ready 👇", reply_markup=dg_handler.dashboard_reply_keyboard(is_online=is_online))
        await state.clear()
        return

    # --- RETURNING STUDENT EXPERIENCE ---
    if user:
        from datetime import datetime

        hour = datetime.now().hour

        if hour < 12:
            greeting = "🌞 Good morning!"
        elif hour < 18:
            greeting = "🌤 Good afternoon!"
        else:
            greeting = "🌙 Good evening!"

        await message.answer(
            f"{greeting} \n\n👋 Welcome back to UniBites Delivery ",
            parse_mode="HTML"
        )
        await message.answer(build_profile_card(user), parse_mode="Markdown")
        await message.answer("Choose your next move:", reply_markup=main_menu())
        await state.clear()
        return

    # --- NEW STUDENT ONBOARDING ---
    await typing_pause(message, "🌟 Welcome to **UniBites Delivery** — where campus life meets effortless delivery.")
    await asyncio.sleep(0.6)
    await typing_pause(message, "Let’s build your profile together 🚀")

    # Progress animation
    progress_msg = await message.answer("▱▱▱ Initializing…")
    for stage in ["▰▱▱ Setting up…", "▰▰▱ Almost there…", "▰▰▰ Ready!"]:
        await asyncio.sleep(0.6)
        await progress_msg.edit_text(stage)

    await typing_pause(message, "📍 Step 1 of 3 — Verify your phone number")
    await asyncio.sleep(0.3)
    await message.answer("Tap below to share instantly 👇", reply_markup=contact_keyboard())
    await state.set_state(OnboardingStates.share_phone)


# --- STEP 2: PHONE HANDLING ---

@router.message(OnboardingStates.share_phone, F.contact)
async def handle_contact(message: Message, state: FSMContext):
    phone = format_phone_number(message.contact.phone_number)
    await state.update_data(phone=phone)

    await message.answer("✅ Phone linked successfully!", reply_markup=ReplyKeyboardRemove())
    await asyncio.sleep(0.4)
    await typing_pause(message, "🏛 Step 2 of 3 — Choose your campus")
    await message.answer("Select your home base 👇", reply_markup=campus_inline_keyboard())
    await state.set_state(OnboardingStates.choose_campus)


# --- STEP 3: CAMPUS SELECTION ---
@router.callback_query(OnboardingStates.choose_campus, F.data.startswith("campus:"))
async def handle_campus(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    campus = cb.data.split(":")[1]
    data = await state.get_data()
    phone = data.get("phone", "")

    await state.update_data(campus=campus, phone=phone)

    await cb.message.answer("⚧ Step 3 of 4 — Select your gender", reply_markup=gender_inline_keyboard())
    await state.set_state(OnboardingStates.choose_gender)



@router.callback_query(OnboardingStates.choose_gender, F.data.startswith("gender:"))
async def handle_gender(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    gender = cb.data.split(":")[1]
    data = await state.get_data()

    # 🎁 Give starter coins
    starter_xp = 50

    await db.create_user(
        telegram_id=cb.from_user.id,
        role="student",
        first_name=cb.from_user.first_name or "",
        phone=data.get("phone", ""),
        campus=data.get("campus", ""),
        gender=gender,
        xp=starter_xp,   # <-- reward XP on creation
    )

    await cb.message.answer("✅ Registration complete!")
    await asyncio.sleep(0.5)
    await typing_pause(cb.message, "🌱 Finalizing your profile…")
    await asyncio.sleep(0.7)

    new_user = {
        "first_name": cb.from_user.first_name,
        "phone": data.get("phone", ""),
        "campus": data.get("campus", ""),
        "coins": 50,   # <-- reflect reward
        "xp": starter_xp,
        "level": 1,
        "gender": gender,
    }

    # 🎉 Tell them about the reward
    await cb.message.answer(
        f"🎁 Surprise! You’ve been gifted **{starter_xp} XP and 50 Coins** to kickstart your UniBites journey.\n"
        "Use them later to unlock perks and play around with rewards ✨"
    )

    await cb.message.answer(build_profile_card(new_user), parse_mode="Markdown")
    await asyncio.sleep(0.8)
    await cb.message.answer("Your dashboard is live 👇", reply_markup=main_menu())
    await state.clear()



# --- FALLBACKS ---

@router.message(OnboardingStates.share_phone)
@router.message(OnboardingStates.choose_campus)
async def block_random_input(message: Message):
    await message.answer(
        "🙏 Please finish the setup using the buttons below.\n"
        "We’ll unlock your dashboard once you’re done 🎯"
    )
