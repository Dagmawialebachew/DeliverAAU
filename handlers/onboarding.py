# -*- coding: utf-8 -*-
import asyncio
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
db = Database(settings.DB_PATH)


class OnboardingStates(StatesGroup):
    share_phone = State()
    choose_campus = State()


# --- REUSABLE UI COMPONENTS ---

def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📲 Share my phone number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Share your phone number to continue"
    )


def campus_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏛 4kilo", callback_data="campus:4kilo")],
            [InlineKeyboardButton(text="📚 5kilo", callback_data="campus:5kilo")],
            [InlineKeyboardButton(text="🎓 6kilo", callback_data="campus:6kilo")],
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
        [KeyboardButton(text="💰 My Coins"), KeyboardButton(text="🪙 Subscriptions")],
        [KeyboardButton(text="⚙️ Settings"), KeyboardButton(text="⬅️ Back")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


# --- PROFILE CARD BUILDER ---

def build_profile_card(user: dict, role: str = "student") -> str:
    """Reusable profile card for all roles."""
    coins = user.get("coins", 0)
    xp = user.get("xp", 0)
    level = user.get("level", 1)
    filled = int((xp % 100) / 10)
    progress_bar = "▰" * filled + "▱" * (10 - filled)

    if role == "delivery_guy":
        return (
            "🆔 **Delivery Partner Card**\n"
            f"👤 {user.get('name', user.get('first_name', 'Unknown'))}\n"
            f"🏛 {user.get('campus', 'N/A')}\n"
            f"📦 Deliveries: {user.get('total_deliveries', 0)}\n"
            f"⚡ Status: {'🟢 Active' if user.get('active', 1) else '🔴 Inactive'}\n\n"
            f"💰 Coins: {coins} • 🏆 XP: {xp} • 🔰 Level: {level}\n"
            f"{progress_bar}\n\n"
            "🚴 Keep hustling — every delivery powers your reputation ⚡"
        )

    return (
        f"🎉 **Welcome, {user.get('first_name', 'User')}!**\n\n"
        f"📱 Phone: {user.get('phone', 'N/A')}\n"
        f"🏛 Campus: {user.get('campus', 'N/A')}\n"
        f"🎓 Role: {role.capitalize()}\n\n"
        f"💰 Coins: {coins} • 🏆 XP: {xp} • 🔰 Level: {level}\n"
        f"{progress_bar}\n\n"
        "✨ Every order, every streak, every badge grows your impact!"
    )


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
            f"⭐ አማካይ ደረጃ: {float(vendor.get('rating_avg', 0.0)):.1f} "
            f"({int(vendor.get('rating_count', 0))} አስተያየት)",
            parse_mode="HTML"
        )
        # Handoff to vendor’s Amharic dashboard
        await message.answer("ይህን ምናሌ ተጠቀሙ 👇", reply_markup=vendor_handler.vendor_dashboard_keyboard())
        await state.clear()
        return

    # --- DELIVERY GUY EXPERIENCE (added manually; delivery_guys.user_id = telegram_id) ---
    user = await db.get_user(telegram_id)
    delivery_guy = None if user else await db.get_delivery_guy_by_user(telegram_id)
    if delivery_guy or (user and (user.get("role") or "").lower() == "delivery_guy"):
        await typing_pause(message, "🚴‍♂️ Welcome back, Campus Hero!")
        await asyncio.sleep(0.4)
        await typing_pause(message, "⚡ You keep the campus heartbeat alive — fast meals, real smiles, pure hustle.")

        await message.answer(build_profile_card(delivery_guy or user, role="delivery_guy"), parse_mode="Markdown")

        from handlers import delivery_guy as dg_handler
        await message.answer("🛠 Control Center ready 👇", reply_markup=dg_handler.dashboard_reply_keyboard())
        await state.clear()
        return

    # --- RETURNING STUDENT EXPERIENCE ---
    if user:
        await typing_pause(message, "👋 Welcome back to **Deliver AAU** 🎉")
        await asyncio.sleep(0.3)
        await typing_pause(message, "Fuel your day, support your peers — fast, easy, right from your campus 🏛")

        await message.answer(build_profile_card(user), parse_mode="Markdown")
        await message.answer("Choose your next move:", reply_markup=main_menu())
        await state.clear()
        return

    # --- NEW STUDENT ONBOARDING ---
    await typing_pause(message, "🌟 Welcome to **Deliver AAU** — where campus life meets effortless delivery.")
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

    await db.create_user(
        telegram_id=cb.from_user.id,
        role="student",
        first_name=cb.from_user.first_name or "",
        phone=phone,
        campus=campus,
    )

    try:
        if settings.ADMIN_GROUP_ID:
            await cb.bot.send_message(
                settings.ADMIN_GROUP_ID,
                f"📢 New student joined Deliver AAU!\n\n"
                f"👤 Name: {cb.from_user.first_name}\n"
                f"📱 Phone: {phone}\n"
                f"🏛 Campus: {campus}",
            )
    except Exception:
        pass

    await cb.message.answer("✅ Registration complete!")
    await asyncio.sleep(0.5)
    await typing_pause(cb.message, "🌱 Step 3 of 3 — Setting up your profile…")
    await asyncio.sleep(0.7)

    new_user = {
        "first_name": cb.from_user.first_name,
        "phone": phone,
        "campus": campus,
        "coins": 0,
        "xp": 0,
        "level": 1,
    }

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
