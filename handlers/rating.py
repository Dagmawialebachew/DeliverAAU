# handlers/rating.py
import contextlib
import logging
from typing import Dict
from aiogram import Bot, Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters import Command
from config import settings
from app_context import db
from database.db import Database
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)

from handlers.onboarding import build_profile_card, main_menu
router = Router()

# --- States ---
class RatingStates(StatesGroup):
    rate_comment = State()



# --- Inline Keyboards ---
def delivery_rating_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="⭐1", callback_data=f"rate_delivery:{order_id}:1"),
            InlineKeyboardButton(text="⭐2", callback_data=f"rate_delivery:{order_id}:2"),
            InlineKeyboardButton(text="⭐3", callback_data=f"rate_delivery:{order_id}:3"),
            InlineKeyboardButton(text="⭐4", callback_data=f"rate_delivery:{order_id}:4"),
            InlineKeyboardButton(text="⭐5", callback_data=f"rate_delivery:{order_id}:5"),
        ]]
    )

def vendor_rating_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="⭐1", callback_data=f"rate_vendor:{order_id}:1"),
            InlineKeyboardButton(text="⭐2", callback_data=f"rate_vendor:{order_id}:2"),
            InlineKeyboardButton(text="⭐3", callback_data=f"rate_vendor:{order_id}:3"),
            InlineKeyboardButton(text="⭐4", callback_data=f"rate_vendor:{order_id}:4"),
            InlineKeyboardButton(text="⭐5", callback_data=f"rate_vendor:{order_id}:5"),
        ]]
    )

@router.callback_query(F.data.startswith("rate_delivery:"))
async def rate_delivery(cb: CallbackQuery, state: FSMContext):
    _, order_id, stars = cb.data.split(":")
    order_id, stars = int(order_id), int(stars)

    # Save rating
    async with db._open_connection() as conn:
        await conn.execute(
            "INSERT INTO ratings (order_id, stars, type) VALUES ($1, $2, 'delivery') "
            "ON CONFLICT (order_id, type) DO UPDATE SET stars = EXCLUDED.stars",
            order_id, stars
        )

    # Reward delivery guy if 5 stars + reward student +10 XP
    order = await db.get_order(order_id)
    delivery_guy_id = order.get("delivery_guy_id")
    student_id = order.get("user_id")

    async with db._open_connection() as conn:
        if student_id:
            await conn.execute("UPDATE users SET xp = xp + 10 WHERE id=$1", student_id)
        if delivery_guy_id and stars == 5:
            await conn.execute("UPDATE delivery_guys SET xp = xp + 10 WHERE id=$1", delivery_guy_id)

    await cb.answer("✅ Thanks for rating the delivery!")

    # Notify delivery guy
    if delivery_guy_id:
        dg_chat_id = await db.get_delivery_guy_telegram_id_by_id(delivery_guy_id)
        if dg_chat_id:
            await cb.bot.send_message(
                dg_chat_id,
                f"⭐ You received a {stars}-star rating for Order #{order_id}!\n🔥 +10 XP awarded!"
                if stars == 5 else f"⭐ You received a {stars}-star rating for Order #{order_id}."
            )

    # Notify admin
    if settings.ADMIN_DAILY_GROUP_ID:
        await cb.bot.send_message(
            settings.ADMIN_DAILY_GROUP_ID,
            f"📢 Rating submitted for Order #{order_id}\n"
            f"👤 Delivery Guy ID: {delivery_guy_id}\n"
            f"⭐ Stars: {stars}\n"
            f"{'🔥 XP reward applied' if stars == 5 else ''}"
        )

    # Offer optional comment
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📝 Add Comment", callback_data=f"rate_comment:{order_id}:delivery")]]
    )
    await cb.message.edit_text(
        f"⭐ You rated the delivery {stars} stars.\nWant to add a comment?",
        reply_markup=kb
    )

# --- Vendor Rating Handlers ---
@router.callback_query(F.data.startswith("rate_vendor:"))
async def rate_vendor(cb: CallbackQuery, state: FSMContext):
    _, order_id, stars = cb.data.split(":")
    order_id, stars = int(order_id), int(stars)

    async with db._open_connection() as conn:
        await conn.execute(
            "INSERT INTO ratings (order_id, stars, type) VALUES ($1, $2, 'vendor') "
            "ON CONFLICT (order_id, type) DO UPDATE SET stars = EXCLUDED.stars",
            order_id, stars
        )

        # Update vendor averages
        await conn.execute(
            """
            UPDATE vendors
            SET rating_avg = (rating_avg * rating_count + $1) / (rating_count + 1),
                rating_count = rating_count + 1
            WHERE id = (SELECT vendor_id FROM orders WHERE id=$2)
            """,
            stars, order_id
        )

    await cb.answer("✅ Thanks for rating your overall experience!")

    # Notify admin
    if settings.ADMIN_DAILY_GROUP_ID:
        await cb.bot.send_message(
            settings.ADMIN_DAILY_GROUP_ID,
            f"📢 Vendor rating submitted for Order #{order_id}\n"
            f"⭐ Stars: {stars}"
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📝 Add Comment", callback_data=f"rate_comment:{order_id}:vendor")]]
    )
    await cb.message.edit_text(
        f"⭐ You rated your overall experience {stars} stars.\nWant to add a comment?",
        reply_markup=kb
    )


# --- Comment Flow ---
@router.callback_query(F.data.startswith("rate_comment:"))
async def rate_comment_start(cb: CallbackQuery, state: FSMContext):
    _, order_id, rate_type = cb.data.split(":")
    await state.update_data(rate_order_id=int(order_id), rate_type=rate_type)
    await cb.message.edit_text("✍️ Please type your comment:")
    await state.set_state(RatingStates.rate_comment)

@router.message(RatingStates.rate_comment)
async def rate_comment_save(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("rate_order_id")
    rate_type = data.get("rate_type")
    comment = (message.text or "").strip()

    async with db._open_connection() as conn:
        await conn.execute(
            "UPDATE ratings SET comment=$1 WHERE order_id=$2 AND type=$3",
            comment, order_id, rate_type
        )

    # Confirm to student
    await message.answer("✅ Thanks for your feedback! 🙏")

    # Notify admin group
    if settings.ADMIN_DAILY_GROUP_ID:
        await message.bot.send_message(
            settings.ADMIN_DAILY_GROUP_ID,
            f"📝 New {rate_type} comment for Order #{order_id}\n"
            f"👤 From User ID: {message.from_user.id}\n"
            f"💬 Comment: {comment}"
        )

    await state.clear()







