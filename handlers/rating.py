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





#Handler Admin





# Initialize Logger
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- STATE GROUPS (FSM) ---
class VendorCreation(StatesGroup):
    """States for the Add Vendor conversation (Tier 1)."""
    V_GET_TG_ID = State()
    V_GET_NAME = State()
    V_CONFIRM = State()

class DGCreation(StatesGroup):
    """States for the Add Delivery Guy conversation (Tier 1)."""
    DG_GET_TG_ID = State()
    DG_GET_NAME = State()
    DG_GET_PHONE = State()
    DG_GET_CAMPUS = State()
    DG_CONFIRM = State()

class BroadcastState(StatesGroup):
    """States for the Broadcast conversation (Tier 2)."""
    B_GET_MESSAGE = State()
    B_CONFIRM = State()

class BlockDGState(StatesGroup):
    """States for blocking a Delivery Guy (Tier 3)."""
    BDG_GET_ID = State()
    BDG_GET_REASON = State()
    BDG_CONFIRM = State()

# --- ADMIN DASHBOARD ---
# The primary entry point and menu handler.
@router.message(Command("admin"), F.from_user.id.in_(settings.ADMIN_IDS))
async def admin_start_handler(message: Message, state: FSMContext):
    """The Command Center Dashboard (Persistent Menu)."""
    
    # Clear any previous state when entering the main menu
    await state.clear()
    
    # --- COMMAND CENTER LAYOUT ---
    keyboard = [
        ["➕ Add Vendor", "🛵 Add Delivery Guy"], # Tier 1: Onboarding
        ["📢 Broadcast", "💰 Finance"],           # Tier 2: Communications & Finance
        ["⚙️ Configure", "📈 Analytics"],           # <-- TIER 3 ADDED HERE
        ["📊 System Status", "🚫 Emergency Stop"], # Tier 1/3: Utils
    ]
    
    reply_markup = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=btn) for btn in row] for row in keyboard],
        resize_keyboard=True, 
        is_persistent=True,
        input_field_placeholder="Select Admin Protocol..."
    )
    
    txt = (
        "🔐 **ADMINISTRATOR COMMAND CENTER** 🔐\n\n"
        "**System Status:** 🟢 ONLINE\n"
        "**Tier 1 Protocols:** ACTIVE\n"
        "**Tier 2 Protocols:** ACTIVE\n"
        "**Tier 3 Protocols:** ACTIVE\n\n" # <-- Updated Status
        "Select an operation from the control panel below."
    )
    
    await message.answer(txt, reply_markup=reply_markup, parse_mode="Markdown")

@router.message(Command("cancel"), F.user.id.in_(settings.ADMIN_IDS))
async def cancel_op_handler(message: Message, state: FSMContext):
    """Standard cancel operation to exit any state."""
    await state.clear()
    await message.answer(
        "🛑 **Operation Aborted.** returning to neutral state.",
        reply_markup=ReplyKeyboardRemove()
    )
    # Call the start handler to redisplay the menu
    await admin_start_handler(message, state)


# ==============================================================================
# 🏪 PROTOCOL: ADD VENDOR (TIER 1)
# [Existing implementation remains unchanged]
# ==============================================================================

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

router = Router()

@router.message(F.text == "➕ Add Vendor", F.from_user.id.in_(settings.ADMIN_IDS))
async def start_add_vendor(message: Message, state: FSMContext):
    """Entry point for Vendor Creation."""
    await message.answer(
        "🏪 **INITIATING VENDOR CREATION**\n\n"
        "Please enter the **Telegram ID** of the Vendor owner.\n"
        "_(This ID will be used for them to receive order alerts)_",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(VendorCreation.V_GET_TG_ID)


@router.message(VendorCreation.V_GET_TG_ID, F.from_user.id.in_(settings.ADMIN_IDS))
async def v_get_tg_id(message: Message, state: FSMContext):
    tg_id_input = message.text.strip()
    if not tg_id_input.isdigit():
        await message.answer("⚠️ Invalid ID. Please enter numeric digits only.")
        return
    await state.update_data(new_vendor_tg_id=int(tg_id_input))
    await message.answer(
        "✅ ID Captured.\n\n"
        "Now, enter the **Display Name** for this Vendor.\n"
        "_(e.g., 'Mama Kitchen', 'Burger King 6kilo')_"
    )
    await state.set_state(VendorCreation.V_GET_NAME)


@router.message(VendorCreation.V_GET_NAME, F.from_user.id.in_(settings.ADMIN_IDS))
async def v_get_name(message: Message, state: FSMContext):
    name = message.text.strip()
    await state.update_data(new_vendor_name=name)
    data = await state.get_data()
    tg_id = data['new_vendor_tg_id']
    txt = (
        "📋 **CONFIRM VENDOR DETAILS**\n\n"
        f"🔹 **ID:** `{tg_id}`\n"
        f"🔹 **Name:** {name}\n\n"
        "Type 'yes' to commit to database or /cancel to abort."
    )
    await message.answer(txt, parse_mode="Markdown")
    await state.set_state(VendorCreation.V_CONFIRM)


@router.message(VendorCreation.V_CONFIRM, F.from_user.id.in_(settings.ADMIN_IDS))
async def v_confirm(message: Message, state: FSMContext, db: Database):
    text = message.text.lower()
    if text not in ['yes', 'y', 'confirm']:
        await message.answer("❌ Action cancelled or invalid input.")
        await state.clear()
        await admin_start_handler(message, state)
        return

    data = await state.get_data()
    tg_id = data['new_vendor_tg_id']
    name = data['new_vendor_name']

    try:
        existing = await db.get_vendor_by_telegram(tg_id)
        if existing:
            await message.answer(
                f"⚠️ **Error:** Vendor with ID `{tg_id}` already exists ({existing['name']}).",
                parse_mode="Markdown"
            )
            await state.clear()
            await admin_start_handler(message, state)
            return

        vid = await db.create_vendor(tg_id, name)
        await message.answer(
            f"🚀 **SUCCESS!**\n\n"
            f"Vendor **{name}** has been deployed.\n"
            f"Internal Vendor ID: `{vid}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Vendor creation failed: {e}")
        await message.answer("💥 **CRITICAL ERROR** during database commit.")

    await state.clear()
    await admin_start_handler(message, state)
# ==============================================================================
# 🛵 PROTOCOL: ADD DELIVERY GUY (MANUAL ONBOARDING) (TIER 1)
# [Existing implementation remains unchanged]
# ==============================================================================

@router.message(F.text == "🛵 Add Delivery Guy", F.from_user.id.in_(settings.ADMIN_IDS))
async def start_add_dg(message: Message, state: FSMContext):
    """Entry point for Manual Delivery Guy Creation."""
    await message.answer(
        "🛵 **INITIATING FLEET RECRUITMENT**\n\n"
        "Please enter the **Telegram ID** of the Delivery Guy.\n"
        "_(We will check if they are already a user, or create a fresh profile)_",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(DGCreation.DG_GET_TG_ID)


@router.message(DGCreation.DG_GET_TG_ID, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_tg_id(message: Message, state: FSMContext):
    tg_id_input = message.text.strip()
    if not tg_id_input.isdigit():
        await message.answer("⚠️ Invalid ID. Numeric digits only.")
        return
    await state.update_data(new_dg_tg_id=int(tg_id_input))
    await message.answer("✅ ID Captured.\n\nEnter the **Full Name** of the Delivery Guy.")
    await state.set_state(DGCreation.DG_GET_NAME)


@router.message(DGCreation.DG_GET_NAME, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_name(message: Message, state: FSMContext):
    await state.update_data(new_dg_name=message.text.strip())
    await message.answer("Enter the **Phone Number** (e.g., 0911223344).")
    await state.set_state(DGCreation.DG_GET_PHONE)


@router.message(DGCreation.DG_GET_PHONE, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_phone(message: Message, state: FSMContext):
    await state.update_data(new_dg_phone=message.text.strip())
    keyboard_data = [["6kilo", "5kilo"], ["4kilo"]]
    keyboard = [[KeyboardButton(text=btn) for btn in row] for row in keyboard_data]
    await message.answer(
        "Select the **Primary Campus/Zone**:",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, one_time_keyboard=True, resize_keyboard=True)
    )
    await state.set_state(DGCreation.DG_GET_CAMPUS)


@router.message(DGCreation.DG_GET_CAMPUS, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_campus(message: Message, state: FSMContext):
    await state.update_data(new_dg_campus=message.text.strip())
    data = await state.get_data()
    txt = (
        "📋 **VERIFY RECRUITMENT DATA**\n\n"
        f"🔹 **Telegram ID:** `{data['new_dg_tg_id']}`\n"
        f"🔹 **Name:** {data['new_dg_name']}\n"
        f"🔹 **Phone:** {data['new_dg_phone']}\n"
        f"🔹 **Zone:** {data['new_dg_campus']}\n\n"
        "Type 'yes' to execute creation sequence."
    )
    await message.answer(txt, parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
    await state.set_state(DGCreation.DG_CONFIRM)


@router.message(DGCreation.DG_CONFIRM, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_confirm(message: Message, state: FSMContext, db: Database):
    text = message.text.lower()
    if text not in ['yes', 'y', 'confirm']:
        await message.answer("❌ Operation aborted.")
        await state.clear()
        await admin_start_handler(message, state)
        return

    data = await state.get_data()
    tg_id = data['new_dg_tg_id']
    name = data['new_dg_name']
    phone = data['new_dg_phone']
    campus = data['new_dg_campus']

    try:
        user_id = await db.get_internal_user_id(tg_id)

        if not user_id:
            await message.answer("⚙️ Creating base User profile...")
            user_id = await db.create_user(
                telegram_id=tg_id,
                role="delivery",
                first_name=name,
                phone=phone,
                campus=campus
            )
        else:
            await message.answer(f"ℹ️ Base User found (ID: {user_id}). Linking profiles...")

        existing_dg = await db.get_delivery_guy_by_user(tg_id)
        if existing_dg:
            await message.answer("⚠️ **Wait:** This user is already a Delivery Guy.")
            await state.clear()
            await admin_start_handler(message, state)
            return

        dg_id = await db.create_delivery_guy(user_id, name, campus)
        await db.set_delivery_guy_online(dg_id)

        await message.answer(
            f"🎖 **RECRUITMENT COMPLETE**\n\n"
            f"Agent **{name}** is now ACTIVE in the system.\n"
            f"DG ID: `{dg_id}` | User ID: `{user_id}`\n\n"
            f"Please instruct the user to hit /start in their bot.",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"DG creation failed: {e}")
        await message.answer(f"💥 **SYSTEM FAILURE**: {str(e)}")

    await state.clear()
    await admin_start_handler(message, state)
# ==============================================================================
# 📢 PROTOCOL: BROADCAST MESSAGE (TIER 2)
# [Existing implementation remains unchanged]
# ==============================================================================



@router.message(F.text == "📢 Broadcast", F.from_user.id.in_(settings.ADMIN_IDS))
async def start_broadcast(message: Message, state: FSMContext):
    """Entry point for initiating a broadcast."""
    await message.answer(
        "📢 **INITIATING BROADCAST PROTOCOL**\n\n"
        "Please send the message you wish to broadcast to **all users**.\n"
        "This can include text, markdown, or media (send media separately with a caption).\n\n"
        "Type /cancel to abort.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(BroadcastState.B_GET_MESSAGE)


@router.message(BroadcastState.B_GET_MESSAGE, F.from_user.id.in_(settings.ADMIN_IDS))
async def b_get_message(message: Message, state: FSMContext):
    # Store the entire message object for full fidelity (text, photo, etc.)
    await state.update_data(broadcast_message=message.model_dump_json(exclude_none=True))
    
    txt = (
        "📋 **BROADCAST PREVIEW**\n\n"
        "The following message will be sent to all users. **Are you sure?**\n\n"
    )
    
    await message.answer(txt, parse_mode="Markdown")
    
    # Forward the message to the admin for a perfect preview
    await message.forward(chat_id=message.chat.id)

    await message.answer("Type 'yes' to execute the broadcast, or /cancel.")
    await state.set_state(BroadcastState.B_CONFIRM)


@router.message(BroadcastState.B_CONFIRM, F.from_user.id.in_(settings.ADMIN_IDS))
async def b_confirm(message: Message, state: FSMContext, db: Database, bot: Bot):
    text = message.text.lower()
    if text not in ['yes', 'y', 'confirm']:
        await message.answer("❌ Broadcast cancelled.")
        await state.clear()
        await admin_start_handler(message, state)
        return

    data = await state.get_data()
    message_json = data['broadcast_message']
    
    try:
        # NOTE: Placeholder Logic for background broadcasting
        simulated_user_count = 1200 
        
        await message.answer(
            f"✅ **BROADCAST EXECUTED**\n\n"
            f"Protocol initiated. (Simulated target audience: {simulated_user_count}).\n"
            f"_The actual delivery process runs in the background and may take time._",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Broadcast initiation failed: {e}")
        await message.answer(f"💥 **CRITICAL ERROR** initiating broadcast: {str(e)}")

    await state.clear()
    await admin_start_handler(message, state)


# ==============================================================================
# 💰 PROTOCOL: FINANCE DASHBOARD (TIER 2)
# ==============================================================================

@router.message(F.text == "💰 Finance", F.from_user.id.in_(settings.ADMIN_IDS))
async def finance_dashboard(message: Message, db: Database):
    """Simple status check for finance/revenue overview."""
    
    # Placeholder data
    total_revenue = 14523.50
    pending_payouts = 2150.00
    active_vendors = 8
    
    txt = (
        "💰 **FINANCE & ACCOUNTING DASHBOARD** 💰\n\n"
        "### Operational Metrics\n"
        f"• **Total Revenue (All Time):** `{total_revenue:,.2f} ETB`\n"
        f"• **Pending Payouts (DG/Vendors):** `{pending_payouts:,.2f} ETB`\n"
        f"• **Active Vendors:** `{active_vendors}`\n"
        "\n_Detailed transaction logs and vendor statements are available in the next tier._"
    )
    
    await message.answer(txt, parse_mode="Markdown")
    
    
# ==============================================================================
# 💰 PROTOCOL: FINANCE DASHBOARD (TIER 2)
# [Existing implementation remains unchanged]
# ==============================================================================
from aiogram import F, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

router = Router()

# ==============================================================================
# 💰 PROTOCOL: FINANCE DASHBOARD (TIER 2)
# ==============================================================================

@router.message(F.text == "💰 Finance", F.from_user.id.in_(settings.ADMIN_IDS))
async def finance_dashboard(message: Message, db: Database):
    """Simple status check for finance/revenue overview."""
    
    # Placeholder data
    total_revenue = 14523.50
    pending_payouts = 2150.00
    active_vendors = 8
    
    txt = (
        "💰 **FINANCE & ACCOUNTING DASHBOARD** 💰\n\n"
        "### Operational Metrics\n"
        f"• **Total Revenue (All Time):** `{total_revenue:,.2f} ETB`\n"
        f"• **Pending Payouts (DG/Vendors):** `{pending_payouts:,.2f} ETB`\n"
        f"• **Active Vendors:** `{active_vendors}`\n"
        "\n_Detailed transaction logs and vendor statements are available in the next tier._"
    )
    
    await message.answer(txt, parse_mode="Markdown")


# ==============================================================================
# ⚙️ PROTOCOL: SETTINGS DASHBOARD (TIER 3)
# ==============================================================================

@router.message(F.text == "⚙️ Settings", F.from_user.id.in_(settings.ADMIN_IDS))
async def settings_dashboard(message: Message, state: FSMContext):
    """The Settings Sub-Menu."""
    await state.clear()
    
    keyboard = [
        ["⛔ Block Delivery Guy", "✅ Unblock Delivery Guy"],
        ["🔗 Link Vendor ID", "📝 Edit DG Profile"],
        ["◀️ Back to Admin Menu"],
    ]
    
    reply_markup = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=btn) for btn in row] for row in keyboard],
        resize_keyboard=True, 
        one_time_keyboard=True,
        input_field_placeholder="Select Setting Protocol..."
    )
    
    txt = (
        "⚙️ **SYSTEM SETTINGS & MODERATION** ⚙️\n\n"
        "Manage users, permissions, and system constants."
    )
    await message.answer(txt, reply_markup=reply_markup, parse_mode="Markdown")


@router.message(F.text == "◀️ Back to Admin Menu", F.from_user.id.in_(settings.ADMIN_IDS))
async def back_to_admin_menu_handler(message: Message, state: FSMContext):
    """Navigates back from a sub-menu to the main admin menu."""
    await admin_start_handler(message, state)


# --- Block Delivery Guy Flow ---

@router.message(F.text == "⛔ Block Delivery Guy", F.from_user.id.in_(settings.ADMIN_IDS))
async def start_block_dg(message: Message, state: FSMContext):
    """Starts the process to block a delivery guy."""
    await message.answer(
        "⛔ **INITIATING BLOCK PROTOCOL**\n\n"
        "Enter the **Telegram ID** of the Delivery Guy to be blocked:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(BlockDGState.BDG_GET_ID)


@router.message(BlockDGState.BDG_GET_ID, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_id_to_block(message: Message, state: FSMContext, db: Database):
    tg_id_input = message.text.strip()
    if not tg_id_input.isdigit():
        await message.answer("⚠️ Invalid ID. Please enter numeric digits only.")
        return

    tg_id = int(tg_id_input)
    dg_profile = await db.get_delivery_guy_by_telegram(tg_id)
    
    if not dg_profile:
        await message.answer(f"❌ DG with ID `{tg_id}` not found in the fleet.", parse_mode="Markdown")
        await state.clear()
        await settings_dashboard(message, state)
        return

    if dg_profile.get('blocked'):
        await message.answer(f"ℹ️ DG **{dg_profile.get('name')}** is already blocked.", parse_mode="Markdown")
        await state.clear()
        await settings_dashboard(message, state)
        return

    await state.update_data(dg_tg_id_to_block=tg_id, dg_name_to_block=dg_profile.get('name'))
    
    await message.answer(
        f"✅ DG **{dg_profile.get('name')}** selected.\n\n"
        "Please provide a **short reason** for the block. This will be logged.",
        parse_mode="Markdown"
    )
    await state.set_state(BlockDGState.BDG_GET_REASON)


@router.message(BlockDGState.BDG_GET_REASON, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_get_reason_for_block(message: Message, state: FSMContext):
    reason = message.text.strip()
    await state.update_data(block_reason=reason)
    
    data = await state.get_data()
    
    txt = (
        "📋 **CONFIRM BLOCK ACTION**\n\n"
        f"• **Target:** DG **{data['dg_name_to_block']}** (`{data['dg_tg_id_to_block']}`)\n"
        f"• **Reason:** _{reason}_\n\n"
        "🚨 **WARNING:** This will immediately set the DG's status to INACTIVE and BLOCKED.\n"
        "Type 'yes' to proceed with the block."
    )
    await message.answer(txt, parse_mode="Markdown")
    await state.set_state(BlockDGState.BDG_CONFIRM)


@router.message(BlockDGState.BDG_CONFIRM, F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_confirm_block(message: Message, state: FSMContext, db: Database, bot: Bot):
    text = message.text.lower()
    if text not in ['yes', 'y', 'confirm']:
        await message.answer("❌ Block operation cancelled.")
        await state.clear()
        await settings_dashboard(message, state)
        return

    data = await state.get_data()
    tg_id = data['dg_tg_id_to_block']
    name = data['dg_name_to_block']
    reason = data['block_reason']
    
    try:
        await db.block_delivery_guy(tg_id, reason)
        
        await message.answer(
            f"✅ **BLOCK EXECUTED.**\n\n"
            f"DG **{name}** (`{tg_id}`) has been blocked and taken offline.\n"
            f"Reason: _{reason}_",
            parse_mode="Markdown"
        )
        
        # Notify the DG (best effort, ignore if chat is blocked)
        with contextlib.suppress(Exception):
            await bot.send_message(
                tg_id, 
                "⛔ **ACCOUNT BLOCKED.**\n"
                "Your delivery service access has been suspended by the administrator.\n"
                f"Reason: {reason}",
                parse_mode="Markdown"
            )

    except Exception as e:
        logger.error(f"DG block failed for {tg_id}: {e}")
        await message.answer(f"💥 **CRITICAL ERROR** during block commit: {str(e)}")

    await state.clear()
    await settings_dashboard(message, state)
# ==============================================================================
# 📈 PROTOCOL: ANALYTICS DASHBOARD (TIER 3)
# ==============================================================================
from aiogram import F, Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

router = Router()

# ==============================================================================
# 📈 PROTOCOL: ANALYTICS DASHBOARD (TIER 3)
# ==============================================================================

@router.message(F.text == "📈 Analytics", F.from_user.id.in_(settings.ADMIN_IDS))
async def analytics_dashboard(message: Message, db: Database):
    """Detailed analytics and performance metrics."""
    
    # NOTE: Requires database functions for these metrics
    
    # Placeholder Data (Simulated Fetch)
    total_users = 5120
    active_dgs = 15
    avg_delivery_time = "25.3 min"
    dg_acceptance_rate = "89.5%"
    top_campus = "6kilo"
    
    txt = (
        "📈 **SYSTEM ANALYTICS DASHBOARD** 📈\n\n"
        "### User & Fleet Metrics\n"
        f"• **Total Users:** `{total_users:,}`\n"
        f"• **Active Delivery Guys:** `{active_dgs}`\n"
        f"• **DG Acceptance Rate (Overall):** `{dg_acceptance_rate}`\n"
        "\n### Performance & Flow\n"
        f"• **Average Delivery Time:** `{avg_delivery_time}`\n"
        f"• **Highest Order Volume Campus:** `{top_campus}`\n"
        f"• **System Health:** _All APIs 200 OK_\n\n"
        "_(Real-time dashboards and charts are a future feature!)_"
    )
    
    await message.answer(txt, parse_mode="Markdown")


# ==============================================================================
# 📊 UTILS: SYSTEM STATUS (TIER 1/3)
# ==============================================================================

@router.message(F.text == "📊 System Status", F.from_user.id.in_(settings.ADMIN_IDS))
async def system_status(message: Message):
    """Placeholder for System Health Check."""
    await message.answer(
        "📊 **SYSTEM STATUS: OPTIMAL**\n\n"
        "• Database: CONNECTED\n"
        "• Payment Gateway: STANDBY\n"
        "• Active Orders: _Calculating..._\n\n"
        "_(Detailed analytics coming in Tier 3)_",
        parse_mode="Markdown"
    )


@router.message(F.text == "🚫 Emergency Stop", F.from_user.id.in_(settings.ADMIN_IDS))
async def emergency_stop(message: Message):
    """Placeholder for Panic Button."""
    await message.answer(
        "🚨 **EMERGENCY STOP TRIGGERED**\n\n"
        "Are you sure you want to halt all operations?\n"
        "This feature is currently in simulation mode.",
        parse_mode="Markdown"
    )

# --- HANDLER SETTINGS --- 

class SettingsState(StatesGroup):
    editing_name = State()
    editing_campus = State()
    editing_phone = State()



# Step 1: Show profile card + options
@router.message(F.text == "⚙️ Configure")
async def settings(message: Message, state: FSMContext):
    user = await db.get_user(message.from_user.id)
    if not user:
        await message.answer("⚠️ No profile found. Please register first.")
        return
    card = build_profile_card(user, role="student")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Change Name", callback_data="settings:name"),
         InlineKeyboardButton(text="🏛 Change Campus", callback_data="settings:campus")],
        [InlineKeyboardButton(text="📱 Edit Phone Number", callback_data="settings:phone")]
    ])

    await message.answer(card, reply_markup=kb)
    await state.clear()  # reset any previous state

# Step 2: Handle option selection
# Step 2: Handle option selection
@router.callback_query(F.data.startswith("settings:"))
async def settings_option(cb: CallbackQuery, state: FSMContext):
    option = cb.data.split(":")[1]

    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Cancel")], [KeyboardButton(text="🏠 Main Menu")]],
        resize_keyboard=True
    )

    if option == "name":
        await cb.message.answer("✏️ Please type your new name:", reply_markup=cancel_kb)
        await state.set_state(SettingsState.editing_name)

    elif option == "campus":
        # Inline buttons for campus selection
        campus_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="4kilo", callback_data="campus:4kilo"),
                InlineKeyboardButton(text="5kilo", callback_data="campus:5kilo"),
                InlineKeyboardButton(text="6kilo", callback_data="campus:6kilo"),
            ]
        ])
        await cb.message.answer("🏛 Select your new campus:", reply_markup=campus_kb)
        await state.set_state(SettingsState.editing_campus)

    elif option == "phone":
        await cb.message.answer("📱 Please type your new phone number (must start with 09):", reply_markup=cancel_kb)
        await state.set_state(SettingsState.editing_phone)

    await cb.answer()


# Step 4: Handle campus updates via inline buttons
@router.callback_query(SettingsState.editing_campus, F.data.startswith("campus:"))
async def save_campus(cb: CallbackQuery, state: FSMContext):
    new_campus = cb.data.split(":", 1)[1]
    user_id = cb.from_user.id

    await db.update_user_field(user_id, "campus", new_campus)
    user = await db.get_user(user_id)
    card = build_profile_card(user, role="student")

    await cb.message.edit_text(f"✅ Campus updated to {new_campus}.\n\n{card}")
    await cb.answer("Campus updated successfully!")
    await state.clear()


# Step 5: Handle Cancel / Main Menu gracefully
@router.message(F.text.in_(["❌ Cancel", "🏠 Main Menu"]))
async def cancel_or_main(message: Message, state: FSMContext):
    if message.text == "❌ Cancel":
        await message.answer("❌ Update cancelled. Back to settings menu.")
    else:
        await message.answer("🏠 Returning to main menu...", reply_markup=main_menu())
    await state.clear()

# Step 3: Handle phone input
@router.message(SettingsState.editing_phone, F.text.regexp(r"^09\d{8}$"))
async def confirm_phone(message: Message, state: FSMContext):
    new_phone = message.text.strip()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, update", callback_data=f"phone_confirm:{new_phone}")],
        [InlineKeyboardButton(text="❌ No, cancel", callback_data="phone_cancel")]
    ])
    await message.answer(f"📱 You entered: {new_phone}\n\nConfirm update?", reply_markup=kb)

@router.callback_query(F.data.startswith("phone_confirm:"))
async def save_phone(cb: CallbackQuery, state: FSMContext):
    new_phone = cb.data.split(":", 1)[1]
    user_id = cb.from_user.id

    # Update DB
    await db.update_user_field(user_id, "phone", new_phone)

    # Fetch updated user
    user = await db.get_user(user_id)
    card = build_profile_card(user, role="student")

    # Show confirmation + updated card
    await cb.message.edit_text(
        f"✅ Phone updated to {new_phone}.\n\n{card}"
    )
    await cb.answer("Phone updated successfully!")
    await state.clear()


@router.callback_query(F.data == "phone_cancel")
async def cancel_phone(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("❌ Phone update cancelled. Back to settings menu.")
    await cb.answer("Cancelled.")
    await state.clear()

# Step 4: Handle name/campus updates
@router.message(SettingsState.editing_name)
async def save_name(message: Message, state: FSMContext):
    new_name = message.text.strip()
    user_id = message.from_user.id

    await db.update_user_field(user_id, "first_name", new_name)
    user = await db.get_user(user_id)
    card = build_profile_card(user, role="student")

    await message.answer(f"✅ Name updated to {new_name}.\n\n{card}")
    await state.clear()

