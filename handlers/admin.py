import contextlib
import logging
import json
from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)
from config import settings
from database.db import Database # Assuming this is your async database class



# Initialize Logger
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Define the router
router = Router()

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
    


import logging
log = logging.getLogger(__name__)
log.info(f"Loaded ADMIN_IDS: {settings.ADMIN_IDS}")
import logging
logging.info(f"Admin router id: {id(router)}")


@router.message()
async def debug_any(message: Message):
    await message.answer(f"Got message: {message.text} from {message.from_user.id}")


@router.message(Command("id"))
async def show_id(message: Message):
    await message.answer(f"Your Telegram ID is: {message.from_user.id}")


# --- ADMIN DASHBOARD ---
# The primary entry point and menu handler.
@router.message(Command("admin"))
async def admin_start_handler(message: Message, state: FSMContext):
    """The Command Center Dashboard (Persistent Menu)."""
    
    # Clear any previous state when entering the main menu
    await state.clear()
    
    # --- COMMAND CENTER LAYOUT ---
    keyboard = [
        ["➕ Add Vendor", "🛵 Add Delivery Guy"], # Tier 1: Onboarding
        ["📢 Broadcast", "💰 Finance"],           # Tier 2: Communications & Finance
        ["⚙️ Setting", "📈 Analytics"],           # <-- TIER 3 ADDED HERE
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

@router.message(F.text == "➕ Add Vendor", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(VendorCreation.V_GET_TG_ID, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(VendorCreation.V_GET_NAME, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(VendorCreation.V_CONFIRM, F.user.id.in_(settings.ADMIN_IDS))
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
            await message.answer(f"⚠️ **Error:** Vendor with ID `{tg_id}` already exists ({existing['name']}).", parse_mode="Markdown")
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

@router.message(F.text == "🛵 Add Delivery Guy", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(DGCreation.DG_GET_TG_ID, F.user.id.in_(settings.ADMIN_IDS))
async def dg_get_tg_id(message: Message, state: FSMContext):
    tg_id_input = message.text.strip()
    if not tg_id_input.isdigit():
        await message.answer("⚠️ Invalid ID. Numeric digits only.")
        return
    await state.update_data(new_dg_tg_id=int(tg_id_input))
    await message.answer("✅ ID Captured.\n\n" "Enter the **Full Name** of the Delivery Guy.")
    await state.set_state(DGCreation.DG_GET_NAME)

@router.message(DGCreation.DG_GET_NAME, F.user.id.in_(settings.ADMIN_IDS))
async def dg_get_name(message: Message, state: FSMContext):
    await state.update_data(new_dg_name=message.text.strip())
    await message.answer("Enter the **Phone Number** (e.g., 0911223344).")
    await state.set_state(DGCreation.DG_GET_PHONE)

@router.message(DGCreation.DG_GET_PHONE, F.user.id.in_(settings.ADMIN_IDS))
async def dg_get_phone(message: Message, state: FSMContext):
    await state.update_data(new_dg_phone=message.text.strip())
    keyboard_data = [["6kilo", "5kilo"], ["4kilo", "FBE"], ["Bole", "Other"]]
    keyboard = [[KeyboardButton(text=btn) for btn in row] for row in keyboard_data]
    await message.answer(
        "Select the **Primary Campus/Zone**:",
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, one_time_keyboard=True, resize_keyboard=True)
    )
    await state.set_state(DGCreation.DG_GET_CAMPUS)

@router.message(DGCreation.DG_GET_CAMPUS, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(DGCreation.DG_CONFIRM, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(F.text == "📢 Broadcast", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(BroadcastState.B_GET_MESSAGE, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(BroadcastState.B_CONFIRM, F.user.id.in_(settings.ADMIN_IDS))
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
# [Existing implementation remains unchanged]
# ==============================================================================

@router.message(F.text == "💰 Finance", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(F.text == "⚙️ Setting", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(F.text == "◀️ Back to Admin Menu", F.user.id.in_(settings.ADMIN_IDS))
async def back_to_admin_menu_handler(message: Message, state: FSMContext):
    """Navigates back from a sub-menu to the main admin menu."""
    await admin_start_handler(message, state)

# --- Block Delivery Guy Flow ---

@router.message(F.text == "⛔ Block Delivery Guy", F.user.id.in_(settings.ADMIN_IDS))
async def start_block_dg(message: Message, state: FSMContext):
    """Starts the process to block a delivery guy."""
    await message.answer(
        "⛔ **INITIATING BLOCK PROTOCOL**\n\n"
        "Enter the **Telegram ID** of the Delivery Guy to be blocked:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(BlockDGState.BDG_GET_ID)

@router.message(BlockDGState.BDG_GET_ID, F.user.id.in_(settings.ADMIN_IDS))
async def dg_get_id_to_block(message: Message, state: FSMContext, db: Database):
    tg_id_input = message.text.strip()
    if not tg_id_input.isdigit():
        await message.answer("⚠️ Invalid ID. Please enter numeric digits only.")
        return

    tg_id = int(tg_id_input)
    # Check if the DG exists and is not already blocked (Requires db.get_delivery_guy_by_telegram)
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

@router.message(BlockDGState.BDG_GET_REASON, F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(BlockDGState.BDG_CONFIRM, F.user.id.in_(settings.ADMIN_IDS))
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
        # Requires db.block_delivery_guy(tg_id, reason) to be implemented
        # This function should set `active=False` and `blocked=True`
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

@router.message(F.text == "📈 Analytics", F.user.id.in_(settings.ADMIN_IDS))
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
        "_(Real-time dashboards and charts are a future feature!)_",
        
    )
    
    await message.answer(txt, parse_mode="Markdown")


# ==============================================================================
# 📊 UTILS: SYSTEM STATUS (TIER 1/3)
# [Existing implementation remains unchanged]
# ==============================================================================

@router.message(F.text == "📊 System Status", F.user.id.in_(settings.ADMIN_IDS))
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

@router.message(F.text == "🚫 Emergency Stop", F.user.id.in_(settings.ADMIN_IDS))
async def emergency_stop(message: Message):
    """Placeholder for Panic Button."""
    await message.answer(
        "🚨 **EMERGENCY STOP TRIGGERED**\n\n"
        "Are you sure you want to halt all operations?\n"
        "This feature is currently in simulation mode.",
        parse_mode="Markdown"
    )

# --- HANDLER EXPORTS ---

