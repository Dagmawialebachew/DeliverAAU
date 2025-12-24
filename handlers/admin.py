import html
import logging
import contextlib
import asyncio
from typing import Any, Dict, Union

from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

# --- CONFIG & DATABASE IMPORTS ---
# Ensure these imports match your project structure
from config import settings
from app_context import db
from utils.helpers import time_ago  # wherever you placed the helper
from database.db import AnalyticsService, Database 
# Initialize Router
router = Router()

# Initialize Logger with a professional format
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("AdminCenter")

# ==============================================================================
# 🎛 STATE MACHINE (Unified Admin States)
# ==============================================================================
class AdminStates(StatesGroup):
    # Vendor Protocol
    vendor_get_id = State()
    vendor_get_name = State()
    vendor_confirm = State()
    vendor_edit_menu = State()
    vendor_get_status = State()

    # Delivery Guy (DG) Protocol
    dg_get_id = State()
    dg_get_name = State()
    dg_get_phone = State()
    dg_get_campus = State()
    dg_get_gender = State()   # <-- NEW
    dg_confirm = State()

    # Broadcast Protocol
    broadcast_get_content = State()
    broadcast_confirm = State()

    # Settings / Blocking
    block_dg_get_id = State()
    block_dg_reason = State()
    block_dg_confirm = State()



class AdminReplyState(StatesGroup):
    waiting_for_reply = State()
# ==============================================================================
# 🛠 UI HELPERS (Keyboards & Formatting)
# ==============================================================================
def get_main_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🏪 Vendors"), KeyboardButton(text="🚴 Delivery Guys")],
        [KeyboardButton(text="📦 Orders"), KeyboardButton(text="📈 Analytics")],
        [KeyboardButton(text="📢 Broadcast")]
        # [KeyboardButton(text="⚙️ Settings"), KeyboardButton(text="🛡 System Status")],
        # [KeyboardButton(text="🆘 Support"), KeyboardButton(text="🛑 Emergency Stop")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Choose a module..."
    )
    


@router.message(F.text == "🏪 Vendors", F.from_user.id.in_(settings.ADMIN_IDS))
async def admin_vendors_entry(message: Message, state: FSMContext):
    """Handles when admin presses Vendors from the main menu."""
    await state.clear()

    # Fetch vendors from DB
    async with db._open_connection() as conn:
        rows = await conn.fetch(
            "SELECT id, name, status, rating_avg, rating_count FROM vendors ORDER BY id ASC"
        )

    if not rows:
        await message.answer(
            "⚠️ <b>No vendors found</b>\nTap ➕ <b>Add Vendor</b> to create one.",
            reply_markup=get_main_menu_kb(),
            parse_mode="HTML"
        )
        return

    # Dashboard style summary
    summary_lines = [
        "⚡ <b>Vendor Directory</b> 𖤐",
        "━━━━━━━━━━━━━━━━━━━━━━"
    ]

    for i, r in enumerate(rows, start=1):
        status_emoji = (
            "🟢 Active" if r['status'] == "active"
            else "🟡 Busy" if r['status'] == "busy"
            else "🔴 Offline"
        )
        summary_lines.append(
            f"{i}️⃣ <b>{r['name']}</b>\n"
            f"   📊 {status_emoji} • ⭐ {round(r['rating_avg'],1)} ({r['rating_count']} ratings)\n"
        )

    summary_lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    summary_lines.append("🔍 <i>Select a vendor below to view full details</i>")

    # Inline keyboard with numbered View buttons
    vendors = [{"id": r["id"], "name": r["name"]} for r in rows]
    kb = get_vendor_list_kb(vendors)

    await message.answer("\n".join(summary_lines), reply_markup=kb, parse_mode="HTML")



@router.callback_query(F.data.startswith("vendor_view:"))
async def vendor_view_callback(callback: CallbackQuery, state: FSMContext):
    vendor_id = int(callback.data.split(":")[1])

    async with db._open_connection() as conn:
        vendor = await conn.fetchrow(
            "SELECT id, name, status, rating_avg, rating_count, created_at FROM vendors WHERE id = $1",
            vendor_id
        )

    if not vendor:
        await callback.answer("⚠️ Vendor not found.", show_alert=True)
        return

    # Status emoji
    status_emoji = (
        "🟢 Active" if vendor['status'] == "active"
        else "🟡 Busy" if vendor['status'] == "busy"
        else "🔴 Offline"
    )
    created_display = time_ago(vendor["created_at"])

    # Card text
    card_text = (
        f"🏪 <b>Vendor: {vendor['name']}</b>\n"
        f"📊 Status: {status_emoji}\n"
        f"⭐ Rating: {round(vendor['rating_avg'],1)} ({vendor['rating_count']} ratings)\n"
        f"📅 Created: {created_display}\n"       
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Tap buttons below to manage or view stats"
    )

    kb = get_vendor_card_kb(vendor_id)
    await callback.message.edit_text(card_text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


def get_vendor_list_kb(vendors: list) -> InlineKeyboardMarkup:
    rows = []
    numbered_buttons = [
        InlineKeyboardButton(text=f"🔎 View {i+1}", callback_data=f"vendor_view:{vendor['id']}")
        for i, vendor in enumerate(vendors)
    ]
    for i in range(0, len(numbered_buttons), 3):
        rows.append(numbered_buttons[i:i+3])
    rows.append([
        InlineKeyboardButton(text="➕ Add Vendor", callback_data="vendor_add"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_vendor_card_kb(vendor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"vendor_edit:{vendor_id}"),
            InlineKeyboardButton(text="🗑 Delete", callback_data=f"vendor_delete:{vendor_id}")
        ],
        [
            InlineKeyboardButton(text="📊 Status", callback_data=f"vendor_status:{vendor_id}"),
            InlineKeyboardButton(text="⬅️ Back to List", callback_data="admin_vendors")
        ]
    ])

@router.callback_query(F.data == "admin_vendors")
async def vendor_back_to_list(callback: CallbackQuery, state: FSMContext):
    """Handles when admin presses ⬅️ Back to List from a vendor card."""
    await state.clear()

    async with db._open_connection() as conn:
        rows = await conn.fetch(
            "SELECT id, name, status, rating_avg, rating_count FROM vendors ORDER BY id ASC"
        )

    if not rows:
        await callback.message.edit_text(
            "⚠️ <b>No vendors found</b>\nTap ➕ <b>Add Vendor</b> to create one.",
            reply_markup=get_main_menu_kb(),
            parse_mode="HTML"
        )
        await callback.answer()
        return

    # Build summary again
    summary_lines = [
        "⚡ <b>Vendor Directory</b> 𖤐",
        "━━━━━━━━━━━━━━━━━━━━━━"
    ]

    for i, r in enumerate(rows, start=1):
        status_emoji = (
            "🟢 Active" if r['status'] == "active"
            else "🟡 Busy" if r['status'] == "busy"
            else "🔴 Offline"
        )
        summary_lines.append(
            f"{i}️⃣ <b>{r['name']}</b>\n"
            f"   📊 {status_emoji} • ⭐ {round(r['rating_avg'],1)} ({r['rating_count']} ratings)\n"
        )

    summary_lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    summary_lines.append("🔍 <i>Select a vendor below to view full details</i>")

    vendors = [{"id": r["id"], "name": r["name"]} for r in rows]
    kb = get_vendor_list_kb(vendors)

    # Edit the existing message back to the list view
    await callback.message.edit_text("\n".join(summary_lines), reply_markup=kb, parse_mode="HTML")
    await callback.answer()



def get_dg_list_kb(dgs: list) -> InlineKeyboardMarkup:
    rows = []
    numbered_buttons = [
        InlineKeyboardButton(text=f"🔎 View {i+1}", callback_data=f"dg_view:{dg['id']}")
        for i, dg in enumerate(dgs)
    ]
    for i in range(0, len(numbered_buttons), 3):
        rows.append(numbered_buttons[i:i+3])
    rows.append([
        InlineKeyboardButton(text="➕ Add DG", callback_data="dg_add"),
        InlineKeyboardButton(text="⬅️ Back to Main", callback_data="admin_back")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_order_list_kb(orders: list) -> InlineKeyboardMarkup:
    rows = []
    numbered_buttons = [
        InlineKeyboardButton(text=f"🔎 View {i+1}", callback_data=f"order_view:{order['id']}")
        for i, order in enumerate(orders)
    ]
    for i in range(0, len(numbered_buttons), 3):
        rows.append(numbered_buttons[i:i+3])
    rows.append([
        InlineKeyboardButton(text="⬅️ Back to Main", callback_data="admin_back")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def get_confirm_cancel_kb(action_prefix: str) -> InlineKeyboardMarkup:
    """Generates a standard Confirm/Cancel inline keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Confirm Execution", callback_data=f"{action_prefix}_confirm"),
            InlineKeyboardButton(text="❌ Abort", callback_data="admin_cancel_inline")
        ]
    ])

def get_campus_kb() -> ReplyKeyboardMarkup:
    """Selection for Campuses."""
    kb = [
        [KeyboardButton(text="6kilo"), KeyboardButton(text="5kilo")],
        [KeyboardButton(text="4kilo"), KeyboardButton(text="FBE")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, one_time_keyboard=True)

# ==============================================================================
# 🎮 ENTRY POINT: ADMIN DASHBOARD
# ==============================================================================
@router.message(Command("admin"), F.from_user.id.in_(settings.ADMIN_IDS))
async def admin_entry(message: Message, state: FSMContext):
    """Initializes the Admin Command Center."""
    await state.clear()
    
    dashboard_text = (
        "<b>🔐 COMMAND CENTER v3.0</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🟢 <b>System Status:</b> ONLINE\n"
        "🛡 <b>Auth Level:</b> SUPERUSER\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Select a protocol to initiate:</i>"
    )
    
    await message.answer(dashboard_text, reply_markup=get_main_menu_kb(), parse_mode="HTML")
    logger.info(f"[ADMIN:AUTH] User {message.from_user.id} accessed Command Center.")

@router.message(Command("cancel"), F.from_user.id.in_(settings.ADMIN_IDS))
@router.callback_query(F.data == "admin_cancel_inline")
async def cancel_operation(event: Union[Message, CallbackQuery], state: FSMContext):
    """Universal Cancellation Handler."""
    current_state = await state.get_state()
    if current_state is None:
        return

    logging.info(f"[ADMIN:CANCEL] Cancelled state {current_state}")
    await state.clear()
    
    text = "🛑 <b>Operation Aborted.</b> Returning to neutral state."
    
    if isinstance(event, Message):
        await event.answer(text, reply_markup=get_main_menu_kb(), parse_mode="HTML")
    elif isinstance(event, CallbackQuery):
        await event.message.edit_text(text + "\n(Interface Reset)", parse_mode="HTML")
        await event.answer("Cancelled")

# ==============================================================================
# 🏪 PROTOCOL: VENDOR ONBOARDING
# ==============================================================================
@router.callback_query(F.data == "vendor_add", F.from_user.id.in_(settings.ADMIN_IDS))
async def vendor_add_init(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "<b>🏪 VENDOR DEPLOYMENT // STEP 1/3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram ID</b> of the Vendor owner.\n"
        "<i>(This ID is used for notifications)</i>",
        parse_mode="HTML"
        # ❌ remove reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.vendor_get_id)
    await callback.answer()


async def render_vendor_edit_preview(bot: Bot, chat_id: int, message_id: int, data: dict):
    summary = (
        "<b>📋 EDIT VENDOR DATA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>Telegram ID:</b> <code>{data.get('v_id','—')}</code>\n"
        f"🏷 <b>Name:</b> {data.get('v_name','—')}\n"
        f"📊 <b>Status:</b> {data.get('v_status','—')}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Select a field to edit or confirm changes.</i>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🆔 Edit Telegram ID", callback_data="edit_vendor_id"),
            InlineKeyboardButton(text="🏷 Edit Name", callback_data="edit_vendor_name")
        ],
        [
            InlineKeyboardButton(text="📊 Edit Status", callback_data="edit_vendor_status"),
            InlineKeyboardButton(text="✅ Confirm", callback_data="vendor_edit_confirm")
        ],
        [
            InlineKeyboardButton(text="❌ Cancel", callback_data="admin_cancel_inline")
        ]
    ])

    return await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=summary,
        parse_mode="HTML",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("vendor_edit:"))
async def vendor_edit_init(callback: CallbackQuery, state: FSMContext):
    vendor_id = int(callback.data.split(":")[1])

    async with db._open_connection() as conn:
        vendor = await conn.fetchrow(
            "SELECT id, telegram_id, name, status FROM vendors WHERE id = $1",
            vendor_id
        )

    if not vendor:
        await callback.answer("⚠️ Vendor not found.", show_alert=True)
        return

    preview_msg = await callback.message.edit_text("Loading vendor data...")
    
    await state.update_data(
        card_message_id=preview_msg.message_id,
        edit_mode=True,
        vendor_db_id=vendor["id"],
        v_id=vendor["telegram_id"],
        v_name=vendor["name"],
        v_status=vendor["status"]
    )

    await render_vendor_edit_preview(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        message_id=preview_msg.message_id,
        data=await state.get_data()
    )
    # FIX: Set to edit_menu so the edit buttons are handled
    await state.set_state(AdminStates.vendor_edit_menu)

@router.callback_query(F.data == "edit_vendor_id", AdminStates.vendor_edit_menu)
async def edit_vendor_id(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Enter new Telegram ID:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(AdminStates.vendor_get_id)
    await callback.answer()


@router.message(AdminStates.vendor_get_id)
async def vendor_id_updated(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ ID must be numeric. Try again.")
        return

    await state.update_data(v_id=int(message.text))
    data = await state.get_data()

    if data.get("edit_mode"):
        # Edit mode → refresh preview card
        if "card_message_id" in data:
            await render_vendor_edit_preview(
                bot=message.bot,
                chat_id=message.chat.id,
                message_id=data["card_message_id"],
                data=data
            )
        else:
            # Fallback if card_message_id is missing
            await message.answer("⚠️ Preview card not found, continuing with edit flow.")
        await state.set_state(AdminStates.vendor_confirm)
    else:
        # Add mode → continue to next step
        await message.answer(
            "<b>🏪 VENDOR DEPLOYMENT // STEP 2/3</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Enter the <b>Display Name</b> for this Vendor.\n"
            "<i>(e.g., 'Juice Hub 5kilo')</i>",
            parse_mode="HTML"
        )
        await state.set_state(AdminStates.vendor_get_name)


@router.callback_query(F.data == "edit_vendor_name", AdminStates.vendor_edit_menu)
async def edit_vendor_name(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Enter new Vendor Name:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(AdminStates.vendor_get_name)
    await callback.answer()

@router.message(AdminStates.vendor_get_name)
async def vendor_name_updated(message: Message, state: FSMContext):
    await state.update_data(v_name=message.text)
    data = await state.get_data()

    if data.get("edit_mode"):
        # Edit mode → refresh preview card
        if "card_message_id" in data:
            await render_vendor_edit_preview(
                bot=message.bot,
                chat_id=message.chat.id,
                message_id=data["card_message_id"],
                data=data
            )
        else:
            await message.answer("⚠️ Preview card not found, continuing with edit flow.")
        await state.set_state(AdminStates.vendor_confirm)
    else:
        # Add mode → show review summary instead of preview card
        summary = (
            "<b>📋 REVIEW DEPLOYMENT DATA</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Telegram ID:</b> <code>{data['v_id']}</code>\n"
            f"🏷 <b>Name:</b> {data['v_name']}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "<i>Confirm to commit this Vendor to the database?</i>"
        )
        await message.answer(summary, parse_mode="HTML", reply_markup=get_confirm_cancel_kb("vendor"))
        await state.set_state(AdminStates.vendor_confirm)


@router.callback_query(F.data == "edit_vendor_status", AdminStates.vendor_edit_menu)
async def edit_vendor_status(callback: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Active", callback_data="status_active"),
            InlineKeyboardButton(text="🟡 Busy", callback_data="status_busy"),
            InlineKeyboardButton(text="🔴 Offline", callback_data="status_offline")
        ]
    ])
    await callback.message.edit_text("Choose new status:", reply_markup=kb)
    await state.set_state(AdminStates.vendor_get_status)
    await callback.answer()


@router.callback_query(F.data.startswith("status_"), AdminStates.vendor_get_status)
async def vendor_status_updated(callback: CallbackQuery, state: FSMContext):
    status = callback.data.replace("status_", "")
    await state.update_data(v_status=status)

    data = await state.get_data()
    await render_vendor_edit_preview(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        message_id=data["card_message_id"],
        data=data
    )

    # Allow more edits or confirmation
    await state.set_state(AdminStates.vendor_edit_menu) 
    await callback.answer("Status updated in preview.")

# Handle the Confirm Button specifically for the Edit Menu
@router.callback_query(F.data == "vendor_edit_confirm", AdminStates.vendor_edit_menu)
async def vendor_edit_confirm_trigger(callback: CallbackQuery, state: FSMContext):
    # Transition to the final commit state
    await state.set_state(AdminStates.vendor_confirm)
    # Manually trigger the commit logic
    await vendor_commit(callback, state)
    
    
@router.message(AdminStates.vendor_get_id)
async def vendor_id_captured(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ <b>Error:</b> ID must be numeric. Try again.", parse_mode="HTML")
        return
    
    await state.update_data(v_id=int(message.text))
    await message.answer(
        "<b>🏪 VENDOR DEPLOYMENT // STEP 2/3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Display Name</b> for this Vendor.\n"
        "<i>(e.g., 'Juice Hub 5kilo')</i>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.vendor_get_name)
    
    
# --- ID UPDATE HANDLER ---
@router.message(AdminStates.vendor_get_id)
async def vendor_id_captured(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ ID must be numeric. Try again.")
        return

    await state.update_data(v_id=int(message.text))
    data = await state.get_data()

    if data.get("edit_mode"):
        # Return to Edit Menu
        await render_vendor_edit_preview(
            bot=message.bot, chat_id=message.chat.id,
            message_id=data["card_message_id"], data=data
        )
        await state.set_state(AdminStates.vendor_edit_menu)
    else:
        # Continue Onboarding
        await message.answer(
            "<b>🏪 VENDOR DEPLOYMENT // STEP 2/3</b>\n"
            "Enter the <b>Display Name</b> for this Vendor.",
            parse_mode="HTML"
        )
        await state.set_state(AdminStates.vendor_get_name)

# --- NAME UPDATE HANDLER ---
@router.message(AdminStates.vendor_get_name)
async def vendor_name_captured(message: Message, state: FSMContext):
    await state.update_data(v_name=message.text)
    data = await state.get_data()

    if data.get("edit_mode"):
        # Return to Edit Menu
        await render_vendor_edit_preview(
            bot=message.bot, chat_id=message.chat.id,
            message_id=data["card_message_id"], data=data
        )
        await state.set_state(AdminStates.vendor_edit_menu)
    else:
        # Show Review for New Vendor
        summary = (
            "<b>📋 REVIEW DEPLOYMENT DATA</b>\n"
            f"🆔 ID: <code>{data['v_id']}</code>\n"
            f"🏷 Name: {data['v_name']}\n"
            "Confirm to commit to database?"
        )
        await message.answer(summary, parse_mode="HTML", reply_markup=get_confirm_cancel_kb("vendor"))
        await state.set_state(AdminStates.vendor_confirm)
  
@router.callback_query(F.data.startswith("vendor_status:"))
async def vendor_status_view(callback: CallbackQuery):
    vendor_id = int(callback.data.split(":")[1])

    async with db._open_connection() as conn:
        vendor = await conn.fetchrow(
            "SELECT id, name, status, rating_avg, rating_count FROM vendors WHERE id=$1",
            vendor_id
        )

    if not vendor:
        await callback.answer("⚠️ Vendor not found.", show_alert=True)
        return

    status_emoji = (
        "🟢 Active" if vendor["status"] == "active"
        else "🟡 Busy" if vendor["status"] == "busy"
        else "🔴 Offline"
    )

    stats_text = (
        f"<b>📊 Vendor Stats</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 <b>Name:</b> {vendor['name']}\n"
        f"📊 <b>Status:</b> {status_emoji}\n"
        f"⭐ <b>Rating:</b> {round(vendor['rating_avg'],1)} ({vendor['rating_count']} ratings)\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )

    await callback.message.edit_text(
        stats_text,
        parse_mode="HTML",
        reply_markup=get_vendor_card_kb(vendor_id)
    )
    await callback.answer()

# INIT: show confirmation prompt
@router.callback_query(F.data.startswith("vendor_delete:") & ~F.data.endswith("_confirm"))
async def vendor_delete_init(callback: CallbackQuery, state: FSMContext):
    try:
        vendor_id_str = callback.data.split(":")[1]
        vendor_id = int(vendor_id_str)
    except Exception:
        await callback.answer("❌ Invalid vendor ID format.", show_alert=True)
        return

    async with db._open_connection() as conn:
        vendor = await conn.fetchrow("SELECT id, name FROM vendors WHERE id=$1", vendor_id)

    if not vendor:
        await callback.answer("⚠️ Vendor not found.", show_alert=True)
        return

    confirm_text = (
        f"⚠️ <b>Confirm Deletion</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 Vendor: {vendor['name']}\n"
        f"🆔 ID: {vendor['id']}\n\n"
        "<i>This action is irreversible. Proceed?</i>"
    )

    await callback.message.edit_text(
        confirm_text,
        parse_mode="HTML",
        reply_markup=get_confirm_cancel_kb(f"vendor_delete:{vendor_id}")
    )
    await callback.answer()


# CONFIRM: actually delete
@router.callback_query(F.data.startswith("vendor_delete:") & F.data.endswith("_confirm"))
async def vendor_delete_confirm(callback: CallbackQuery):
    try:
        vendor_id_str = callback.data.split(":")[1].split("_")[0]
        vendor_id = int(vendor_id_str)
    except Exception:
        await callback.answer("❌ Invalid vendor ID format.", show_alert=True)
        return

    async with db._open_connection() as conn:
        await conn.execute("DELETE FROM vendors WHERE id=$1", vendor_id)

    await callback.message.answer(
    f"🗑 Vendor #{vendor_id} deleted successfully.",
    parse_mode="HTML",
    reply_markup=get_main_menu_kb()  # ReplyKeyboardMarkup is fine here
)

    await callback.answer("Vendor deleted.")


@router.callback_query(
    (F.data == "vendor_confirm") | (F.data == "vendor_edit_confirm"),
    AdminStates.vendor_confirm
)
async def vendor_commit(call: CallbackQuery, state: FSMContext):
    """Handles both add and edit vendor commit actions."""
    data = await state.get_data()

    try:
        if data.get("edit_mode"):
            # Update existing vendor
            await db.update_vendor(
                vendor_id=data["vendor_db_id"],
                telegram_id=data["v_id"],
                name=data["v_name"],
                status=data.get("v_status", "active")
            )
            await call.message.edit_text(
                f"✅ <b>SUCCESS: VENDOR UPDATED</b>\n"
                f"Vendor <b>{data['v_name']}</b> has been updated.",
                parse_mode="HTML"
            )
            logger.info(
                f"[ADMIN:VENDOR] Updated vendor {data['v_name']} "
                f"(DB ID: {data['vendor_db_id']})"
            )
        else:
            # Create new vendor
            existing = await db.get_vendor_by_telegram(data['v_id'])
            if existing:
                await call.message.edit_text(
                    f"⚠️ <b>Failed:</b> Vendor with Telegram ID {data['v_id']} already exists.",
                    parse_mode="HTML"
                )
                await state.clear()
                return

            vid = await db.create_vendor(data['v_id'], data['v_name'])
            await call.message.edit_text(
                f"✅ <b>SUCCESS: VENDOR DEPLOYED</b>\n"
                f"Reference ID: <code>{vid}</code>\n"
                f"Vendor <b>{data['v_name']}</b> is now active.",
                parse_mode="HTML"
            )
            logger.info(
                f"[ADMIN:VENDOR] Created vendor {data['v_name']} "
                f"(Telegram ID: {data['v_id']})"
            )

        # Restore main menu
        await call.message.answer("Ready for next command.", reply_markup=get_main_menu_kb())

    except Exception as e:
        logger.error(f"[ADMIN:ERROR] Vendor commit failed: {e}")
        await call.message.edit_text(
            f"💥 <b>CRITICAL ERROR:</b> {str(e)}",
            parse_mode="HTML"
        )

    await state.clear()
    
    
    
    
    
    
    
    
    
# =============================================================================
# Delivery Guy LOGIC
# =============================================================================
def build_dg_list_text(rows) -> str:
    summary_lines = ["🚴 <b>Delivery Guys</b>", "━━━━━━━━━━━━━━━━━━━━━━"]
    for i, r in enumerate(rows, start=1):
        status_emoji = "🟢 Online" if r["active"] else "🔴 Offline"
        if r["blocked"]:
            status_emoji = "⛔ Blocked"
        acceptance_rate = (r["accepted_requests"]/r["total_requests"]*100) if r["total_requests"] else 100
        summary_lines.append(
            f"{i}️⃣ <b>{r['name']}</b> ({r['campus']})\n"
            f"   {status_emoji} • 📊 {acceptance_rate:.1f}% • 📦 {r['total_deliveries']} deliveries\n"
        )
    summary_lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    summary_lines.append("🔍 Select a DG below to view full details")
    return "\n".join(summary_lines)


def build_dg_list_kb(rows) -> InlineKeyboardMarkup:
    dgs = [{"id": r["id"], "name": r["name"]} for r in rows]
    kb = get_dg_list_kb(dgs)  # reuse your existing helper
    return kb


@router.message(F.text == "🚴 Delivery Guys", F.from_user.id.in_(settings.ADMIN_IDS))
async def admin_dgs_entry(message: Message, state: FSMContext):
    await state.clear()
    async with db._open_connection() as conn:
        rows = await conn.fetch(
            "SELECT id, name, phone, campus, active, blocked, total_deliveries, accepted_requests, total_requests, skipped_requests FROM delivery_guys ORDER BY id ASC"
        )
    if not rows:
        # Show message + action buttons
        text = "⚠️ No delivery guys found.\n\nTap below to add one or go back."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Add DG", callback_data="dg_add"),
                InlineKeyboardButton(text="⬅️ Back to Main", callback_data="admin_back")
            ]
        ])
        await message.answer(text, reply_markup=kb)
        return

    await message.answer(build_dg_list_text(rows), reply_markup=build_dg_list_kb(rows), parse_mode="HTML")


@router.callback_query(F.data == "admin_dgs")
async def dg_back_to_list(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
    except Exception:
        pass
    await state.clear()

    async with db._open_connection() as conn:
        rows = await conn.fetch(
            "SELECT id, name, phone, campus, active, blocked, total_deliveries, accepted_requests, total_requests, skipped_requests FROM delivery_guys ORDER BY id ASC"
        )
    if not rows:
        await callback.message.edit_text("⚠️ No delivery guys found.", reply_markup=get_main_menu_kb(), parse_mode="HTML")
        return

    try:
        await callback.message.edit_text(build_dg_list_text(rows), reply_markup=build_dg_list_kb(rows), parse_mode="HTML")
    except Exception:
        await callback.message.answer(build_dg_list_text(rows), reply_markup=build_dg_list_kb(rows), parse_mode="HTML")

    
@router.callback_query(F.data.startswith("dg_view:"))
async def dg_view_callback(callback: CallbackQuery, state: FSMContext):
    # Answer immediately
    try:
        await callback.answer()
    except Exception:
        pass

    dg_id = int(callback.data.split(":")[1])
    async with db._open_connection() as conn:
        dg = await conn.fetchrow("SELECT * FROM delivery_guys WHERE id = $1", dg_id)

    if not dg:
        await callback.message.answer("⚠️ DG not found.")
        return

    status_emoji = "🟢 Online" if dg["active"] else "🔴 Offline"
    if dg["blocked"]:
        status_emoji = "⛔ Blocked"

    acceptance_rate = (dg["accepted_requests"]/dg["total_requests"]*100) if dg["total_requests"] else 100

    card_text = (
        f"🛵 <b>Delivery Guy: {dg['name']}</b>\n"
        f"📱 Phone: {dg['phone']}\n"
        f"🆔 ID: <code>{dg['telegram_id']}</code>\n"
        f"🏛 Campus: {dg['campus']}\n"
        f"⚧ Gender: {dg.get('gender','—')}\n"
        f"⚡ Status: {status_emoji}\n"
        f"📊 Acceptance Rate: {acceptance_rate:.1f}%\n"
        f"📦 Deliveries: {dg['total_deliveries']} • Skipped: {dg['skipped_requests']}\n"
        f"🪙 Coins: {dg['coins']} • XP: {dg['xp']} • Level: {dg['level']}\n"
    )

    kb = get_dg_card_kb(dg_id)

    from aiogram.exceptions import TelegramBadRequest
    try:
        await callback.message.edit_text(card_text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Ignore silently
            pass
        else:
            raise
    except Exception:
        await callback.message.answer(card_text, reply_markup=kb, parse_mode="HTML")


def get_dg_card_kb(dg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"dg_edit:{dg_id}"),
            InlineKeyboardButton(text="⛔ Block", callback_data=f"dg_block:{dg_id}")
        ],
        [
            InlineKeyboardButton(text="🗑 Delete", callback_data=f"dg_delete:{dg_id}"),
            InlineKeyboardButton(text="📈 Stats", callback_data=f"dg_stats:{dg_id}")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back to List", callback_data="admin_dgs")
        ]
    ])


@router.callback_query(F.data == "dg_add", F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_add_init(callback: CallbackQuery, state: FSMContext):
    # Answer immediately
    try:
        await callback.answer()
    except Exception:
        pass

    await state.clear()
    await callback.message.answer(
        "<b>🚴 Delivery Guy Onboarding // STEP 1/5</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram ID</b> of the Delivery Agent.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.dg_get_id)


@router.message(AdminStates.dg_get_id)
async def dg_id_captured(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ ID must be numeric. Try again.")
        return
    await state.update_data(dg_id=int(message.text))
    await message.answer("<b>Step 2/5:</b> Enter <b>Full Name</b>.", parse_mode="HTML")
    await state.set_state(AdminStates.dg_get_name)

@router.message(AdminStates.dg_get_name)
async def dg_name_captured(message: Message, state: FSMContext):
    await state.update_data(dg_name=message.text)
    await message.answer("<b>Step 3/4:</b> Enter <b>Phone Number</b> (e.g., 0911...).", parse_mode="HTML")
    await state.set_state(AdminStates.dg_get_phone)

@router.message(AdminStates.dg_get_phone)
async def dg_phone_captured(message: Message, state: FSMContext):
    await state.update_data(dg_phone=message.text)
    await message.answer(
        "<b>Step 4/5:</b> Select <b>Primary Campus</b>.", 
        parse_mode="HTML", 
        reply_markup=get_campus_kb()
    )
    await state.set_state(AdminStates.dg_get_campus)


@router.message(AdminStates.dg_get_campus)
async def dg_campus_captured(message: Message, state: FSMContext):
    await state.update_data(dg_campus=message.text)
    await message.answer(
        "<b>Step 5/5:</b> Select <b>Gender</b> of the Delivery Agent.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Male"), KeyboardButton(text="Female")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(AdminStates.dg_get_gender)


@router.message(AdminStates.dg_get_gender)
async def dg_gender_captured(message: Message, state: FSMContext):
    await state.update_data(dg_gender=message.text)
    data = await state.get_data()

    summary = (
        "<b>📋 VERIFY AGENT PROFILE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> <code>{data['dg_id']}</code>\n"
        f"👤 <b>Name:</b> {data['dg_name']}\n"
        f"📱 <b>Phone:</b> {data['dg_phone']}\n"
        f"🏛 <b>Campus:</b> {data['dg_campus']}\n"
        f"⚧ <b>Gender:</b> {data['dg_gender']}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Confirm to commit this Delivery Guy to the database?</i>"
    )
    await message.answer(summary, parse_mode="HTML", reply_markup=get_confirm_cancel_kb("dg"))
    await state.set_state(AdminStates.dg_confirm)
@router.callback_query(F.data == "dg_confirm", AdminStates.dg_confirm)
async def dg_commit(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    try:
        user_id = await db.get_internal_user_id(data['dg_id'])
        new_gender = data['dg_gender'].strip().lower()

        if not user_id:
            user_id = await db.create_user(
                telegram_id=data['dg_id'], role="delivery_guy",
                first_name=data['dg_name'], phone=data['dg_phone'],
                campus=data['dg_campus'], gender=new_gender
            )

        if await db.get_delivery_guy_by_user(data['dg_id']):
            await call.message.edit_text("⚠️ <b>Error:</b> Agent already exists.", parse_mode="HTML")
            await state.clear()
            return
        
        new_gender = data['dg_gender'].strip().lower()

        dg_id = await db.create_delivery_guy(data['dg_id'], data['dg_name'], data['dg_campus'], new_gender, data['dg_phone'])

        await call.message.edit_text(
            f"✅ <b>AGENT ONBOARDED SUCCESSFULLY</b>\n"
            f"Agent <b>{data['dg_name']}</b> is active.\n"
            f"System ID: <code>{dg_id}</code>",
            parse_mode="HTML"
        )
        logger.info(f"[ADMIN:DG] Onboarded {data['dg_name']} (System ID: {dg_id})")
        await call.message.answer("Ready.", reply_markup=get_main_menu_kb())

    except Exception as e:
        logger.exception("DG Error")
        await call.message.edit_text(f"💥 System Failure: {e}", parse_mode=None)
    await state.clear()


class DGEditStates(StatesGroup):
    dg_edit_phone = State()
    dg_edit_campus = State()
    dg_edit_status = State()
    dg_edit_tgid = State()
    dg_edit_gender = State()

@router.callback_query(F.data.startswith("dg_edit:"))
async def dg_edit_init(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await state.update_data(dg_id=dg_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="✏️ Edit Phone", callback_data=f"dg_edit_phone:{dg_id}"),
        InlineKeyboardButton(text="🏛 Edit Campus", callback_data=f"dg_edit_campus:{dg_id}")
    ],
    [
        InlineKeyboardButton(text="⚡ Edit Status", callback_data=f"dg_edit_status:{dg_id}"),
        InlineKeyboardButton(text="⚧ Edit Gender", callback_data=f"dg_edit_gender:{dg_id}")
    ],
    [
        InlineKeyboardButton(text="🆔 Edit Telegram ID", callback_data=f"dg_edit_tgid:{dg_id}")  # ✅ NEW
    ],
    [
        InlineKeyboardButton(text="⛔ Block", callback_data=f"dg_block:{dg_id}"),
        InlineKeyboardButton(text="🗑 Delete", callback_data=f"dg_delete:{dg_id}")
    ],
    [
        InlineKeyboardButton(text="📈 Stats", callback_data=f"dg_stats:{dg_id}"),
        InlineKeyboardButton(text="⬅️ Back to List", callback_data="admin_dgs")
    ]
])
    await callback.message.edit_text("Choose a field to edit:", reply_markup=kb)
    await callback.answer()

@router.callback_query(F.data == "dg_edit_phone")
async def dg_edit_phone_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Enter new phone number:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(DGEditStates.dg_edit_phone)
    await callback.answer()


@router.callback_query(F.data.startswith("dg_edit_gender:"))
async def dg_edit_gender_start(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await state.update_data(dg_id=dg_id)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Male"), KeyboardButton(text="Female")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await callback.message.answer("Select new gender:", reply_markup=kb)
    await state.set_state(DGEditStates.dg_edit_gender)
    await callback.answer()


@router.message(DGEditStates.dg_edit_gender)
async def dg_edit_gender_commit(message: Message, state: FSMContext):
    data = await state.get_data()
    new_gender = message.text.strip().lower()
    if new_gender not in ["male", "female"]:
        await message.answer("⚠️ Invalid gender. Please choose Male or Female.")
        return

    async with db._open_connection() as conn:
        await conn.execute("UPDATE delivery_guys SET gender=$1 WHERE id=$2", new_gender, data["dg_id"])

    await message.answer(f"✅ Gender updated to {new_gender}.", reply_markup=get_main_menu_kb())
    logger.info(f"[ADMIN:DG] Updated gender for DG {data['dg_id']} → {new_gender}")
    await state.clear()



@router.callback_query(F.data.startswith("dg_edit_tgid:"))
async def dg_edit_tgid_start(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await state.update_data(dg_id=dg_id)

    await callback.message.answer(
        "🆔 Enter the **new Telegram ID** for this delivery agent.\n\n"
        "⚠️ Must be numeric and belong to the agent’s Telegram account.",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="Markdown"
    )

    await state.set_state(DGEditStates.dg_edit_tgid)
    await callback.answer()


@router.message(DGEditStates.dg_edit_tgid)
async def dg_edit_tgid_commit(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ Telegram ID must be numeric. Try again.")
        return

    new_tgid = int(message.text)
    data = await state.get_data()
    dg_id = data["dg_id"]

    async with db._open_connection() as conn:
        async with conn.transaction():

            # 1️⃣ Get linked user_id
            row = await conn.fetchrow(
                "SELECT user_id FROM delivery_guys WHERE id=$1",
                dg_id
            )
            if not row:
                await message.answer("❌ Delivery agent not found.")
                await state.clear()
                return

            user_id = row["user_id"]

            # 2️⃣ Prevent Telegram ID collision
            # exists = await conn.fetchval(
            #     "SELECT 1 FROM users WHERE telegram_id=$1 AND id != $2",
            #     new_tgid, user_id
            # )
            # if exists:
            #     await message.answer("⚠️ This Telegram ID is already assigned to another user.")
            #     return

            # 3️⃣ Update users table
            # await conn.execute(
            #     "UPDATE users SET telegram_id=$1 WHERE id=$2",
            #     new_tgid, user_id
            # )

            # 4️⃣ Update delivery_guys tabl
            await conn.execute(
                "UPDATE delivery_guys SET telegram_id=$1 WHERE id=$2",
                new_tgid, dg_id
            )

    await message.answer(
        f"✅ Telegram ID updated successfully.\n🆔 New ID: `{new_tgid}`",
        reply_markup=get_main_menu_kb(),
        parse_mode="Markdown"
    )

    logger.info(f"[ADMIN:DG] Updated Telegram ID for DG {dg_id} → {new_tgid}")
    await state.clear()


@router.callback_query(F.data.startswith("dg_edit_status:"))
async def dg_edit_status_start(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await state.update_data(dg_id=dg_id)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Online"), KeyboardButton(text="🔴 Offline")],
            [KeyboardButton(text="⛔ Blocked")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await callback.message.answer("Select new status:", reply_markup=kb)
    await state.set_state(DGEditStates.dg_edit_status)
    await callback.answer()



@router.message(DGEditStates.dg_edit_status)
async def dg_edit_status_commit(message: Message, state: FSMContext):
    data = await state.get_data()
    new_status = message.text.strip()

    # Map text → DB fields
    active, blocked = False, False
    if "Online" in new_status:
        active, blocked = True, False
    elif "Offline" in new_status:
        active, blocked = False, False
    elif "Blocked" in new_status:
        active, blocked = False, True
    else:
        await message.answer("⚠️ Invalid status. Please choose Online, Offline, or Blocked.")
        return

    async with db._open_connection() as conn:
        await conn.execute(
            "UPDATE delivery_guys SET active=$1, blocked=$2 WHERE id=$3",
            active, blocked, data["dg_id"]
        )

    await message.answer(f"✅ Status updated to {new_status}.", reply_markup=get_main_menu_kb())
    logger.info(f"[ADMIN:DG] Updated status for DG {data['dg_id']} → {new_status}")
    await state.clear()

@router.message(DGEditStates.dg_edit_phone)
async def dg_edit_phone_commit(message: Message, state: FSMContext):
    data = await state.get_data()
    async with db._open_connection() as conn:
        await conn.execute("UPDATE delivery_guys SET phone=$1 WHERE id=$2", message.text, data["dg_id"])
    await message.answer("✅ Phone updated.", reply_markup=get_main_menu_kb())
    await state.clear()


@router.callback_query(F.data.startswith("dg_block:"))
async def dg_block(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    async with db._open_connection() as conn:
        await conn.execute("UPDATE delivery_guys SET blocked=TRUE WHERE id=$1", dg_id)
    await callback.message.edit_text("⛔ DG blocked successfully.")
    await callback.answer()

@router.callback_query(F.data.startswith("dg_unblock:"))
async def dg_unblock(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    async with db._open_connection() as conn:
        await conn.execute("UPDATE delivery_guys SET blocked=FALSE WHERE id=$1", dg_id)
    await callback.message.edit_text("✅ DG unblocked successfully.")
    await callback.answer()


# --- Delete Confirmation Flow ---
def get_delete_confirm_kb(dg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Confirm Delete", callback_data=f"dg_delete_confirm:{dg_id}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"dg_view:{dg_id}")
        ]
    ])

@router.callback_query(F.data.startswith("dg_delete:"))
async def dg_delete_init(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        f"⚠️ Are you sure you want to delete DG <code>{dg_id}</code>?",
        parse_mode="HTML",
        reply_markup=get_delete_confirm_kb(dg_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("dg_delete_confirm:"))
async def dg_delete_confirm(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])

    async with db._open_connection() as conn:
        async with conn.transaction():
            # 1. Get linked user_id
            row = await conn.fetchrow(
                "SELECT user_id FROM delivery_guys WHERE id=$1",
                dg_id
            )

            if not row or not row["user_id"]:
                await callback.message.edit_text("⚠️ DG not found or already deleted.")
                return

            user_id = row["user_id"]

            # 2. Delete delivery guy
            await conn.execute(
                "DELETE FROM delivery_guys WHERE id=$1",
                dg_id
            )

            # 3. Delete user
            await conn.execute(
                "DELETE FROM users WHERE id=$1",
                user_id
            )

    await callback.message.edit_text("🗑 Delivery Guy and linked user deleted successfully.")
    await callback.answer()


# --- Edit Campus Flow ---
@router.callback_query(F.data.startswith("dg_edit_campus:"))
async def dg_edit_campus_init(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])
    await state.update_data(dg_id=dg_id)
    await callback.message.edit_text(
        "🏛 Enter the new campus for this Delivery Guy:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(DGEditStates.dg_edit_campus)
    await callback.answer()

@router.message(DGEditStates.dg_edit_campus)
async def dg_edit_campus_captured(message: Message, state: FSMContext):
    data = await state.get_data()
    new_campus = message.text.strip()
    async with db._open_connection() as conn:
        await conn.execute(
            "UPDATE delivery_guys SET campus=$1 WHERE id=$2",
            new_campus, data["dg_id"]
        )
    await message.answer(f"✅ Campus updated to <b>{new_campus}</b>.", parse_mode="HTML", reply_markup=get_main_menu_kb())
    await state.clear()



@router.callback_query(F.data.startswith("dg_stats:"))
async def dg_stats(callback: CallbackQuery, state: FSMContext):
    dg_id = int(callback.data.split(":")[1])

    async with db._open_connection() as conn:
        # Count active orders by status
        active_rows = await conn.fetch(
            """
            SELECT status, COUNT(*) AS count
            FROM orders
            WHERE delivery_guy_id = $1
              AND status IN ('assigned','preparing','ready','in_progress')
            GROUP BY status
            """,
            dg_id
        )

        # Fetch lifetime stats
        dg = await conn.fetchrow(
            """
            SELECT total_deliveries, accepted_requests, total_requests, skipped_requests
            FROM delivery_guys
            WHERE id = $1
            """,
            dg_id
        )

    if not dg:
        await callback.message.edit_text("⚠️ DG not found.")
        await callback.answer()
        return

    # Build stats summary
    lines = ["📈 <b>Delivery Guy Stats</b>", "━━━━━━━━━━━━━━━━━━━━━━"]

    if active_rows:
        lines.append("🟢 <b>Active Orders</b>")
        status_map = {
            "assigned": "📌 Assigned",
            "preparing": "👨‍🍳 Preparing",
            "ready": "✅ Ready",
            "in_progress": "🚚 In Progress"
        }
        for r in active_rows:
            lines.append(f"{status_map.get(r['status'], r['status'])}: {r['count']}")
    else:
        lines.append("🟢 No active orders.")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━")

    acceptance_rate = (dg["accepted_requests"] / dg["total_requests"] * 100) if dg["total_requests"] else 100
    lines.append(f"📦 Total Deliveries: {dg['total_deliveries']}")
    lines.append(f"📊 Acceptance Rate: {acceptance_rate:.1f}%")
    lines.append(f"⏭ Skipped Requests: {dg['skipped_requests']}")

    # Inline keyboard with back button
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back", callback_data=f"dg_view:{dg_id}")]
    ])

    await callback.message.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=kb)
    await callback.answer()
# ==============================================================================
# 🛵 PROTOCOL: DELIVERY FLEET ONBOARDING
# ==============================================================================
@router.message(F.text == "🛵 Add Delivery Guy", F.from_user.id.in_(settings.ADMIN_IDS))
async def dg_start(message: Message, state: FSMContext):
    await message.answer(
        "<b>🛵 FLEET RECRUITMENT // STEP 1/4</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram ID</b> of the Delivery Agent.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.dg_get_id)

@router.message(AdminStates.dg_get_id)
async def dg_id_captured(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ Numeric ID only.", parse_mode="HTML")
        return
    
    await state.update_data(dg_id=int(message.text))
    await message.answer("<b>Step 2/4:</b> Enter <b>Full Name</b>.", parse_mode="HTML")
    await state.set_state(AdminStates.dg_get_name)

@router.message(AdminStates.dg_get_name)
async def dg_name_captured(message: Message, state: FSMContext):
    await state.update_data(dg_name=message.text)
    await message.answer("<b>Step 3/4:</b> Enter <b>Phone Number</b> (e.g., 0911...).", parse_mode="HTML")
    await state.set_state(AdminStates.dg_get_phone)

@router.message(AdminStates.dg_get_phone)
async def dg_phone_captured(message: Message, state: FSMContext):
    await state.update_data(dg_phone=message.text)
    await message.answer(
        "<b>Step 4/4:</b> Select <b>Primary Campus</b>.", 
        parse_mode="HTML", 
        reply_markup=get_campus_kb()
    )
    await state.set_state(AdminStates.dg_get_campus)

@router.message(AdminStates.dg_get_campus)
async def dg_campus_captured(message: Message, state: FSMContext):
    await state.update_data(dg_campus=message.text)
    data = await state.get_data()
    
    summary = (
        "<b>📋 VERIFY AGENT PROFILE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>ID:</b> <code>{data['dg_id']}</code>\n"
        f"👤 <b>Name:</b> {data['dg_name']}\n"
        f"📱 <b>Phone:</b> {data['dg_phone']}\n"
        f"📍 <b>Zone:</b> {data['dg_campus']}\n"
    )
    # Remove the campus keyboard before showing inline
    await message.answer("Generating profile...", reply_markup=ReplyKeyboardRemove())
    await message.answer(summary, parse_mode="HTML", reply_markup=get_confirm_cancel_kb("dg"))
    await state.set_state(AdminStates.dg_confirm)

@router.callback_query(F.data == "dg_confirm", AdminStates.dg_confirm)
async def dg_commit(call: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    try:
        user_id = await db.get_internal_user_id(data['dg_id'])
        # if not user_id:
        #     user_id = await db.create_user(
        #         telegram_id=data['dg_id'], role="delivery", 
        #         first_name=data['dg_name'], phone=data['dg_phone'], campus=data['dg_campus']
        #     )
        
        # Check existing DG
        if await db.get_delivery_guy_by_user(data['dg_id']):
             await call.message.edit_text("⚠️ <b>Error:</b> Agent already exists.", parse_mode="HTML")
             return

        dg_id = await db.create_delivery_guy(data['dg_id'], data['dg_name'], data['dg_campus'])
        
        await call.message.edit_text(
            f"✅ <b>AGENT ONBOARDED SUCCESSFULLY</b>\n"
            f"Agent <b>{data['dg_name']}</b> is active.\n"
            f"System ID: <code>{dg_id}</code>",
            parse_mode="HTML"
        )
        logger.info(f"[ADMIN:DG] Onboarded {data['dg_name']}")
        await call.message.answer("Ready.", reply_markup=get_main_menu_kb())
        
    except Exception as e:
        logger.exception("DG Error")
        await call.message.edit_text(f"💥 System Failure: {e}")
    
    await state.clear()

# ==============================================================================
# 📢 PROTOCOL: BROADCAST SYSTEM
# ==============================================================================
@router.message(F.text == "📢 Broadcast", F.from_user.id.in_(settings.ADMIN_IDS))
async def broadcast_start(message: Message, state: FSMContext):
    await message.answer(
        "<b>📢 BROADCAST TRANSMISSION</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Send the message (Text/Photo/Video) you wish to send to <b>ALL USERS</b>.\n"
        "<i>Markdown formatting is supported.</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.broadcast_get_content)

@router.message(AdminStates.broadcast_get_content)
async def broadcast_preview(message: Message, state: FSMContext):
    # Copy message to show preview
    await state.update_data(msg_id=message.message_id, chat_id=message.chat.id)
    
    await message.answer("<b>👁 PREVIEWING TRANSMISSION...</b>", parse_mode="HTML")
    await message.copy_to(chat_id=message.chat.id)
    
    await message.answer(
        "<b>⚠️ CONFIRM MASS TRANSMISSION</b>\n"
        "This will reach all active users.", 
        parse_mode="HTML", 
        reply_markup=get_confirm_cancel_kb("broadcast")
    )
    await state.set_state(AdminStates.broadcast_confirm)

@router.callback_query(F.data == "broadcast_confirm", AdminStates.broadcast_confirm)
async def broadcast_execute(call: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    
    # Placeholder for background task
    # In production: Use Celery or asyncio.create_task for the loop
    simulated_count = 1420 
    
    await call.message.edit_text(
        f"🚀 <b>TRANSMISSION STARTED</b>\n"
        f"Target Audience: ~{simulated_count} nodes.\n"
        f"<i>Process is running in background.</i>",
        parse_mode="HTML"
    )
    logger.info(f"[ADMIN:BROADCAST] Started broadcast msg_id={data['msg_id']}")
    await call.message.answer("Systems Normal.", reply_markup=get_main_menu_kb())
    await state.clear()

# ==============================================================================
# ⚙️ PROTOCOL: SETTINGS & MODERATION
# ==============================================================================
@router.message(F.text == "⚙️ Settings", F.from_user.id.in_(settings.ADMIN_IDS))
async def settings_menu(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⛔ Block Delivery Agent", callback_data="setting_block_dg")],
        [InlineKeyboardButton(text="✅ Unblock Delivery Agent", callback_data="setting_unblock_dg")],
        [InlineKeyboardButton(text="🗑 Close Menu", callback_data="admin_cancel_inline")]
    ])
    
    await message.answer(
        "<b>⚙️ SYSTEM CONFIGURATION</b>\n"
        "Select a modification parameter:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data == "setting_block_dg")
async def block_dg_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("<b>⛔ BLOCK PROTOCOL</b>\nEnter Target DG Telegram ID:", parse_mode="HTML")
    await state.set_state(AdminStates.block_dg_get_id)

@router.message(AdminStates.block_dg_get_id)
async def block_dg_reason(message: Message, state: FSMContext):
    await state.update_data(target_id=message.text)
    await message.answer("📝 Enter <b>Reason</b> for blocking (logged):", parse_mode="HTML")
    await state.set_state(AdminStates.block_dg_reason)

@router.message(AdminStates.block_dg_reason)
async def block_dg_confirm_step(message: Message, state: FSMContext):
    reason = message.text
    await state.update_data(reason=reason)
    data = await state.get_data()
    
    await message.answer(
        f"⚠️ <b>CONFIRM BAN</b>\nTarget: <code>{data['target_id']}</code>\nReason: {reason}",
        parse_mode="HTML",
        reply_markup=get_confirm_cancel_kb("block_dg")
    )
    await state.set_state(AdminStates.block_dg_confirm)

@router.callback_query(F.data == "block_dg_confirm", AdminStates.block_dg_confirm)
async def block_dg_execute(call: CallbackQuery, state: FSMContext, db: Database, bot: Bot):
    data = await state.get_data()
    target_id = int(data['target_id'])
    
    try:
        await db.block_delivery_guy(target_id, data['reason'])
        
        # Notify user safely
        with contextlib.suppress(Exception):
            await bot.send_message(target_id, f"⛔ <b>Access Revoked.</b> Reason: {data['reason']}", parse_mode="HTML")
            
        await call.message.edit_text("✅ <b>Target Neutralized (Blocked).</b>", parse_mode="HTML")
        logger.warning(f"[ADMIN:BAN] Blocked {target_id} - {data['reason']}")
        
    except Exception as e:
        await call.message.edit_text(f"❌ Error: {e}")
        
    await call.message.answer("Returning to Command.", reply_markup=get_main_menu_kb())
    await state.clear()

# ==============================================================================
# 📈 DASHBOARDS: ANALYTICS & FINANCE
# ==============================================================================
@router.message(F.text == "📈 Analytics", F.from_user.id.in_(settings.ADMIN_IDS))
async def analytics_view(message: Message):
    async with db._open_connection() as conn:
        # Users
        users_total = await conn.fetchval("SELECT COUNT(*) FROM users")
        users_active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status='active'")
        users_blocked = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status!='active'")

        # Orders
        orders_today = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE created_at::date=CURRENT_DATE")
        completed_orders = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE status='delivered' AND created_at::date=CURRENT_DATE")
        cancelled_orders = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE status='cancelled' AND created_at::date=CURRENT_DATE")
        avg_order_value = await conn.fetchval("SELECT AVG(food_subtotal+delivery_fee) FROM orders WHERE status='delivered' AND created_at::date=CURRENT_DATE")
        revenue_today = await conn.fetchval("SELECT COALESCE(SUM(food_subtotal+delivery_fee),0) FROM orders WHERE status='delivered' AND created_at::date=CURRENT_DATE")

        # Fleet
        active_fleet = await conn.fetchval("SELECT COUNT(*) FROM delivery_guys WHERE active=TRUE AND blocked=FALSE")
        total_fleet = await conn.fetchval("SELECT COUNT(*) FROM delivery_guys")
        fleet_utilization = f"{(active_fleet/total_fleet*100):.1f}%" if total_fleet else "0%"
        avg_acceptance_rate = await conn.fetchval(
            "SELECT AVG(acceptance_rate) FROM daily_stats WHERE date::date = CURRENT_DATE"
        )

        # Vendors
        vendors_total = await conn.fetchval("SELECT COUNT(*) FROM vendors")
        vendors_active = await conn.fetchval("SELECT COUNT(*) FROM vendors WHERE status='active'")
        top_vendor = await conn.fetchrow("""
            SELECT v.name, COUNT(*) AS orders
            FROM orders o
            JOIN vendors v ON o.vendor_id=v.id
            WHERE o.created_at::date=CURRENT_DATE
            GROUP BY v.name
            ORDER BY orders DESC LIMIT 1
        """)

        # Ratings
        avg_delivery_rating = await conn.fetchval("SELECT AVG(stars) FROM ratings WHERE type='delivery'")
        avg_vendor_rating = await conn.fetchval(
            "SELECT COALESCE(AVG(stars), 0) FROM ratings WHERE type='vendor'"
        )        
        ratings_today = await conn.fetchval("SELECT COUNT(*) FROM ratings WHERE created_at::date=CURRENT_DATE")

        # System health
        jobs_today = await conn.fetchval("SELECT COUNT(*) FROM jobs_log WHERE created_at::date=CURRENT_DATE")
        tickets_open = await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='open'")
        subs_active = await conn.fetchval("SELECT COUNT(*) FROM subscriptions WHERE status='active'")

    conversion_rate = f"{(completed_orders/orders_today*100):.1f}%" if orders_today else "0%"

    txt = (
    "<b>📈 LIVE TELEMETRY</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "👥 <b>Users</b>\n"
    f"   • Total: <code>{users_total}</code>\n"
    f"   • Active: <code>{users_active}</code>\n"
    f"   • Blocked: <code>{users_blocked}</code>\n\n"

    "📦 <b>Orders (Today)</b>\n"
    f"   • Total: <code>{orders_today}</code>\n"
    f"   • Completed: ✅ {completed_orders}\n"
    f"   • Cancelled: ❌ {cancelled_orders}\n"
    f"   • Conversion: <code>{conversion_rate}</code>\n"
    f"   • Avg Value: {avg_order_value:.1f} ETB\n"
    f"   • Revenue: {revenue_today} ETB\n\n"

    "🚴 <b>Fleet</b>\n"
    f"   • Active: <code>{active_fleet}</code> / {total_fleet}\n"
    f"   • Utilization: {fleet_utilization}\n"
    f"   • Avg Acceptance Rate: {avg_acceptance_rate:.1f}%\n\n"

    "🏪 <b>Vendors</b>\n"
    f"   • Total: <code>{vendors_total}</code>\n"
    f"   • Active: {vendors_active}\n"
    f"   • Top Today: {top_vendor['name']} ({top_vendor['orders']} orders)\n\n"

    "⭐ <b>Ratings</b>\n"
    f"   • Delivery: {avg_delivery_rating:.1f}\n"
    f"   • Vendor: {avg_vendor_rating:.1f}\n"
    f"   • Submitted Today: {ratings_today}\n\n"

    "🛡 <b>System Health</b>\n"
    f"   • Jobs Today: {jobs_today}\n"
    f"   • Tickets Open: {tickets_open}\n"
    f"   • Active Subs: {subs_active}\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "<i>Data updated: Just now</i>"
)


    await message.answer(txt, parse_mode="HTML", reply_markup=get_analytics_kb())
    
    
@router.message(F.text == "💰 Finance", F.from_user.id.in_(settings.ADMIN_IDS))
async def finance_view(message: Message):
    # Mock data
    revenue = 45200.50
    payouts = 3200.00
    
    txt = (
        "<b>💰 FINANCIAL OVERVIEW</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 <b>Gross Revenue:</b> <code>{revenue:,.2f} ETB</code>\n"
        f"💸 <b>Pending Payouts:</b> <code>{payouts:,.2f} ETB</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Secure ledger access required for details.</i>"
    )
    await message.answer(txt, parse_mode="HTML")

@router.message(F.text == "🛡 System Status", F.from_user.id.in_(settings.ADMIN_IDS))
async def system_health(message: Message):
    await message.answer(
        "<b>🛡 SYSTEM DIAGNOSTICS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔌 <b>Database:</b> 🟢 CONNECTED (12ms)\n"
        "🤖 <b>Bot API:</b> 🟢 ONLINE\n"
        "💳 <b>Payment Gateway:</b> 🟡 LATENCY DETECTED\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode="HTML"
    )

@router.message(F.text == "🛑 Emergency Stop", F.from_user.id.in_(settings.ADMIN_IDS))
async def panic_button(message: Message):
    await message.answer(
        "<b>🛑 EMERGENCY INTERRUPT</b>\n\n"
        "Are you sure you want to <b>HALT ALL BOT OPERATIONS?</b>\n"
        "<i>This requires manual server restart to undo.</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💀 YES, KILL PROCESS", callback_data="kill_process"),
            InlineKeyboardButton(text="🔙 Cancel", callback_data="admin_cancel_inline")
        ]])
    )
    
    
# ==============================================================================
# 🆘 PROTOCOL: SUPPORT TICKET MANAGEMENT
    
@router.message(F.text == "🆘 Support", F.from_user.id.in_(settings.ADMIN_IDS))
async def support_dashboard(message: Message):
    rows = await db.list_open_tickets()
    closed = await db.list_closed_tickets()  # add this helper

    text = (
        f"📊 Support Dashboard\n"
        f"Open tickets: {len(rows)}\n"
        f"Closed tickets: {len(closed)}\n\n"
        "Select a ticket to manage:"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{row['ticket_id']} — {row['status']}", callback_data=f"support_manage:{row['ticket_id']}")]
        for row in rows
    ])
    await message.answer(text, reply_markup=kb)
    
@router.callback_query(F.data.startswith("support_manage:"))
async def manage_ticket(cb: CallbackQuery):
    ticket_id = cb.data.split(":", 1)[1]
    ticket = await db.get_ticket(ticket_id)
    if not ticket:
        await cb.answer("Ticket not found.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Reply", callback_data=f"support_reply_start:{ticket_id}:{ticket['user_id']}")],
        [InlineKeyboardButton(text="🔄 Need More Info", callback_data=f"support_reply:{ticket['user_id']}:info"),
         InlineKeyboardButton(text="📞 Call Back", callback_data=f"support_reply:{ticket['user_id']}:callback"),
         InlineKeyboardButton(text="✅ Resolve", callback_data=f"support_reply:{ticket['user_id']}:resolve")],
        [InlineKeyboardButton(text="✅ Close", callback_data=f"support_close:{ticket_id}")],
        [InlineKeyboardButton(text="♻️ Reopen", callback_data=f"support_reopen:{ticket_id}")]
    ])

    await cb.message.edit_text(
        f"🎟 Ticket {ticket['ticket_id']}\n"
        f"👤 User ID: {ticket['user_id']}\n"
        f"💬 Message: {ticket['text']}\n"
        f"📌 Status: {ticket['status']}\n"
        f"🕒 Created: {ticket['created_at']}\n",
        reply_markup=kb
    )



@router.callback_query(F.data.startswith("support_reopen:"))
async def reopen_ticket(cb: CallbackQuery):
    ticket_id = cb.data.split(":", 1)[1]
    ticket = await db.get_ticket(ticket_id)
    if not ticket:
        await cb.answer("Ticket not found.")
        return
    await db.reopen_ticket(ticket_id)  # add helper
    await cb.bot.send_message(ticket["user_id"], "♻️ Your support ticket has been reopened.")
    await cb.answer("Ticket reopened.")




@router.callback_query(F.data.startswith("support_reply:"))
async def handle_shortcut_reply(cb: CallbackQuery):
    _, user_id, action = cb.data.split(":")
    responses = {
        "resolve": "✅ Your issue has been marked as resolved. Thanks for reaching out!",
        "info": "🔄 Could you please provide more details so we can assist better?",
        "callback": "📞 Our team will reach out to you directly for further support."
    }
    await cb.bot.send_message(int(user_id), responses[action])
    await cb.answer("Shortcut reply sent!")



@router.callback_query(F.data.startswith("support_close:"))
async def close_ticket(cb: CallbackQuery):
    ticket_id = cb.data.split(":", 1)[1]
    ticket = await db.get_ticket(ticket_id)
    if not ticket:
        await cb.answer("Ticket not found.")
        return

    await db.close_ticket(ticket_id)
    await cb.bot.send_message(ticket["user_id"], "✅ Your support ticket has been closed. Thank you!")
    await cb.answer("Ticket closed.")


from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def get_analytics_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔙 Back to Main Menu")],
            [KeyboardButton(text="📊 Today's Analytics")],
            [KeyboardButton(text="💰 Financial Summary")],
            [KeyboardButton(text="🚚 Delivery Report")],
        ],
        resize_keyboard=True
    )


@router.message(F.text == "🔙 Back to Main Menu", F.from_user.id.in_(settings.ADMIN_IDS))
async def back_to_main_menu(message: Message, state: FSMContext):
    # Reuse your existing admin_entry to go back to dashboard
    await admin_entry(message, state)


@router.message(F.text == "📊 Today's Analytics", F.from_user.id.in_(settings.ADMIN_IDS))
async def todays_analytics(message: Message):
    analytics = AnalyticsService(db)
    summary = await analytics.summary_text()
    await message.answer(summary, parse_mode="Markdown")


@router.message(F.text == "💰 Financial Summary", F.from_user.id.in_(settings.ADMIN_IDS))
async def financial_summary(message: Message):
    analytics = AnalyticsService(db)
    summary = await analytics.summary_financial_text()
    await message.answer(summary, parse_mode="Markdown")


@router.message(F.text == "🚚 Delivery Report", F.from_user.id.in_(settings.ADMIN_IDS))
async def delivery_report(message: Message):
    analytics = AnalyticsService(db)
    report = await analytics.delivery_report_text()
    await message.answer(report, parse_mode="Markdown")






class BroadcastStates(StatesGroup):
    composing = State()
    confirming = State()

# Entry (admin menu button)
@router.message(F.text == "📢 Broadcast", F.from_user.id.in_(settings.ADMIN_IDS))
async def start_broadcast(message: Message, state: FSMContext):
    await state.set_state(BroadcastStates.composing)
    await state.update_data()
    await message.answer(
        "✍️ <b>Broadcast Composer</b>\n\n"
        "Type the message you want to send to all users.\n\n"
        "You can include HTML formatting. When ready, I'll show a preview.",
        parse_mode="HTML"
    )

# Receive text and show preview
@router.message(BroadcastStates.composing)
async def receive_broadcast_text(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text:
        await message.answer("Please type a non-empty message.")
        return

    await state.update_data(broadcast_text=text)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm & Send", callback_data="broadcast:confirm")],
        [InlineKeyboardButton(text="✏️ Edit", callback_data="broadcast:edit")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="broadcast:cancel")]
    ])
    preview = (
        "📢 <b>Broadcast Preview</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{text}\n\n"
        "<i>Confirm to send this message to all users.</i>"
    )
    await state.set_state(BroadcastStates.confirming)
    await message.answer(preview, reply_markup=kb, parse_mode="HTML")

from aiogram.exceptions import TelegramBadRequest

async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except TelegramBadRequest as e:
        return False
    except Exception:
        return False

# Confirm send
@router.callback_query(BroadcastStates.confirming, F.data == "broadcast:confirm")
async def confirm_broadcast(cb: CallbackQuery, state: FSMContext):
    await cb.answer()  # immediate ack
    data = await state.get_data()
    text = data.get("broadcast_text")
    if not text:
        await cb.message.answer("Nothing to send. Please compose a message first.")
        await state.clear()
        return

    # Fetch recipients
    recipients = await db.list_all_users()  # implement below if missing
    total = len(recipients)
    sent = 0
    failed = 0
    failed_ids = []

    # Batch send to avoid hitting Telegram rate limits
    BATCH_SIZE = 25
    PAUSE_BETWEEN_BATCHES = 1.0  # seconds

    await cb.message.edit_text("🚀 Sending broadcast... This may take a moment.", parse_mode="HTML")

    for i in range(0, total, BATCH_SIZE):
        batch = recipients[i:i + BATCH_SIZE]
        tasks = []
        for u in batch:
            chat_id = u.get("telegram_id")
            if not chat_id:
                failed += 1
                failed_ids.append(u.get("id"))
                continue
            try:
                ok = await safe_send(cb.bot, chat_id, text, parse_mode="HTML")
                if ok:
                    sent += 1
                else:
                    failed += 1
                    failed_ids.append(u.get("id"))
            except Exception:
                failed += 1
                failed_ids.append(u.get("id"))
        # polite pause
        await asyncio.sleep(PAUSE_BETWEEN_BATCHES)

    summary = (
        f"✅ Broadcast complete.\n\n"
        f"Total recipients: {total}\n"
        f"Sent: {sent}\n"
        f"Failed: {failed}\n"
    )
    if failed_ids:
        summary += "\nFailed user IDs logged."

    await cb.message.answer(summary)
    from handlers.admin_order import _notify_admin_action
    await _notify_admin_action(cb.bot, cb.from_user, "Broadcast sent", extra={"sent": sent, "failed": failed})
    await state.clear()

# Edit callback
@router.callback_query(BroadcastStates.confirming, F.data == "broadcast:edit")
async def edit_broadcast(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.set_state(BroadcastStates.composing)
    await cb.message.answer("✏️ Edit your broadcast message. Send the new text when ready.")

# Cancel callback
@router.callback_query(BroadcastStates.confirming, F.data == "broadcast:cancel")
async def cancel_broadcast(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Broadcast cancelled.")
    await state.clear()
    await cb.message.edit_text("❌ Broadcast cancelled.")
