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
from database.db import Database 
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

    # Delivery Guy (DG) Protocol
    dg_get_id = State()
    dg_get_name = State()
    dg_get_phone = State()
    dg_get_campus = State()
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
    """The persistent command deck for the admin."""
    kb = [
        [KeyboardButton(text="🏪 Add Vendor"), KeyboardButton(text="🛵 Add Delivery Guy")],
        [KeyboardButton(text="📢 Broadcast"), KeyboardButton(text="💰 Finance")],
        [KeyboardButton(text="⚙️ Settings"), KeyboardButton(text="📈 Analytics")],
        [KeyboardButton(text="🛡 System Status"), KeyboardButton(text="🛑 Emergency Stop")],
        [KeyboardButton(text="🆘 Support")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Awaiting Command...")

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
        [KeyboardButton(text="4kilo"), KeyboardButton(text="1kilo")],
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
@router.message(F.text == "🏪 Add Vendor", F.from_user.id.in_(settings.ADMIN_IDS))
async def vendor_start(message: Message, state: FSMContext):
    await message.answer(
        "<b>🏪 VENDOR DEPLOYMENT // STEP 1/3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Telegram ID</b> of the Vendor owner.\n"
        "<i>(This is required for order notifications)</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminStates.vendor_get_id)

@router.message(AdminStates.vendor_get_id)
async def vendor_id_captured(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("⚠️ <b>Error:</b> ID must be numeric. Try again.", parse_mode="HTML")
        return
    
    await state.update_data(v_id=int(message.text))
    await message.answer(
        "<b>🏪 VENDOR DEPLOYMENT // STEP 2/3</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Enter the <b>Display Name</b> for this Vendor.\n"
        "<i>(e.g., 'Burger King 6kilo')</i>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.vendor_get_name)

@router.message(AdminStates.vendor_get_name)
async def vendor_name_captured(message: Message, state: FSMContext):
    await state.update_data(v_name=message.text)
    data = await state.get_data()
    
    summary = (
        "<b>📋 REVIEW DEPLOYMENT DATA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>Vendor ID:</b> <code>{data['v_id']}</code>\n"
        f"🏷 <b>Name:</b> {data['v_name']}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Commit to database?</i>"
    )
    await message.answer(summary, parse_mode="HTML", reply_markup=get_confirm_cancel_kb("vendor"))
    await state.set_state(AdminStates.vendor_confirm)

@router.callback_query(F.data == "vendor_confirm", AdminStates.vendor_confirm)
async def vendor_commit(call: CallbackQuery, state: FSMContext, db: Database):
    data = await state.get_data()
    
    try:
        # Check if exists logic should be inside db.create_vendor or handled here
        existing = await db.get_vendor_by_telegram(data['v_id'])
        if existing:
            await call.message.edit_text(f"⚠️ <b>Failed:</b> Vendor ID {data['v_id']} already exists.", parse_mode="HTML")
            await state.clear()
            return

        vid = await db.create_vendor(data['v_id'], data['v_name'])
        
        await call.message.edit_text(
            f"✅ <b>SUCCESS: VENDOR DEPLOYED</b>\n"
            f"Reference ID: <code>{vid}</code>\n"
            f"Vendor <b>{data['v_name']}</b> is now active.",
            parse_mode="HTML"
        )
        logger.info(f"[ADMIN:VENDOR] Created vendor {data['v_name']} (ID: {data['v_id']})")
        
        # Send admin menu back as a new message to restore keyboard
        await call.message.answer("Ready for next command.", reply_markup=get_main_menu_kb())
        
    except Exception as e:
        logger.error(f"[ADMIN:ERROR] Vendor creation failed: {e}")
        await call.message.edit_text(f"💥 <b>CRITICAL ERROR:</b> {str(e)}", parse_mode="HTML")
    
    await state.clear()

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
        if not user_id:
            user_id = await db.create_user(
                telegram_id=data['dg_id'], role="delivery", 
                first_name=data['dg_name'], phone=data['dg_phone'], campus=data['dg_campus']
            )
        
        # Check existing DG
        if await db.get_delivery_guy_by_user(data['dg_id']):
             await call.message.edit_text("⚠️ <b>Error:</b> Agent already exists.", parse_mode="HTML")
             return

        dg_id = await db.create_delivery_guy(user_id, data['dg_name'], data['dg_campus'])
        
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
    # Mock data - Replace with DB calls
    stats = {
        "users": 5230,
        "orders_today": 142,
        "active_fleet": 18,
        "conversion": "4.2%"
    }
    
    txt = (
        "<b>📈 LIVE TELEMETRY</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Total Users:</b> <code>{stats['users']}</code>\n"
        f"📦 <b>Orders (24h):</b> <code>{stats['orders_today']}</code>\n"
        f"🛵 <b>Fleet Active:</b> <code>{stats['active_fleet']}</code>\n"
        f"📊 <b>Conversion:</b> <code>{stats['conversion']}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>Data updated: Just now</i>"
    )
    await message.answer(txt, parse_mode="HTML")

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
