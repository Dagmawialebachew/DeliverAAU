from aiogram.fsm.state import StatesGroup, State

from app_context import db
from handlers.onboarding import main_menu

class HelpState(StatesGroup):
    waiting_for_message = State()
    
    


from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from config import settings
from aiogram.filters import Command
router = Router()

ADMIN_IDS = settings.ADMIN_IDS


class AdminReplyState(StatesGroup):
    waiting_for_reply = State()


# Step 1: Entry point — Need Help button
@router.message(Command("help"))
@router.message(F.text == "🧑‍🍳 Need Help")
async def need_help(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Send Message to Admin", callback_data="help:send")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="help:cancel")]
    ])
    await message.answer(
    "👋 Welcome to <b>UniBites Delivery Support</b> 🎓🍔\n\n"
    "UniBites is your campus-first food delivery service — "
    "connecting students, vendors & delivery partners with ease and soul.\n"
    "You can use our telegram support <a href='https://t.me/unibites_support'>@unibites_support</a> for direct assistance. or use our support number +251922643416\n\n"
    "✨ Need help or want to share feedback? Tap below to get support:",
    reply_markup=kb,
    parse_mode="HTML"
)



# Step 2: Handle inline button clicks
@router.callback_query(F.data == "help:send")
async def help_send(cb: CallbackQuery, state: FSMContext):
    cancel_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Back")]],
        resize_keyboard=True
    )
    await cb.message.answer("✍️ Please type your message for the admin:", reply_markup=cancel_kb)
    await state.set_state(HelpState.waiting_for_message)
    await cb.answer()

@router.callback_query(F.data == "help:cancel")
async def help_cancel(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("❌ Help request cancelled. Back to main menu.")
    await state.clear()
    await cb.answer()
@router.message(HelpState.waiting_for_message)
async def forward_help(message: Message, state: FSMContext):
    ticket_id = f"#HELP-{message.from_user.id}-{message.message_id}"

    # Save ticket to DB
    await db.save_ticket(
        ticket_id=ticket_id,
        user_id=message.from_user.id,
        text=message.text,
        status="open",
        original_msg_id=message.message_id
    )

    # Student feedback
    await message.answer("⌛ Processing your request…")
    await message.answer("📡 Sending to admins…")
    await message.answer(f"✅ Delivered! 🎟 Ticket ID: {ticket_id}")

       # Admin notification (unchanged)
    for admin_id in ADMIN_IDS:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Reply", callback_data=f"support_reply_start:{ticket_id}:{message.from_user.id}")],
            [InlineKeyboardButton(text="🔄 Need More Info", callback_data=f"support_reply:{message.from_user.id}:info"),
             InlineKeyboardButton(text="📞 Call Back", callback_data=f"support_reply:{message.from_user.id}:callback")],
            [InlineKeyboardButton(text="✅ Close Ticket", callback_data=f"support_close:{ticket_id}")]
        ])

        await message.bot.send_message(
            admin_id,
            f"📩 New Help Request {ticket_id}\n"
            f"👤 {message.from_user.full_name} (@{message.from_user.username or 'no_username'})\n"
            f"User ID: {message.from_user.id}\n\n"
            f"💬 Message:\n{message.text}",
            reply_markup=kb
        )

    await state.clear()


@router.message(Command("support"), F.chat.id.in_(ADMIN_IDS))
async def support_dashboard(message: Message):
    rows = await db.list_open_tickets()

    if not rows:
        await message.answer("📊 No open support tickets.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{row['ticket_id']} — {row['status']}", callback_data=f"support_manage:{row['ticket_id']}")]
        for row in rows
    ])
    await message.answer("📊 Support Dashboard\nSelect a ticket to manage:", reply_markup=kb)


@router.callback_query(F.data.startswith("support_manage:"))
async def manage_ticket(cb: CallbackQuery):
    ticket_id = cb.data.split(":", 1)[1]
    ticket = await db.get_ticket(ticket_id)

    if not ticket:
        await cb.answer("Ticket not found.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Close", callback_data=f"support_close:{ticket_id}")],
        [InlineKeyboardButton(text="🔄 Request More Info", callback_data=f"support_reply:{ticket['user_id']}:info")]
    ])
    await cb.message.answer(
        f"🎟 Ticket {ticket['ticket_id']}\n"
        f"👤 User ID: {ticket['user_id']}\n"
        f"💬 Message: {ticket['text']}\n"
        f"📌 Status: {ticket['status']}",
        reply_markup=kb
    )


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



@router.callback_query(F.data.startswith("support_reply_start:"))
async def start_admin_reply(cb: CallbackQuery, state: FSMContext):
    _, ticket_id, user_id = cb.data.split(":", 2)
    await state.update_data(ticket_id=ticket_id, user_id=int(user_id))
    await cb.message.answer(f"✍️ Please type your reply for ticket {ticket_id}.")
    await state.set_state(AdminReplyState.waiting_for_reply)
    await cb.answer()

@router.message(AdminReplyState.waiting_for_reply, F.chat.id.in_(ADMIN_IDS))
async def send_admin_reply(message: Message, state: FSMContext):
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    ticket = await db.get_ticket(ticket_id)

    if not ticket:
        await message.answer("⚠️ Ticket not found.")
        await state.clear()
        return

    await message.bot.send_message(
        ticket["user_id"],
        f"👨‍🍳 Admin reply to {ticket_id}:\n\n{message.text}",
        reply_to_message_id=ticket["original_msg_id"]
    )
    await message.answer(f"✅ Reply sent to user {ticket['user_id']} for ticket {ticket_id}.")
    await state.clear()
    


@router.callback_query(F.data.startswith("support_reply:"))
async def handle_shortcut_reply(cb: CallbackQuery):
    _, user_id, action = cb.data.split(":")
    responses = {
        "info": "🔄 Could you please provide more details so we can assist better?",
        "callback": "📞 Our team will reach out to you directly for further support."
    }
    await cb.bot.send_message(int(user_id), responses[action])
    await cb.answer("Shortcut reply sent!")
