from aiogram.fsm.state import StatesGroup, State

from handlers.onboarding import main_menu

class HelpState(StatesGroup):
    waiting_for_message = State()
    
    


from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from config import settings

router = Router()

ADMIN_IDS = settings.ADMIN_IDS

# Step 1: Entry point — Need Help button
@router.message(F.text == "🧑‍🍳 Need Help")
async def need_help(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Send Message to Admin", callback_data="help:send")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="help:cancel")]
    ])
    await message.answer(
        "👋 Welcome to Deliver AAU Support!\n\n"
        "Deliver AAU is your campus-first meal delivery service — "
        "connecting students, vendors, and delivery partners with soul.\n\n"
        "✨ If you’re facing an issue or want to give feedback, tap below:",
        reply_markup=kb
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

# Step 3: Capture user’s help message only in FSM state
@router.message(HelpState.waiting_for_message)
async def forward_help(message: Message, state: FSMContext):
    if message.text in ["⬅️ Back", "🏠 Main Menu"]:
        # Graceful exit
        if message.text == "❌ Cancel":
            await message.answer("❌ Help request cancelled. Back to main menu.")
        else:
            await message.answer("🏠 Returning to main menu...", reply_markup=main_menu())
        await state.clear()
        return

    try:
        for admin_id in ADMIN_IDS:
            await message.bot.send_message(
                admin_id,
                f"📩 Help request from {message.from_user.full_name} (@{message.from_user.username or 'no_username'})\n"
                f"User ID: {message.from_user.id}\n\n"
                f"Message:\n{message.text}"
            )
        await message.answer("✅ Your message has been sent to Deliver AAU support. Our team will reply soon!")
    except Exception:
        await message.answer("⚠️ Could not send your request. Please try again later.")
    finally:
        await state.clear()

# Step 4: Admin replies back to user
@router.message(F.chat.id.in_(ADMIN_IDS))
async def admin_reply(message: Message):
    if message.text.startswith("/reply"):
        parts = message.text.split(maxsplit=2)
        if len(parts) >= 3:
            user_id = int(parts[1])
            reply_text = parts[2]
            try:
                await message.bot.send_message(user_id, f"👨‍🍳 Admin reply:\n\n{reply_text}")
                await message.answer("✅ Reply sent to user.")
            except Exception:
                await message.answer("⚠️ Could not deliver reply.")
