from aiogram.fsm.state import StatesGroup, State

from database import db
from handlers.onboarding import build_profile_card, main_menu

class SettingsState(StatesGroup):
    editing_name = State()
    editing_campus = State()
    editing_phone = State()



from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext

router = Router()

# Step 1: Show profile card + options
@router.message(F.text == "⚙️ Settings")
async def settings(message: Message, state: FSMContext):
    user = await db.get_user(message.from_user.id)
    card = build_profile_card(user, role="student")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Change Name", callback_data="settings:name"),
         InlineKeyboardButton(text="🏛 Change Campus", callback_data="settings:campus")],
        [InlineKeyboardButton(text="📱 Edit Phone Number", callback_data="settings:phone")]
    ])

    await message.answer(card, reply_markup=kb)
    await state.clear()  # reset any previous state

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
        await cb.message.answer("🏛 Please type your new campus:", reply_markup=cancel_kb)
        await state.set_state(SettingsState.editing_campus)
    elif option == "phone":
        await cb.message.answer("📱 Please type your new phone number (must start with 09):", reply_markup=cancel_kb)
        await state.set_state(SettingsState.editing_phone)

    await cb.answer()

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


@router.message(SettingsState.editing_campus)
async def save_campus(message: Message, state: FSMContext):
    new_campus = message.text.strip()
    user_id = message.from_user.id
    await db.update_user_field(user_id, "campus", new_campus)

    user = await db.get_user(user_id)
    card = build_profile_card(user, role="student")

    await message.answer(f"✅ Campus updated to {new_campus}.\n\n{card}")
    await state.clear()

# Step 5: Handle Cancel / Main Menu gracefully
@router.message(F.text.in_(["❌ Cancel", "🏠 Main Menu"]))
async def cancel_or_main(message: Message, state: FSMContext):
    if message.text == "❌ Cancel":
        await message.answer("❌ Update cancelled. Back to settings menu.")
    else:
        await message.answer("🏠 Returning to main menu...", reply_markup=main_menu())
    await state.clear()
