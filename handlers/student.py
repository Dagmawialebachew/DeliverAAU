# handlers/student.py
import asyncio
import contextlib
import json
import logging
from typing import List, Dict, Any, Tuple, Optional

from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters import StateFilter
import aiosqlite
from utils.helpers import eta_and_distance, typing_pause

from config import settings
from database.db import Database
from utils.helpers import haversine
from utils.helpers import assign_delivery_guy
from handlers.onboarding import main_menu

router = Router()
db = Database(settings.DB_PATH)


# --- States ---
class OrderStates(StatesGroup):
    choose_place = State()
    menu = State()
    live_choice = State()
    dropoff_choice = State()
    dropoff_other = State()
    notes = State()
    confirm = State()


# --- Main menu placeholder ---

# --- Keyboards ---
def places_keyboard(vendors: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    buf: List[InlineKeyboardButton] = []
    for v in vendors:
        buf.append(InlineKeyboardButton(text=v["name"], callback_data=f"place:{v['id']}"))
        if len(buf) == 2:
            rows.append(buf); buf = []
    if buf:
        rows.append(buf)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def menu_keyboard(items: List[Dict[str, Any]], selected_ids: List[int], page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    buf: List[InlineKeyboardButton] = []
    for it in items:
        label = f"{'✅ ' if it['id'] in selected_ids else ''}{it['name']} — {it['price']} birr"
        buf.append(InlineKeyboardButton(text=label, callback_data=f"cart:toggle:{it['id']}"))
        if len(buf) == 2:
            rows.append(buf); buf = []
    if buf: rows.append(buf)

    rows.append([
        InlineKeyboardButton(text="🛒 View Cart", callback_data="cart:view"),
        InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel"),
    ])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"menu:page:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"Page {page}/{total_pages}", callback_data="menu:noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"menu:page:{page+1}"))
    rows.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=rows)


def cart_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm Order", callback_data="cart:confirm"),
         InlineKeyboardButton(text="➕ Add More", callback_data="cart:addmore")],
        [InlineKeyboardButton(text="🗑 Clear Cart", callback_data="cart:clear"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel")],
    ])


def dropoff_keyboard(campus: str) -> InlineKeyboardMarkup:
    presets_map = {
        "4kilo": ["Dorm", "Library", "Main Gate", "Engineering Block"],
        "5kilo": ["Dorm", "Library", "Main Gate", "Addis Hall"],
        "6kilo": ["Dorm", "Library", "Main Gate", "Science Block"],
    }
    presets = presets_map.get(campus, ["Dorm", "Library", "Main Gate"])
    rows: List[List[InlineKeyboardButton]] = []
    buf: List[InlineKeyboardButton] = []
    for p in presets:
        buf.append(InlineKeyboardButton(text=p, callback_data=f"drop:{p}"))
        if len(buf) == 2:
            rows.append(buf); buf = []
    if buf: rows.append(buf)
    rows.append([InlineKeyboardButton(text="✍️ Other", callback_data="drop:other")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def live_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Share Live Location (Telegram)", callback_data="live:request")],
        [InlineKeyboardButton(text="🏛 Use Preset Spot", callback_data="live:preset")],
        [InlineKeyboardButton(text="✍️ Other (type)", callback_data="live:other")],
    ])


def notes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Notes", callback_data="notes:add"),
         InlineKeyboardButton(text="⏭ Skip", callback_data="notes:skip")],
    ])


def final_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm", callback_data="final:confirm"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel")],
    ])


# --- Helpers ---
def paginate_menu(menu: List[Dict[str, Any]], page: int, page_size: int = 6) -> Tuple[List[Dict[str, Any]], int, int]:
    total = len(menu)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    return menu[start:start + page_size], page, total_pages


def render_cart(cart_items: List[Dict[str, Any]]) -> Tuple[str, float]:
    subtotal = sum(it["price"] for it in cart_items)
    counts = {}
    for it in cart_items:
        key = (it["id"], it["name"], it["price"])
        counts[key] = counts.get(key, 0) + 1
    lines = ["🛒 Your Cart"]
    for (iid, name, price), qty in counts.items():
        lines.append(f"{name} x{qty} — {price * qty} birr")
    lines.append("-----------------")
    lines.append(f"💵 Subtotal: {subtotal} birr")
    return "\n".join(lines), subtotal


# --- Flow handlers ---
@router.message(F.text == "🛒 Order")
async def start_order(message: Message, state: FSMContext):
    user = await db.get_user(message.from_user.id)
    if not user:
        await message.answer("Please complete onboarding first with /start.")
        return

    vendors = await db.list_vendors()
    if not vendors:
        await message.answer("No spots are available yet. Please try again later.")
        return

    sent = await message.answer("Choose your spot ↓", reply_markup=places_keyboard(vendors))
    await state.set_state(OrderStates.choose_place)
    await state.update_data(selected_ids=[], vendor=None, menu=None, menu_page=1, pivot_msg_id=sent.message_id)


@router.callback_query(OrderStates.choose_place, F.data.startswith("place:"))
async def choose_place(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    place_id = int(cb.data.split(":")[1])
    vendor = await db.get_vendor(place_id)
    if not vendor:
        await cb.message.edit_text("Spot not found. Please pick another.")
        return

    menu = json.loads(vendor.get("menu_json") or "[]")
    if not menu:
        await cb.message.edit_text("This spot has no menu items right now. Please pick another.")
        return

    short_menu, page, total_pages = paginate_menu(menu, 1)
    await state.update_data(vendor=vendor, menu=menu, menu_page=1, selected_ids=[])
    await cb.message.edit_text(f"{vendor['name']} — browse and tap to select items:", reply_markup=menu_keyboard(short_menu, [], page, total_pages))
    await state.set_state(OrderStates.menu)


@router.callback_query(OrderStates.menu, F.data.startswith("menu:page:"))
async def menu_paginate(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    selected = data.get("selected_ids", [])
    page = int(cb.data.split(":")[2])
    items, page, total_pages = paginate_menu(menu, page)
    await state.update_data(menu_page=page)
    await cb.message.edit_reply_markup(reply_markup=menu_keyboard(items, selected, page, total_pages))


@router.callback_query(OrderStates.menu, F.data.startswith("cart:toggle:"))
async def cart_toggle_item(cb: CallbackQuery, state: FSMContext):
    item_id = int(cb.data.split(":")[2])
    data = await state.get_data()
    menu = data.get("menu", []) or []
    selected: List[int] = data.get("selected_ids", [])
    page = data.get("menu_page", 1)

    if item_id in selected:
        selected.remove(item_id); await cb.answer("Removed from cart ❎")
    else:
        selected.append(item_id); await cb.answer("Added to cart ✅")

    await state.update_data(selected_ids=selected)
    items, page, total_pages = paginate_menu(menu, page)
    await cb.message.edit_reply_markup(reply_markup=menu_keyboard(items, selected, page, total_pages))


@router.callback_query(OrderStates.menu, F.data == "cart:view")
async def cart_view(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    selected: List[int] = data.get("selected_ids", [])
    if not selected:
        await cb.answer("Your cart is empty.", show_alert=False); return

    cart_items = [next(m for m in menu if m["id"] == sid) for sid in selected]
    text, subtotal = render_cart(cart_items)
    await state.update_data(cart=cart_items, food_subtotal=subtotal)
    await cb.message.edit_text(text, reply_markup=cart_keyboard())


@router.callback_query(OrderStates.menu, F.data == "cart:addmore")
async def cart_add_more(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    selected = data.get("selected_ids", [])
    page = data.get("menu_page", 1)
    items, page, total_pages = paginate_menu(menu, page)
    await cb.message.edit_text(f"{data['vendor']['name']} — browse and tap to select items:", reply_markup=menu_keyboard(items, selected, page, total_pages))


@router.callback_query(OrderStates.menu, F.data == "cart:clear")
async def cart_clear(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Cart cleared")
    data = await state.get_data()
    menu = data.get("menu", []) or []
    await state.update_data(selected_ids=[], cart=[])
    items, page, total_pages = paginate_menu(menu, data.get("menu_page", 1))
    await cb.message.edit_text(f"{data['vendor']['name']} — browse and tap to select items:", reply_markup=menu_keyboard(items, [], page, total_pages))


@router.callback_query(OrderStates.menu, F.data == "order:cancel")
@router.callback_query(OrderStates.confirm, F.data == "order:cancel")
async def order_cancel(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    try: await cb.message.edit_reply_markup(reply_markup=None)
    except: pass
    await state.clear()
    await cb.message.answer("Order cancelled. Start again from the main menu.", reply_markup=main_menu())


@router.callback_query(OrderStates.menu, F.data == "cart:confirm")
async def cart_confirm(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    
    # --- 🌟 FIX: Explicitly build and save cart here ---
    menu = data.get("menu") or []
    selected = data.get("selected_ids", [])
    cart_items = [next(m for m in menu if m["id"] == sid) for sid in selected]
    
    if not cart_items:
        await cb.answer("Your cart is empty. Please select items first.", show_alert=True); return
    
    # Save the full cart items list and subtotal
    subtotal = sum(it["price"] for it in cart_items)
    await state.update_data(cart=cart_items, food_subtotal=subtotal)
    
    # --- End Fix ---
    
    # Ask live vs preset
    await cb.message.edit_text(
        "📍 Share your live location for faster delivery, or choose a preset spot.", 
        reply_markup=live_choice_keyboard()
    )
    await state.set_state(OrderStates.live_choice)


# --- Live Location / Drop-off Flow ---


from aiogram.types import ReplyKeyboardRemove # Ensure this is imported at the top of your file

# --- Consolidated Live Location Request (No Change Needed, it's already clean) ---


@router.callback_query(OrderStates.live_choice, F.data == "live:request")
async def live_request(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    
    # 1. Prepare the ReplyKeyboardMarkup for the location button
    reply_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📡 Tap to Share Live Location", request_location=True)]],
        resize_keyboard=True, one_time_keyboard=True, selective=True
    )
    
    # 2. Prepare the InlineKeyboardMarkup (Alternatives)
    inline_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏛 Preset Spot", callback_data="live:preset")],
        [InlineKeyboardButton(text="✍️ Type Other", callback_data="live:other")],
        [InlineKeyboardButton(text="❌ Cancel & Hide Button", callback_data="live:cancel")]
    ])
    
    # 3. Send the instruction WITH the Reply Keyboard
    location_message = await cb.message.answer( # <<< CAPTURE THE MESSAGE OBJECT
        "**Step 1/2: Share Location**\n"
        "Please tap the **green button** in your chat keyboard below to share your live location. "
        "This is the fastest method for delivery.",
        reply_markup=reply_kb,
        parse_mode="Markdown"
    )
    
    # 4. Edit the original message (where 'live:request' was clicked) to show alternatives.
    await cb.message.edit_text(
        "**Location Options:**\n"
        "Look for the **📡 Tap to Share Live Location** button in your message bar.\n\n"
        "Or, switch to a manual method here:",
        reply_markup=inline_kb,
        parse_mode="Markdown"
    )

    # 5. Store the message ID of the location prompt
    await state.update_data(location_prompt_id=location_message.message_id) 
    await state.set_state(OrderStates.live_choice)
    
    
    # Helper to delete the intrusive message and clean the state data
async def _cleanup_location_prompt(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    location_prompt_id = data.get("location_prompt_id")
    
    if location_prompt_id:
        try:
            # Delete the specific message that contained the Reply Keyboard
            await cb.bot.delete_message(chat_id=cb.message.chat.id, message_id=location_prompt_id)
        except Exception:
            # Handle case where message is too old or already deleted
            pass
        finally:
            # Clear the stored ID
            await state.update_data(location_prompt_id=None)


# --- Consolidated Location Flow Cancellation (FIXED: Deletes Location Prompt) ---
@router.callback_query(OrderStates.live_choice, F.data == "live:cancel")
@router.callback_query(StateFilter(OrderStates.dropoff_choice), F.data == "drop:cancel")
@router.callback_query(StateFilter(OrderStates.dropoff_other), F.data == "drop:cancel")
async def location_flow_cancel_and_return(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Location selection cancelled.")
    
    # 1. DELETE the message that showed the location button
    await _cleanup_location_prompt(cb, state)
    
    # 2. Re-present the primary drop-off options
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Share Live Location", callback_data="live:request")],
        [InlineKeyboardButton(text="🏛 Preset Spot", callback_data="live:preset")],
        [InlineKeyboardButton(text="✍️ Type Other", callback_data="live:other")]
    ])
    
    # 3. Edit the current alternatives message to reset the menu
    await cb.message.edit_text("🔄 Choose your drop-off method again:", reply_markup=kb)
    await state.set_state(OrderStates.live_choice)

# --- Preset Spot Handler (FIXED: Deletes Location Prompt) ---
@router.callback_query(OrderStates.live_choice, F.data == "live:preset")
async def live_preset(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Switched to Preset input.")
    user = await db.get_user(cb.from_user.id)
    
    # DELETE the message that showed the location button
    await _cleanup_location_prompt(cb, state)
    
    kb = dropoff_keyboard(user["campus"])
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Go Back (Drop-off Method)", callback_data="drop:cancel")])
    
    await cb.message.edit_text(
        "🏛 **Choose a preset spot for drop-off**:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.dropoff_choice)

# --- Other (Text Input) Handler (FIXED: Deletes Location Prompt) ---
@router.callback_query(OrderStates.live_choice, F.data == "live:other")
async def live_other(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Switched to Text input.")
    
    # DELETE the message that showed the location button
    await _cleanup_location_prompt(cb, state)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Go Back (Drop-off Method)", callback_data="drop:cancel")]
    ])
    await cb.message.edit_text(
        "✍️ **Please type your exact drop-off location** (e.g., Engineering Building, Room 203).\n\n"
        "Tap the back button if you change your mind.",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.dropoff_other)

# --- Location Received Handler (New Logic: Deletes Location Prompt on success) ---
@router.message(F.content_type == "location", StateFilter(OrderStates.live_choice))
async def handle_shared_location(message: Message, state: FSMContext):
    loc = message.location
    if not loc:
        # If location failed for some reason, the prompt stays visible for retry
        return
        
    # 1. DELETE the message that prompted the location share after successful receipt
    data = await state.get_data()
    location_prompt_id = data.get("location_prompt_id")
    if location_prompt_id:
         try:
             await message.bot.delete_message(chat_id=message.chat.id, message_id=location_prompt_id)
         except Exception:
             pass # Fails silently if already deleted/too old
         finally:
             await state.update_data(location_prompt_id=None)

    # 2. Process and move on
    lat, lon = loc.latitude, loc.longitude
    await state.update_data(
        dropoff=f"Live location ({lat:.6f},{lon:.6f})",
        live_coords={"lat": lat, "lon": lon}
    )
    
    # Since we deleted the prompt, the ReplyKeyboardRemove is no longer necessary, 
    # but using it here provides insurance against other persistent keyboards.
    await tracked_send(
    message,
    "✅ Live location saved.",
    state,
    key="location_saved",
    reply_markup=ReplyKeyboardRemove()
)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Change Location", callback_data="live:change")],
        [InlineKeyboardButton(text="📝 Add Notes", callback_data="notes:add")],
        [InlineKeyboardButton(text="➡️ Skip Notes", callback_data="notes:skip")]
    ])
    await tracked_send(
    message,
    "Next step:",
    state,
    key="next_step",
    reply_markup=kb
)
    await state.set_state(OrderStates.notes)
    
# --- Change Location Handler (No Change Needed) ---
@router.callback_query(OrderStates.notes, F.data == "live:change")
async def live_change(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Share live location", callback_data="live:request")],
        [InlineKeyboardButton(text="🏛 Preset Spot", callback_data="live:preset")],
        [InlineKeyboardButton(text="✍️ Type Other", callback_data="live:other")]
    ])
    await cb.message.edit_text("🔄 Update your drop‑off location:", reply_markup=kb)
    await state.set_state(OrderStates.live_choice)

# --- Preset Spot Handler (FIXED: Silent Reply KB Removal) ---
@router.callback_query(OrderStates.live_choice, F.data == "live:preset")
async def live_preset(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Switched to Preset input.")
    user = await db.get_user(cb.from_user.id)
    
    # Remove Reply Keyboard silently
    await cb.message.answer(text=".", reply_markup=ReplyKeyboardRemove(), disable_notification=True)
    
    kb = dropoff_keyboard(user["campus"])
    # Add a dedicated back button for better navigation
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Go Back (Drop-off Method)", callback_data="drop:cancel")])
    
    await cb.message.edit_text(
        "🏛 **Choose a preset spot for drop-off**:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.dropoff_choice)

# --- Other (Text Input) Handler (FIXED: Silent Reply KB Removal) ---
@router.callback_query(OrderStates.live_choice, F.data == "live:other")
async def live_other(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Switched to Text input.")
    
    # Remove Reply Keyboard silently
    await cb.message.answer(text=".", reply_markup=ReplyKeyboardRemove(), disable_notification=True)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Go Back (Drop-off Method)", callback_data="drop:cancel")]
    ])
    await cb.message.edit_text(
        "✍️ **Please type your exact drop-off location** (e.g., Engineering Building, Room 203).\n\n"
        "Tap the back button if you change your mind.",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.dropoff_other)


# --- Dropoff Choose Handler (No change needed) ---
@router.callback_query(OrderStates.dropoff_choice, F.data.startswith("drop:"))
async def dropoff_choose(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    where = cb.data.split(":", 1)[1]
    await state.update_data(dropoff=where)
    await cb.message.edit_text(
        f"📍 Drop‑off set to: {where}\nWould you like to add delivery notes?",
        reply_markup=notes_keyboard()
    )
    await state.set_state(OrderStates.notes)

# --- Dropoff Other Text Input Handler (No change needed) ---
@router.message(OrderStates.dropoff_other)
async def dropoff_other_text(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 4:
        await message.answer(
            "⚠️ Could you add a bit more detail?\nFor example: 'Main Library, 2nd floor, near info desk'."
        )
        return
    await state.update_data(dropoff=text)
    await tracked_send(
    message,
    f"📍 Drop-off set to: {text}\nWould you like to add delivery notes?",
    state,
    key="dropoff_set",
    reply_markup=notes_keyboard()
)
    await state.set_state(OrderStates.notes)

# --- Notes Flow (No change needed) ---
# (notes_keyboard definition needed above if not provided)

@router.callback_query(OrderStates.notes, F.data == "notes:add")
async def notes_add(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.edit_text("✍️ Please type your delivery notes (e.g., 'Call when outside'):")
    await state.set_state(OrderStates.notes)


@router.callback_query(OrderStates.notes, F.data == "notes:edit")
async def notes_edit(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.edit_text("✍️ Please type your new delivery notes:")
    await state.set_state(OrderStates.notes)


@router.callback_query(OrderStates.notes, F.data == "notes:skip")
async def notes_skip(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.update_data(notes="") 
    await ask_final_confirmation(cb.message, state)


@router.message(OrderStates.notes)
async def capture_notes(message: Message, state: FSMContext):
    notes = (message.text or "").strip()
    await state.update_data(notes=notes)
    await ask_final_confirmation(message, state)
    
    


# Ensure any previous location messages are deleted before showing final confirmation
from utils.message_tracker import cleanup_tracked_messages, tracked_send

async def _prepare_final_preview(cb_or_message, state):
    """Deletes all tracked messages (clean UX before final summary)."""
    try:
        bot = cb_or_message.bot if hasattr(cb_or_message, "bot") else cb_or_message.message.bot
        chat_id = (
            cb_or_message.message.chat.id
            if hasattr(cb_or_message, "message")
            else cb_or_message.chat.id
        )
        await cleanup_tracked_messages(bot, chat_id, state)
    except Exception as e:
        print(f"[WARN] Cleanup failed before preview: {e}")





# 🎯 Step 1: Ask Final Confirmation (with dynamic typing effect + anchored main menu)
@router.message(F.text == "🔒 Preview & Confirm")  # optional trigger; you can call directly in your flow
async def ask_final_confirmation_entry(message: Message, state: FSMContext):
    await ask_final_confirmation(message, state)


async def ask_final_confirmation(message: Message, state: FSMContext):
    data = await state.get_data()

    # Ensure cart exists
    cart = data.get("cart") or []
    if not cart:
        menu = data.get("menu", [])
        selected = data.get("selected_ids", [])
        cart = [next((m for m in menu if m["id"] == sid), None) for sid in selected]
        cart = [item for item in cart if item]
        await state.update_data(cart=cart)

    dropoff = data.get("dropoff", "")
    notes = data.get("notes", "")

    if not cart or not dropoff:
        await message.answer(
            "⚠️ Something went wrong — your cart or drop-off is missing.\nPlease restart your order 🛒.",
            reply_markup=main_menu()
        )
        await state.clear()
        return

    # Render cart summary
    text, subtotal = render_cart(cart)
    delivery_fee = 20.0
    total = subtotal + delivery_fee

    summary = (
        f"✨ *Final Preview*\n"
        f"──────────────────────\n"
        f"{text}\n"
        f"🚚 _Delivery fee:_ *{delivery_fee:.2f} birr*\n"
        f"──────────────────────\n"
        f"💵 *Total Payable:* *{total:.2f} birr*\n\n"
        f"📍 _Drop-off:_ *{dropoff}*\n"
        f"{('📝 _Notes:_ ' + notes) if notes else ''}\n"
        "\n✅ *Everything looks perfect?*\n"
        "_Tap Confirm to place your order._"
    )

    # Dynamic typing animation
    status_msg = await message.answer("⏳ Preparing your order summary")
    for i in range(1, 4):
        await asyncio.sleep(0.5)
        await status_msg.edit_text("⏳ Preparing your order summary" + "." * i)

    await asyncio.sleep(0.5)
    await status_msg.edit_text("✨ Final Preview ready!")
    await asyncio.sleep(0.7)

    # Show summary with inline confirm/cancel
    await message.answer(summary, reply_markup=final_confirm_keyboard(), parse_mode="Markdown")

    # Also show main menu reply keyboard so user feels anchored
    await message.answer("📋 Use the menu below while you decide:", reply_markup=main_menu())

    # Clean up the typing message
    try:
        await status_msg.delete()
    except Exception:
        pass

    # Update state
    await state.update_data(food_subtotal=subtotal, delivery_fee=delivery_fee)
    await state.set_state(OrderStates.confirm)
# 🎯 Step 2: Handle Final Confirmation (removes inline buttons, shows main menu during placement)
@router.callback_query(OrderStates.confirm, F.data == "final:confirm")
async def final_confirm(cb: CallbackQuery, state: FSMContext):
    await cb.answer("🕓 Placing your order...")

    # Remove inline buttons to prevent double confirmation or mid-cancel
    with contextlib.suppress(Exception):
        await cb.message.edit_reply_markup(reply_markup=None)

    # Show main menu keyboard while the order is being processed
    processing_msg = await cb.message.answer("🛠 Processing your order...", reply_markup=main_menu())

    # Fetch user info
    user = await db.get_user(cb.from_user.id)
    if not user:
        await processing_msg.edit_text(
            "⚠️ Could not find your account. Please /start to register.",
            reply_markup=main_menu()
        )
        return

    # Get all order-related data from FSM
    data = await state.get_data()
    cart_items = data.get("cart", [])
    live_coords = data.get("live_coords")  # {"lat": ..., "lon": ...}

    # Prepare order breakdown
    breakdown = {
        "items": [item["name"] for item in cart_items],
        "subtotal": float(data.get("food_subtotal", 0.0)),
        "delivery_fee": float(data.get("delivery_fee", 0.0)),
        "notes": data.get("notes", ""),
        "live_shared": bool(live_coords),
        "drop_lat": float(live_coords["lat"]) if live_coords and live_coords.get("lat") else None,
        "drop_lon": float(live_coords["lon"]) if live_coords and live_coords.get("lon") else None,
    }
    breakdown_json = json.dumps(breakdown)
    items_json = json.dumps(cart_items)

    # Vendor details
    vendor = data.get("vendor") or {}
    vendor_id = vendor.get("id")
    vendor_name = vendor.get("name", "Unknown")

    total_payable = (
        float(data.get("food_subtotal", 0.0))
        + float(data.get("delivery_fee", 0.0))
    )

    # Create order entry in DB
    order_id = await db.create_order(
        user_id=user["id"],
        delivery_guy_id=None,  # unassigned initially
        vendor_id=vendor_id,
        pickup=vendor_name,
        dropoff=data.get("dropoff", ""),
        items_json=items_json,
        food_subtotal=float(data.get("food_subtotal", 0.0)),
        delivery_fee=float(data.get("delivery_fee", 0.0)),
        status="pending",
        payment_method="cod",
        payment_status="unpaid",
        receipt_id=0,
        breakdown_json=breakdown_json,
    )
    
    vendor_chat_id = vendor.get("telegram_id")
    if vendor_chat_id:
        items = ", ".join([i["name"] for i in cart_items])
        vendor_text = (
            f"📦 አዲስ ትዕዛዝ #{order_id}\n"
            f"🛒 እቃዎች: {items}\n"
            f"💵 ዋጋ: {int(data.get('food_subtotal',0))} ብር\n"
            f"📍 መድረሻ: {data.get('dropoff','')}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ ተቀበል", callback_data=f"vendor:accept:{order_id}")],
                [InlineKeyboardButton(text="❌ አልተቀበለም", callback_data=f"vendor:reject:{order_id}")]
            ]
        )
        await cb.bot.send_message(vendor_chat_id, vendor_text, reply_markup=kb)

    # 🎬 Cinematic progress sequence
    progress_steps = [
    "🍳 Coordinating with kitchen...",
    "🚴 Searching for a nearby delivery guy...",
    "⚡ Optimizing fastest route..."
]

    cinematic_msg = processing_msg
    for step in progress_steps:
        await asyncio.sleep(1.3)
        try:
            await cinematic_msg.edit_text(step)
        except Exception as e:
            logging.warning(f"Failed to edit cinematic message: {e}")
            # fallback: send a new message so the user still sees the step
            try:
                cinematic_msg = await cb.message.answer(step)
            except Exception as inner_e:
                logging.error(f"Fallback also failed: {inner_e}")

    # Delete cinematic message after sequence
    try:
        await cinematic_msg.delete()
    except Exception as e:
        logging.warning(f"Failed to delete cinematic message: {e}")

    # Try assigning a delivery guy
    chosen = await assign_delivery_guy(db.db_path, order_id, bot=cb.bot)

    # Compute ETA and distance if a delivery guy was assigned
    eta_info: Optional[Dict[str, Any]] = None
    if chosen:
        vendor_coords = data.get("vendor_coords")
        drop_coords = live_coords or {"lat": None, "lon": None}

        if vendor_coords and drop_coords.get("lat") and drop_coords.get("lon"):
            eta_info = await eta_and_distance(
                vendor_coords["lat"], vendor_coords["lon"],
                drop_coords["lat"], drop_coords["lon"]
            )
            # Optionally record vendor's initial location as live position
            await db.update_order_live(
                order_id,
                live_shared=True,
                lat=vendor_coords["lat"],
                lon=vendor_coords["lon"]
            )

    # 🧾 Build order summary preview
    cart_text, _ = render_cart(cart_items)
    final_preview = (
        f"🎉 *Order #{order_id} Confirmed!*\n"
        "──────────────────────\n"
        f"{cart_text}\n"
        f"🚚 Delivery fee: *{data.get('delivery_fee', 20.0):.2f} birr*\n"
        f"💵 Total: *{total_payable:.2f} birr*\n\n"
        f"📍 Drop-off: *{data.get('dropoff', '')}*\n"
        f"{('📝 Notes: ' + data.get('notes', '')) if data.get('notes') else ''}\n"
)


    if chosen:
        final_preview += (
            f"\n👤 Delivery Guy: {chosen['name']} ({chosen['campus']})\n"
            f"🚶 Heading from: {vendor_name}\n"
            "🧭 Track your order anytime in *📍 Track Order*."
        )
        if eta_info:
            final_preview += (
                f"\n🕓 ETA: ~{eta_info['eta_min']} min "
                f"({eta_info['distance_km']:.2f} km) from pickup to drop-off."
            )
    else:
        final_preview += "\n⌛ No delivery guy available yet. Admin will assign one soon."

    # Clean up old messages safely
    with contextlib.suppress(Exception):
        await cb.message.delete()

    # Send final confirmation preview with main menu
    await cb.message.answer(final_preview, parse_mode="Markdown", reply_markup=main_menu())

    # 🎞️ Finishing animation + XP reward
    status_msg = await cb.message.answer("🎬 Wrapping things up...")
    await asyncio.sleep(1.5)

    try:
        await status_msg.edit_text(
            "🔥 +10 XP will be added after delivery!",
            parse_mode="Markdown",
            reply_markup=main_menu()  # attach main menu keyboard here
        )
    except Exception as e:
        # fallback: send a new message with the keyboard
        await cb.message.answer(
            "🔥 +10 XP will be added after delivery!",
            parse_mode="Markdown",
            reply_markup=main_menu()
        )
    # 📢 Notify admin about the new order
    if settings.ADMIN_GROUP_ID:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT COUNT(*) FROM delivery_guys WHERE active = 1") as cur:
                count = await cur.fetchone()
                available_count = count[0] if count else 0

        assigned_text = (
            f"Assigned Delivery Guy: {chosen['name']} ({chosen['campus']})"
            if chosen else "No delivery guy assigned yet"
        )
        reason = (
            "No active delivery guys available right now."
            if available_count == 0 else
            "No delivery guy matches campus or location."
        ) if not chosen else "Delivery guy successfully assigned"

        admin_msg = (
            f"📢 *New Order Placed: #{order_id}*\n"
            f"👤 Customer: {user['first_name']} ({user.get('phone', 'N/A')})\n"
            f"🏛 Campus: {user.get('campus', 'N/A')}\n"
            f"🍴 Vendor: {vendor_name}\n"
            f"📍 Drop-off: {data.get('dropoff', '')}\n"
            f"💵 Total: {total_payable:.2f} birr (COD)\n"
            f"⚡ {assigned_text}\n"
            f"ℹ️ Status: {reason}"
        )
        await cb.bot.send_message(settings.ADMIN_GROUP_ID, admin_msg, parse_mode="Markdown")

    # 🚨 Send explicit alert if no delivery guy assigned
    if not chosen and settings.ADMIN_GROUP_ID:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT COUNT(*) FROM delivery_guys WHERE active = 1") as cur:
                count = await cur.fetchone()
                available_count = count[0] if count else 0

        reason = (
            "No active delivery guys available right now."
            if available_count == 0 else
            "No delivery guy matches campus or location."
        )

        admin_msg = (
            f"⚠️ *No Delivery Guy Assigned for Order #{order_id}*\n"
            f"👤 Customer: {user['first_name']} ({user.get('phone', 'N/A')})\n"
            f"🏛 Campus: {user.get('campus', 'N/A')}\n"
            f"🍴 Vendor: {vendor_name}\n"
            f"📍 Drop-off: {data.get('dropoff', '')}\n"
            f"💵 Total: {total_payable:.2f} birr (COD)\n\n"
            f"Reason: {reason}\n"
            "Please assign a delivery guy manually."
        )
        await cb.bot.send_message(settings.ADMIN_GROUP_ID, admin_msg, parse_mode="Markdown")

    # ✅ Clear FSM state after completion
    await state.clear()


# Optional: handle cancel from confirmation gracefully (if you keep a cancel button)
@router.callback_query(OrderStates.confirm, F.data == "final:cancel")
async def final_cancel(cb: CallbackQuery, state: FSMContext):
    await cb.answer("🛑 Order cancelled.")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await cb.message.answer("❌ Order not placed.\nYou can start a new one from the menu below.", reply_markup=main_menu())
    await state.clear()