# handlers/student.py
import asyncio
from collections import Counter
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
from utils.helpers import eta_and_distance, typing_pause

from config import settings
from database.db import Database
from utils.helpers import haversine
from utils.helpers import assign_delivery_guy
from handlers.onboarding import main_menu

router = Router()
from app_context import db

# --- States ---
class OrderStates(StatesGroup):
    choose_place = State()
    menu = State()
    live_choice = State()
    dropoff_choice = State()
    dropoff_other = State()
    campus_choice = State()   # <-- add this
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


def menu_keyboard(items: List[Dict[str, Any]], cart_counts: Dict[int, int], page: int, page_size: int = 8) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    page_items = items[start:start + page_size]

    rows: List[List[InlineKeyboardButton]] = []
    buf: List[InlineKeyboardButton] = []

    # Numeric buttons with quantity highlight
    for it in page_items:
        count = cart_counts.get(it["id"], 0)
        if count > 0:
            label = f"✅ {it['id']} (x{count})"
        else:
            label = str(it["id"])
        buf.append(InlineKeyboardButton(
            text=label,
            callback_data=f"cart:toggle:{it['id']}"
        ))
        if len(buf) == 4:
            rows.append(buf)
            buf = []
    if buf:
        rows.append(buf)

    # Cart + Cancel row
    rows.append([
        InlineKeyboardButton(text="🛒 View Cart", callback_data="cart:view"),
        InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel"),
    ])

    # Navigation row
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
        "4kilo": ["Dorm", "Library", "Main Gate"],
        "5kilo": ["Dorm", "Library", "Main Gate"],
        "6kilo": ["Dorm", "Kennedy", "Main Gate", "False Gate"],
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
        [InlineKeyboardButton(text="📡 Share Live Location", callback_data="live:request")],
        [
            InlineKeyboardButton(text="🏛 Preset Spot", callback_data="live:preset"),
            InlineKeyboardButton(text="🏫 Change Campus", callback_data="live:campus")
        ],
        [InlineKeyboardButton(text="✍️ Type Other", callback_data="live:other"), InlineKeyboardButton(text="❌ Cancel", callback_data="live:cancel")]
    ])


def notes_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Notes", callback_data="notes:add"),
         InlineKeyboardButton(text="⏭ Skip", callback_data="notes:skip")],
    ])


@router.callback_query(OrderStates.live_choice, F.data == "live:campus")
async def live_change_campus(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏛 4 Kilo", callback_data="campus:4kilo"), InlineKeyboardButton(text="📚 5 Kilo", callback_data="campus:5kilo"), InlineKeyboardButton(text="🎓 6 Kilo", callback_data="campus:6kilo")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="live:change")]
    ])
    await cb.message.edit_text("🏫 Choose a campus for this order:", reply_markup=kb)
    await state.set_state(OrderStates.campus_choice)


@router.callback_query(OrderStates.campus_choice, F.data.startswith("campus:"))
async def campus_selected(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    campus_code = cb.data.split(":")[1]
    await state.update_data(override_campus=campus_code)

    await cb.message.edit_text(
        f"✅ Campus temporarily set to {campus_code} for this order.\n"
        "Now choose a preset spot:",
        reply_markup=dropoff_keyboard(campus_code)
    )
    await state.set_state(OrderStates.dropoff_choice)


def final_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm", callback_data="final:confirm"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="order:cancel")],
    ])


# --- Helpers ---
def paginate_menu(menu: List[Dict[str, Any]], page: int, page_size: int = 8) -> Tuple[List[Dict[str, Any]], int, int]:
    total = len(menu)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    return menu[start:start + page_size], page, total_pages

def render_menu_text(menu: List[Dict[str, Any]], vendor_name: str) -> str:
    grouped = {}
    for item in menu:
        cat = item.get("category", "Other")
        grouped.setdefault(cat, []).append(item)

    lines = [
        f"🍴 *Today's Menu at {vendor_name}*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "✨ Pick your favorites below ✨"
    ]

    for cat, items in grouped.items():
        # Category header with emoji flair
        lines.append(f"\n🔹 *{cat}*")
        for it in items:
            lines.append(f"{it['id']}️⃣ {it['name']} — *{it['price']} birr*")

    # Closing line with call to action
    lines.append("\n🛒 *Tap the numbers below to add items to your cart!*")
    lines.append("───────────────────────────────")
    lines.append("💡 You can view your cart anytime.")

    return "\n".join(lines)


def render_cart(cart_counts: Dict[int,int], menu: List[Dict[str,Any]]) -> Tuple[str,float]:
    subtotal = 0
    lines = ["🛒 Your Cart"]
    for item_id, qty in cart_counts.items():
        item = next((m for m in menu if m["id"] == item_id), None)
        if not item:
            continue
        subtotal += item["price"] * qty
        lines.append(f"{item['name']} x{qty} — {item['price'] * qty} birr")
    lines.append("-----------------")
    lines.append(f"💵 Subtotal: {subtotal} birr")
    lines.append("──────────────────────")
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
        await cb.message.edit_text("⚠️ Spot not found. Please pick another.")
        return

    menu = json.loads(vendor.get("menu_json") or "[]")
    if not menu:
        await cb.message.edit_text("📭 This spot has no menu items right now. Please pick another.")
        return

    # Save vendor + menu into FSM state, initialize empty cart_counts
    await state.update_data(vendor=vendor, menu=menu, menu_page=1, cart_counts={})

    # Render cinematic menu text
    text = render_menu_text(menu, vendor["name"])

    # Build numeric keyboard for page 1 with empty cart_counts
    kb = menu_keyboard(menu, {}, page=1)

    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    await state.set_state(OrderStates.menu)

@router.callback_query(OrderStates.menu, F.data.startswith("menu:page:"))
async def menu_paginate(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    vendor = data.get("vendor", {})
    page = int(cb.data.split(":")[2])

    # Update current page in state
    await state.update_data(menu_page=page)

    # Re-render cinematic menu text
    text = render_menu_text(menu, vendor.get("name", "Unknown Spot"))

    # Build numeric keyboard for this page
    kb = menu_keyboard(menu, page=page)

    # Update both text + buttons
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")


MAX_QTY = 2
MAX_CART_ITEMS = 5


@router.callback_query(OrderStates.menu, F.data.startswith("cart:toggle:"))
async def cart_toggle_item(cb: CallbackQuery, state: FSMContext):
    item_id = int(cb.data.split(":")[2])
    data = await state.get_data()
    cart_counts: Dict[int,int] = data.get("cart_counts", {})

    current_qty = cart_counts.get(item_id, 0)

    # Count total items in cart
    total_items = sum(cart_counts.values())

    if current_qty < MAX_QTY:
        if total_items >= MAX_CART_ITEMS:
            await cb.answer(f"⚠️ You can only select {MAX_CART_ITEMS} items total.", show_alert=True)
            return
        cart_counts[item_id] = current_qty + 1
        await cb.answer(f"✅ Quantity set to x{cart_counts[item_id]}")
    else:
        cart_counts.pop(item_id, None)
        await cb.answer("❎ Removed from cart")

    await state.update_data(cart_counts=cart_counts)

    menu = data.get("menu", [])
    page = data.get("menu_page", 1)
    kb = menu_keyboard(menu, cart_counts, page)
    text = render_menu_text(menu, data["vendor"]["name"])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")

@router.callback_query(OrderStates.menu, F.data == "cart:view")
async def cart_view(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    cart_counts: Dict[int,int] = data.get("cart_counts", {}) or {}

    # Clean out any zero‑quantity entries
    cart_counts = {iid: qty for iid, qty in cart_counts.items() if qty > 0}

    if not cart_counts:
        await cb.answer("Your cart is empty.", show_alert=True)
        return

    text, subtotal = render_cart(cart_counts, menu)
    await state.update_data(food_subtotal=subtotal, cart_counts=cart_counts)
    await cb.message.edit_text(text, reply_markup=cart_keyboard())

@router.callback_query(OrderStates.menu, F.data == "cart:addmore")
async def cart_add_more(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()
    menu = data.get("menu", []) or []
    cart_counts: Dict[int,int] = data.get("cart_counts", {})
    page = data.get("menu_page", 1)

    # Re-render full menu text
    text = render_menu_text(menu, data["vendor"]["name"])

    # Build keyboard for current page
    items, page, total_pages = paginate_menu(menu, page)
    kb = menu_keyboard(menu, cart_counts, page)

    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")

@router.callback_query(OrderStates.menu, F.data == "cart:clear")
async def cart_clear(cb: CallbackQuery, state: FSMContext):
    await cb.answer("Cart cleared")
    data = await state.get_data()
    menu = data.get("menu", []) or []
    await state.update_data(cart_counts={}, cart=[])

    page = data.get("menu_page", 1)

    # Re-render full menu text
    text = render_menu_text(menu, data["vendor"]["name"])

    # Build keyboard for current page
    kb = menu_keyboard(menu, {}, page)

    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")

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

    menu = data.get("menu") or []
    cart_counts: Dict[int,int] = data.get("cart_counts", {})

    if not cart_counts:
        await cb.answer("Your cart is empty. Please select items first.", show_alert=True)
        return

    # Build cart items list with quantities
    cart_items = []
    subtotal = 0
    for item_id, qty in cart_counts.items():
        item = next((m for m in menu if m["id"] == item_id), None)
        if not item:
            continue
        subtotal += item["price"] * qty
        cart_items.extend([item] * qty)

    await state.update_data(cart=cart_items, food_subtotal=subtotal)

    # Fetch the student’s campus from DB
    user = await db.get_user(cb.from_user.id)
    campus = user["campus"] if user else "your campus"

    # Tell them clearly which campus is active
    await cb.message.edit_text(
        f"📍 Your meal will be delivered to **{campus} campus** (your home base).\n\n"
        "• If you’re at this campus, choose a preset spot or share live location.\n"
        "• If you need this order delivered to another campus, tap **Change Campus (temporary)**.\n"
        "• To permanently update your home base, go to ⚙️ Settings later.\n\n"
        "Choose how you want to set your drop‑off:",
        reply_markup=live_choice_keyboard(),
        parse_mode="Markdown"
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
    data = await state.get_data()
    campus = data.get("override_campus")

    if not campus:
        user = await db.get_user(cb.from_user.id)
        campus = user["campus"]

    kb = dropoff_keyboard(campus)
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Back", callback_data="drop:cancel")])

    await cb.message.edit_text(
        f"🏛 **Choose a preset spot for {campus} campus:**",
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
        [InlineKeyboardButton(text="🔄 Change Location", callback_data="live:change"), InlineKeyboardButton(text="📝 Add Notes", callback_data="notes:add")],
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




@router.message(F.text == "🔒 Preview & Confirm")  # optional trigger; you can call directly in your flow
async def ask_final_confirmation_entry(message: Message, state: FSMContext):
    await ask_final_confirmation(message, state)


async def ask_final_confirmation(message: Message, state: FSMContext):
    data = await state.get_data()

    menu = data.get("menu", []) or []
    cart_counts: Dict[int,int] = data.get("cart_counts", {})
    dropoff = data.get("dropoff", "")
    notes = data.get("notes", "")

    if not cart_counts or not dropoff:
        await message.answer(
            "⚠️ Something went wrong — your cart or drop-off is missing.\nPlease restart your order 🛒.",
            reply_markup=main_menu()
        )
        await state.clear()
        return

    # Render cart summary from counts
    text, subtotal = render_cart(cart_counts, menu)
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


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

    # 🎯 Step 2: Handle Final Confirmation (removes inline buttons, shows main menu during placement)
@router.callback_query(OrderStates.confirm, F.data == "final:confirm")
async def final_confirm(cb: CallbackQuery, state: FSMContext):
    await cb.answer("🕓 Placing your order...")

    # Remove inline buttons to prevent double confirmation or mid-cancel
    with contextlib.suppress(Exception):
        await cb.message.edit_reply_markup(reply_markup=None)

    # Fetch user info
    user = await db.get_user(cb.from_user.id)
    if not user:
        await cb.message.answer(
            "⚠️ Could not find your account. Please /start to register.",
            reply_markup=main_menu()
        )
        return

    # Get all order-related data from FSM
    data = await state.get_data()
    menu = data.get("menu", []) or []
    cart_counts: Dict[int,int] = data.get("cart_counts", {})
    live_coords = data.get("live_coords")

    if not cart_counts:
        await cb.message.answer("⚠️ Your cart is empty. Please restart your order 🛒.", reply_markup=main_menu())
        await state.clear()
        return

    # ✅ Build cart_items fresh from cart_counts for consistency
    cart_items, subtotal = [], 0
    for item_id, qty in cart_counts.items():
        item = next((m for m in menu if m["id"] == item_id), None)
        if not item:
            continue
        subtotal += item["price"] * qty
        cart_items.extend([item] * qty)

    # Prepare order breakdown
    breakdown = {
        "items": [item["name"] for item in cart_items],
        "subtotal": subtotal,
        "delivery_fee": float(data.get("delivery_fee", 0.0)),
        "notes": data.get("notes", ""),
        "live_shared": bool(live_coords),
        "drop_lat": float(live_coords["lat"]) if live_coords and live_coords.get("lat") else None,
        "drop_lon": float(live_coords["lon"]) if live_coords and live_coords.get("lon") else None,
    }

    # Vendor details
    vendor = data.get("vendor") or {}
    vendor_id = vendor.get("id")
    vendor_name = vendor.get("name", "Unknown")

    total_payable = subtotal + float(data.get("delivery_fee", 0.0))

    # Create order entry in DB
    order_id = await db.create_order(
        user_id=user["id"],
        delivery_guy_id=None,  # not assigned yet
        vendor_id=vendor_id,
        pickup=vendor_name,
        dropoff=data.get("dropoff", ""),
        items_json=json.dumps(cart_items),
        food_subtotal=subtotal,
        delivery_fee=float(data.get("delivery_fee", 0.0)),
        status="pending",
        payment_method="cod",
        payment_status="unpaid",
        receipt_id=0,
        breakdown_json=json.dumps(breakdown),
    )

    # Notify vendor
    vendor_chat_id = vendor.get("telegram_id")
    if vendor_chat_id:
        counts = Counter([i.get("name", "") for i in cart_items if isinstance(i, dict)])
        items = "\n".join(
            f"• {name} x{count}" if count > 1 else f"• {name}"
            for name, count in counts.items()
        ) or "—"

        vendor_text = (
            f"📦 አዲስ ትዕዛዝ #{order_id}\n"
            f"🛒 ምግቦች:\n{items}\n\n"
            f"💵 ዋጋ: {int(subtotal)} ብር\n"
            f"📍 መድረሻ: {data.get('dropoff','')}\n\n"
            f"⚡ እባክዎት ትዕዛዙን ይቀበሉ ወይም ይከለክሉ።"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ ተቀበል", callback_data=f"vendor:accept:{order_id}"),
                    InlineKeyboardButton(text="❌ አይ", callback_data=f"vendor:reject:{order_id}")
                ]
            ]
        )
        try:
            await cb.bot.send_message(vendor_chat_id, vendor_text, reply_markup=kb)
        except Exception as e:
            # Log the error but don't block the rest of the flow
            log.warning(f"Failed to notify vendor {vendor_chat_id} for order {order_id}: {e}")
            # Optionally notify admin group
            from utils.db_helpers import notify_admin_log
            await notify_admin_log(cb.bot, settings.ADMIN_DAILY_GROUP_ID,
                f"⚠️ Could not notify vendor {vendor.get('name','Unknown')} (chat_id={vendor_chat_id}) "
                f"about Order #{order_id}. Error: {e}"
            )

    # 🎬 Cinematic progress sequence
    cinematic_msg = await cb.message.answer("🍳 Coordinating with kitchen...")
    await asyncio.sleep(1.3)
    await cinematic_msg.edit_text("🚴 Waiting for vendor acceptance...")

    # 🧾 Build order summary preview for student
    cart_text, subtotal = render_cart(cart_counts, menu)
    final_preview = (
        f"🎉 *Order #{order_id} Confirmed!*\n"
        "──────────────────────\n"
        f"{cart_text}\n"
        f"🚚 Delivery fee: *{data.get('delivery_fee', 20.0):.2f} birr*\n"
        f"💵 Total: *{total_payable:.2f} birr*\n\n"
        f"📍 Drop-off: *{data.get('dropoff', '')}*\n"
        f"{('📝 Notes: ' + data.get('notes', '')) if data.get('notes') else ''}\n\n"
        "✅ Your order has been sent to the vendor.\n"
        "👨‍🍳 Once the vendor confirms, a delivery guy will be assigned to bring your meal.\n"
        "\n🧭 Track your order anytime in *📍 Track Order*."
    )

    with contextlib.suppress(Exception):
        await cinematic_msg.delete()

    preview_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Track", callback_data=f"order:track:{order_id}")]
        ]
    )

    await cb.message.answer(final_preview, parse_mode="Markdown", reply_markup=preview_kb)

    # 🎞️ Finishing animation + XP reward
    status_msg = await cb.message.answer("🎬 Wrapping things up...")
    await asyncio.sleep(1.5)
    with contextlib.suppress(Exception):
        await status_msg.delete()
    await cb.message.answer("🔥 +10 XP will be added after delivery!", parse_mode="Markdown", reply_markup=main_menu())

    # Admin log: order placed, waiting for vendor
    if settings.ADMIN_DAILY_GROUP_ID:
        try:
            admin_msg = (
                f"📢 *New Order Placed: #{order_id}*\n"
                f"👤 Customer: {user['first_name']} ({user.get('phone', 'N/A')})\n"
                f"🏛 Campus: {user.get('campus', 'N/A')}\n"
                f"🍴 Vendor: {vendor_name}\n"
                f"📍 Drop-off: {data.get('dropoff', '')}\n"
                f"💵 Total: {total_payable:.2f} birr (COD)\n"
                f"⚡ Status: Waiting for vendor acceptance"
            )
            await cb.bot.send_message(settings.ADMIN_DAILY_GROUP_ID, admin_msg, parse_mode="Markdown")
        except Exception:
            pass

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