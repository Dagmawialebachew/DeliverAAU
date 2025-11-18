# handlers/student_track_order.py
import asyncio
import json
from typing import Optional, Dict, Any, List, Tuple
from aiogram.fsm.context import FSMContext
import aiosqlite
from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest
from aiogram import Bot
from handlers.onboarding import main_menu
from database import db as db_module
from config import settings
from utils import helpers

router = Router()
db = db_module.Database(settings.DB_PATH)

# Status mapping with stage index
STATUS_MAP = {
    "pending":    ("🕒 Waiting for vendor", 1),
    "assigned":   ("🙋 Delivery partner assigned", 2),
    "preparing":  ("👨‍🍳 Vendor is preparing", 2),
    "ready":      ("📦 Ready for pickup", 3),
    "on_the_way": ("🚴 Delivery partner on the way", 4),
    "in_progress":("🚴 Delivery in progress", 4),
    "delivered":  ("🎉 Delivered!", 5),
    "cancelled":  ("❌ Cancelled", 0),
}

ANIM_STEPS = ["⏺", "⏹", "⏺", "⏹"]

def render_progress(stage: int) -> str:
    total = 5
    return "".join("🟢" if i < stage else "⚪" for i in range(total))

def animated_dot(stage: int, tick: int = 0) -> str:
    return ANIM_STEPS[(stage + tick) % len(ANIM_STEPS)]


async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except Exception:
        # swallow—optionally log to DB or Sentry
        pass

async def notify_dg_ping(bot: Bot, dg_user_id: int, order_id: int, vendor_name: str):
    msg = f"📣 Pickup Reminder — Order #{order_id} at {vendor_name} is READY. Please collect now."
    await safe_send(bot, dg_user_id, msg)

async def notify_student_ready(bot: Bot, student_chat_id: int, order_id: int):
    msg = f"📦 Your order #{order_id} is ready and will be picked up soon."
    await safe_send(bot, student_chat_id, msg)


def track_reply_keyboard() -> ReplyKeyboardMarkup:
    # Compact contextual reply keyboard while tracking (no Track Order button)
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔁 Refresh"), KeyboardButton(text="⬅️ Back to Menu")]],
        resize_keyboard=True,
    )



def build_track_keyboard(
    order_id: int,
    *,
    has_dg: bool = False,
    dg_user_id: Optional[int] = None,
    dg_phone: Optional[str] = None,
    is_ready: bool = False,
    map_url: Optional[str] = None,
) -> InlineKeyboardMarkup:
    rows = []

    # Row 1: Details + Route if present
    left = InlineKeyboardButton(text="🔍 Details", callback_data=f"order:detail:{order_id}")
    refresh_btn = InlineKeyboardButton(text="❌ Close", callback_data=f"order:close:{order_id}")

    if map_url:
        right = InlineKeyboardButton(text="🗺 Route", url=map_url)
        rows.append([left, refresh_btn, right])
    else:
        rows.append([left, refresh_btn])

    # Row 2: DG actions (if DG assigned)
    if has_dg:
        call_btn = InlineKeyboardButton(text="📞 Contact D.G.", callback_data=f"track:call_dg:{order_id}")
        rows.append([call_btn])

   

    # Row 4: Back / Men
    return InlineKeyboardMarkup(inline_keyboard=rows)

# Pagination helpers
PAGE_SIZE = 3  # orders per page



@router.callback_query(F.data.startswith("track:call_dg:"))
async def callback_call_dg(cb: CallbackQuery):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("❌ Order not found.")
        return
    if not order.get("delivery_guy_id"):
        await cb.message.answer("⚠️ No delivery partner assigned yet.")
        return
    dg = await db.get_delivery_guy(order["delivery_guy_id"])
    if not dg:
        await cb.message.answer("⚠️ Delivery partner not found.")
        return
    phone = dg.get("phone") or dg.get("user_id")  # fallback to user_id if phone absent
    name = dg.get("name", "Delivery Partner")
    # Send the contact info as a secure DM message (not broadcast); user chose this action
    await cb.message.answer(f"📞 {name} — contact: {phone}")


async def _fetch_active_orders_page_for_user(user_internal_id: int, page: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    offset = page * PAGE_SIZE
    orders: List[Dict[str, Any]] = []
    total = 0
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT COUNT(*) AS cnt FROM orders WHERE user_id = ? AND status != 'delivered'",
                (user_internal_id,),
            ) as cur:
                r = await cur.fetchone()
                total = r["cnt"] if r else 0
            async with conn.execute(
                "SELECT * FROM orders WHERE user_id = ? AND status != 'delivered' "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (user_internal_id, PAGE_SIZE, offset),
            ) as cur:
                rows = await cur.fetchall()
                orders = [dict(r) for r in rows]
    except Exception:
        orders = []
        total = 0
    return orders, total


# ETA/map helper
async def _compute_eta_and_map(last_lat, last_lon, drop_lat, drop_lon):
    eta_text = ""
    map_link = ""
    try:
        info = await helpers.eta_and_distance(last_lat, last_lon, drop_lat, drop_lon)
        dist_m = int(info["distance_km"] * 1000)
        eta_min = int(info["eta_min"])
        eta_text = f"⏱ ETA: ~{eta_min} min | {dist_m} m"
    except Exception:
        try:
            dist_m = await helpers.haversine(last_lat, last_lon, drop_lat, drop_lon)
            eta_min = max(1, int(dist_m / 83))
            eta_text = f"⏱ ETA: ~{eta_min} min | {int(dist_m)} m"
        except Exception:
            eta_text = "⏱ ETA: unavailable"

    if last_lat and last_lon and drop_lat and drop_lon:
        map_link = (
            f"https://www.google.com/maps/dir/?api=1"
            f"&origin={last_lat},{last_lon}"
            f"&destination={drop_lat},{drop_lon}"
            f"&travelmode=walking"
        )
    return eta_text, map_link


# --- Dashboard: Active Orders with pagination ---
@router.message(F.text == "📍 Track Order")
async def track_order_list(message: Message):
    user = await db.get_user(message.from_user.id)
    if not user:
        await message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return

    # Show active orders immediately + attach the new menu keyboard
    await send_orders_page(message, user["id"], 0)
    await message.answer("📊 Use the menu below to navigate:", reply_markup=track_menu_keyboard())

def track_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="✨ Active Orders"),
                KeyboardButton(text="🕑 Past Orders")
            ],
            [KeyboardButton(text="⬅️ Back")]
        ],
        resize_keyboard=True
    )

@router.message(F.text == "⬅️ Back")
async def back_to_main_menu(message: Message):
    await message.answer("📋 Back to the main menu.", reply_markup=main_menu())


@router.message(F.text == "✨ Active Orders")
async def show_active_orders(message: Message):
    user = await db.get_user(message.from_user.id)
    if not user:
        await message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return
    await send_orders_page(message, user["id"], 0)



@router.message(F.text == "🕑 Past Orders")
async def show_past_orders(message: Message):
    user = await db.get_user(message.from_user.id)
    if not user:
        await message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return

    # Fetch delivered orders instead of active
    orders, total = await _fetch_past_orders_page_for_user(user["id"], 0)
    if not orders:
        await message.answer("🕑 No past orders yet.\n\n✨ Place one and start your streak!", reply_markup=track_menu_keyboard())
        return

    # Render similar dashboard but filtered for delivered
    await send_past_orders_page(message, user["id"], 0)


# Pagination helpers for past orders
PAST_PAGE_SIZE = 4  # show a bit more since they are static


async def _fetch_past_orders_page_for_user(user_internal_id: int, page: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    """Fetch paginated delivered (past) orders."""
    offset = page * PAST_PAGE_SIZE
    orders: List[Dict[str, Any]] = []
    total = 0
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT COUNT(*) AS cnt FROM orders WHERE user_id = ? AND status = 'delivered'",
                (user_internal_id,),
            ) as cur:
                r = await cur.fetchone()
                total = r["cnt"] if r else 0

            async with conn.execute(
                "SELECT * FROM orders WHERE user_id = ? AND status = 'delivered' "
                "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (user_internal_id, PAST_PAGE_SIZE, offset),
            ) as cur:
                rows = await cur.fetchall()
                orders = [dict(r) for r in rows]
    except Exception:
        orders = []
        total = 0
    return orders, total



async def _fetch_single_past_order(order_id: int):
    """Fetch one delivered order by ID."""
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM orders WHERE id = ? AND status = 'delivered'", (order_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
    except Exception:
        return None


@router.callback_query(lambda c: c.data.startswith("past:view:"))
async def handle_past_order_view(callback: CallbackQuery):
    """Show full receipt-like breakdown of one delivered order."""
    try:
        _, _, order_id_str = callback.data.split(":")
        order_id = int(order_id_str)
    except Exception:
        await callback.answer("Invalid order ID.", show_alert=True)
        return

    order = await _fetch_single_past_order(order_id)
    if not order:
        await callback.answer("Order not found or not delivered yet.", show_alert=True)
        return

    # Parse breakdown
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
        items = breakdown.get("items", [])
    except Exception:
        items = []

    # Receipt text layout
    text_lines = [
        "🧾 **Order Receipt**",
        f"──────────────────────────",
        f"**Order ID:** #{order_id}",
        f"**Status:** ✅ Delivered",
    ]

    if order.get("delivered_at"):
        text_lines.append(f"**Delivered At:** {order['delivered_at']}")

    text_lines.append("\n🍴 **Items:**")
    if items:
        for it in items:
            text_lines.append(f" • {it}")
    else:
        text_lines.append(" • (details unavailable)")

    subtotal = order.get("food_subtotal")
    delivery_fee = order.get("delivery_fee")
    
    text_lines.append(
        f"\n💳 **Payment:** {order.get('payment_method', 'N/A').capitalize()}\n"
        f"💰 **Total:** {subtotal + delivery_fee} birr"
    )

    # Optional breakdown summary
    if subtotal and delivery_fee:
        text_lines.append(f"\nSubtotal: {subtotal} birr\nDelivery Fee: {delivery_fee} birr")

    text_lines.append("\nThank you for choosing DeliverAAU! 🌍")

    # Inline buttons for navigation
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ Back", callback_data=f"past:back"),
            ]
        ]
    )

    try:
        await callback.message.edit_text(
            "\n".join(text_lines),
            reply_markup=kb,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception:
        await callback.message.answer(
            "\n".join(text_lines),
            reply_markup=kb,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    await callback.answer()
    


@router.callback_query(lambda c: c.data == "past:back")
async def handle_past_back(callback: CallbackQuery, state: FSMContext = None):
    await callback.answer("Returning to your orders…", show_alert=False)

    user = await db.get_user(callback.from_user.id)
    if not user:
        await callback.message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return

    try:
        await send_past_orders_page(callback, user["id"], page=0)
    except Exception as e:
        print("Error returning to past orders:", e)
        await callback.message.answer("⚠️ Couldn't load your past orders right now. Please try again.")

async def send_past_orders_page(message_or_callback, user_id: int, page: int):
    """Display paginated delivered orders — calmer, receipt-style design (HTML mode)."""
    orders, total = await _fetch_past_orders_page_for_user(user_id, page)
    if not orders:
        text = "🧾 No completed orders yet.\n\n✨ Once your meals are delivered, they'll appear here."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.answer()
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    total_pages = max(1, (total + PAST_PAGE_SIZE - 1) // PAST_PAGE_SIZE)
    lines = [f"📚 <b>Past Orders</b> — Page {page + 1}/{total_pages}", "──────────────────────────"]

    kb_rows: List[List[InlineKeyboardButton]] = []
    order_buttons: List[InlineKeyboardButton] = []

    for o in orders:
        try:
            breakdown = json.loads(o.get("breakdown_json") or "{}")
            items = breakdown.get("items", [])
        except Exception:
            items = []
        items_preview = " • ".join(items[:3]) if items else "…"

        payment_method = o.get("payment_method", "N/A").capitalize()
        total_cost = (
            int(o.get("food_subtotal", 0)) + int(o.get("delivery_fee", 0))
            if o.get("food_subtotal") and o.get("delivery_fee")
            else o.get("total", "—")
        )

        lines.append(
            f"#{o['id']} — ✅ Delivered\n"
            f"   🍴 {items_preview}\n"
            f"   💳 {payment_method} | 💰 {total_cost} birr\n"
        )

        order_buttons.append(
            InlineKeyboardButton(text=f"#{o['id']}", callback_data=f"past:view:{o['id']}")
        )

    if order_buttons:
        kb_rows.append(order_buttons)

    pagination_row: List[InlineKeyboardButton] = []
    if page > 0:
        pagination_row.append(
            InlineKeyboardButton(text="⬅️ Prev", callback_data=f"past:page:{page - 1}")
        )
    pagination_row.append(
        InlineKeyboardButton(text=f"📄 {page + 1}", callback_data="past:noop")
    )
    if (page + 1) * PAST_PAGE_SIZE < total:
        pagination_row.append(
            InlineKeyboardButton(text="Next ➡️", callback_data=f"past:page:{page + 1}")
        )
    kb_rows.append(pagination_row)

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    text = "\n".join(lines) + "\n\n🧾 Tap an order above to view its full receipt."
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.answer()
        try:
            await message_or_callback.message.edit_text(
                text,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            await message_or_callback.message.answer(
                text,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    else:
        await message_or_callback.answer(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

async def send_orders_page(message_or_callback, user_id: int, page: int):
    """Display active orders dashboard — HTML mode."""
    orders, total = await _fetch_active_orders_page_for_user(user_id, page)
    if not orders:
        text = "🍽 No active orders right now.\n\n✨ Place one and fuel your campus journey!"
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.answer()
            await message_or_callback.message.edit_text(text, reply_markup=None)
        else:
            await message_or_callback.answer(text, reply_markup=main_menu())
        return

    # If only one order, open it immediately
    if len(orders) == 1:
        single = orders[0]
        cb_like = message_or_callback if isinstance(message_or_callback, CallbackQuery) else None
        if cb_like:
            await back_to_summary(cb_like)
        else:
            fake_cb = type(
                "Fake",
                (),
                {
                    "data": f"order:view:{single['id']}",
                    "message": message_or_callback,
                    "answer": lambda *a, **k: None,
                },
            )
            await back_to_summary(fake_cb)
        return

    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    lines = [f"📊 <b>Active Orders Dashboard</b> — Page {page + 1}/{total_pages}", "──────────────────────────"]

    kb_rows: List[List[InlineKeyboardButton]] = []
    order_buttons: List[InlineKeyboardButton] = []

    for o in orders:
        status_text, stage = STATUS_MAP.get(o["status"], (o["status"], 1))
        progress = render_progress(stage)

        try:
            breakdown = json.loads(o.get("breakdown_json") or "{}")
            items = breakdown.get("items", [])
        except Exception:
            items = []
        items_preview = " • ".join(items[:2]) if items else "…"

        lines.append(
            f"#{o['id']} — {status_text}\n"
            f"   🍴 {items_preview}\n"
        )

        order_buttons.append(
            InlineKeyboardButton(text=f"#{o['id']}", callback_data=f"order:view:{o['id']}")
        )

    if order_buttons:
        kb_rows.append(order_buttons)

    pagination_row: List[InlineKeyboardButton] = []
    if page > 0:
        pagination_row.append(
            InlineKeyboardButton(text="⬅️ Prev", callback_data=f"orders:page:{page - 1}")
        )
    pagination_row.append(
        InlineKeyboardButton(text=f"📄 {page + 1}", callback_data="orders:noop")
    )
    if (page + 1) * PAGE_SIZE < total:
        pagination_row.append(
            InlineKeyboardButton(text="Next ➡️", callback_data=f"orders:page:{page + 1}")
        )
    kb_rows.append(pagination_row)

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    text = "\n".join(lines) + "\n\n✨ Tap an order above to open its live card."
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.answer()
        try:
            await message_or_callback.message.edit_text(
                text,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            await message_or_callback.message.answer(
                text,
                reply_markup=kb,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    else:
        await message_or_callback.answer(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


# --- Pagination callbacks ---
@router.callback_query(F.data.startswith("orders:page:"))
async def orders_page_callback(cb: CallbackQuery):
    page = int(cb.data.split(":")[-1])
    user = await db.get_user(cb.from_user.id)
    if not user:
        await cb.answer("⚠️ Please /start to register first.")
        return
    await send_orders_page(cb, user["id"], page)


@router.callback_query(F.data == "orders:close")
async def orders_close(cb: CallbackQuery):
    try:
        await cb.message.delete()
    except Exception:
        try:
            await cb.message.edit_text("Closed.")
        except Exception:
            pass
    await cb.answer()


@router.callback_query(F.data == "orders:noop")
async def orders_noop(cb: CallbackQuery):
    await cb.answer()  # page indicator noop


# --- Per-order view (opened from dashboard) ---
@router.callback_query(F.data.startswith("order:view:"))
async def order_view_from_dashboard(cb: CallbackQuery):
    order_id = int(cb.data.split(":")[2])
    order = await db.get_order(order_id)
    if not order:
        await cb.answer()
        try:
            await cb.message.edit_text("❌ Order not found.")
        except Exception:
            await cb.message.answer("❌ Order not found.")
        return
    await cb.answer()
    await back_to_summary(cb)  # reuse single-order summary rendering


# --- Optional: long-running single-order tracker (silent background refresh) ---
@router.callback_query(F.data.startswith("order:track:"))
async def order_track_long(cb: CallbackQuery):
    order_id = int(cb.data.split(":")[2])
    order = await db.get_order(order_id)
    if not order:
        await cb.answer()
        try:
            await cb.message.edit_text("❌ Order not found.")
        except Exception:
            await cb.message.answer("❌ Order not found.")
        return

    await cb.answer()
    order = dict(order)
    editable_msg = None
    tick = 0
    refresh_interval = 5
    max_rounds = 120
    rounds = 0

    try:
        await cb.message.answer("🔎 Tracking your order — live updates will refresh here.", reply_markup=track_reply_keyboard())
    except Exception:
        pass

    paused = False
    user_wants_pause = False

    while True:
        order = await db.get_order(order["id"])
        if not order:
            try:
                if editable_msg:
                    await editable_msg.edit_text("❌ Order not found (it may have been removed).", reply_markup=None)
                else:
                    await cb.message.answer("❌ Order not found (it may have been removed).", reply_markup=main_menu())
            except Exception:
                pass
            break

        order = dict(order)
        status_text, stage = STATUS_MAP.get(order["status"], (order["status"], 1))

        # Vendor info
        vendor = await db.get_vendor(order["vendor_id"]) if order.get("vendor_id") else None
        vendor_name = vendor.get("name") if vendor else "Vendor"
        vendor_line = f"🏪 {vendor_name}"
        if vendor and vendor.get("rating_avg") is not None:
            vendor_line += f" • ★ {round(vendor.get('rating_avg',0),1)} ({vendor.get('rating_count',0)})"

        # DG info
        dg_text = ""
        dg_profile_line = ""
        has_dg = False
        dg_phone = None
        dg_user_id = None
        last_lat = order.get("last_lat")
        last_lon = order.get("last_lon")
        if order.get("delivery_guy_id"):
            dg = await db.get_delivery_guy(order["delivery_guy_id"])
            if dg:
                has_dg = True
                dg_user_id = dg.get("user_id")
                dg_phone = dg.get("phone") or None
                dg_name = dg.get("name", "Delivery Partner")
                dg_rating = dg.get("rating", None) or dg.get("rating_avg", None)
                dg_campus = dg.get("campus", "")
                dg_profile_line = f"🚴 {dg_name} ({dg_campus})"
                if dg_rating is not None:
                    dg_profile_line += f" • ★ {round(dg_rating,1)}"
                last_lat = last_lat or dg.get("last_lat")
                last_lon = last_lon or dg.get("last_lon")
                dg_text = dg_profile_line

        # breakdown / coords
        breakdown = {}
        drop_lat = drop_lon = None
        try:
            breakdown = json.loads(order.get("breakdown_json") or "{}")
            drop_lat = breakdown.get("drop_lat")
            drop_lon = breakdown.get("drop_lon")
        except Exception:
            drop_lat = drop_lon = None

        # ETA & map
        eta_text = ""
        map_url = None
        if last_lat and last_lon and drop_lat and drop_lon:
            try:
                eta_text, map_url = await _compute_eta_and_map(last_lat, last_lon, drop_lat, drop_lon)
            except Exception:
                eta_text = "⏱ ETA: unavailable"
        else:
            eta_text = "⌛ Waiting for live location..." if has_dg else "⌛ Waiting for a delivery partner..."

        # timestamps
        created = order.get("created_at") or "—"
        accepted = order.get("accepted_at") or "—"
        delivered = order.get("delivered_at") or "—"

        # items summary
        try:
            breakdown_items = breakdown.get("items") or []
            items_preview = ", ".join([str(i) if isinstance(i, str) else i.get("name","") for i in breakdown_items[:6]]) or "Items"
        except Exception:
            items_preview = "Items"

        # compose text
        text = (
            f"{animated_dot(stage, tick)} {status_text}\n\n"
            f"{render_progress(stage)}\n\n"
            f"{vendor_line}\n"
            f"{dg_text + chr(10) if dg_text else ''}"
            f"{eta_text + chr(10) if eta_text else ''}"
            f"\n🛒 {items_preview}\n"
            f"💵 {order.get('food_subtotal',0)} Birr • Fee: {order.get('delivery_fee',0)} Birr\n\n"
            f"⏱ Created: {created}  •  Accepted: {accepted}  •  Delivered: {delivered}\n\n"
            "✨ Hang tight — your campus meal is on its way!"
        )

        # build keyboard with flags
        kb = build_track_keyboard(
            order["id"],
            has_dg=has_dg,
            dg_user_id=dg_user_id,
            dg_phone=dg_phone,
            is_ready=(order.get("status") == "ready"),
            map_url=map_url,
        )

        # send/edit message safely
        try:
            if editable_msg is None:
                editable_msg = await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            else:
                try:
                    await editable_msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
                except TelegramBadRequest as e:
                    if "message is not modified" in str(e):
                        pass
                    else:
                        raise
        except Exception:
            try:
                editable_msg = await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                break

        # delivered end-state
        if order.get("status") == "delivered":
            final_text = f"🎉 Order #{order['id']} has been delivered! Enjoy your meal 💚"
            try:
                await editable_msg.edit_text(final_text, reply_markup=None)
            except Exception:
                await cb.message.answer(final_text, reply_markup=main_menu())
            break

        rounds += 1
        if rounds >= max_rounds:
            try:
                await editable_msg.edit_text(
                    text + "\n\n⌛ Live updates paused to save resources. Tap 🔁 Refresh or reopen Track Order to continue.",
                    reply_markup=kb,
                    disable_web_page_preview=True
                )
            except Exception:
                pass
            break

        tick += 1
        await asyncio.sleep(refresh_interval)
# --- Reply keyboard: silent Refresh and Back to Menu ---
@router.message(F.text == "🔁 Refresh")
async def reply_refresh_silent(message: Message):
    try:
        await message.delete()
    except TelegramBadRequest as e:
        if "message to delete not found" in str(e):
            pass
        else:
            raise
    except Exception:
        pass

    # Tiny ephemeral confirmation
    try:
        info_msg = await message.answer("🔁 Refreshed — live info updated.")
        await asyncio.sleep(0.8)
        try:
            await info_msg.delete()
        except Exception:
            pass
    except Exception:
        pass


@router.message(F.text == "⬅️ Back to Menu")
async def reply_back_to_menu(message: Message):
    try:
        await message.delete()
    except TelegramBadRequest as e:
        if "message to delete not found" in str(e):
            pass
        else:
            raise
    except Exception:
        pass

    try:
        await message.answer("📋 Back to the main menu.", reply_markup=main_menu())
    except Exception:
        pass


# --- Full Order Detail Handler (receipt style) ---
@router.callback_query(F.data.startswith("order:detail:"))
async def show_order_detail(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[2])
    order = await db.get_order(order_id)
    if not order:
        await callback.answer()
        try:
            await callback.message.edit_text("❌ Order not found.")
        except Exception:
            await callback.message.answer("❌ Order not found.")
        return

    # Parse breakdown JSON
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
    except Exception:
        breakdown = {}
    items_list = breakdown.get("items", [])
    items_str = "\n".join([f"• {i}" for i in items_list[:10]]) if isinstance(items_list, list) else str(items_list or "N/A")

    # ETA
    last_lat = order.get("last_lat")
    last_lon = order.get("last_lon")
    drop_lat = breakdown.get("drop_lat")
    drop_lon = breakdown.get("drop_lon")
    eta_text = ""
    if last_lat and last_lon and drop_lat and drop_lon:
        try:
            eta_text, _ = await _compute_eta_and_map(last_lat, last_lon, drop_lat, drop_lon)
        except Exception:
            eta_text = "⏱ ETA: unavailable"

    # Receipt‑style text
    text = (
        f"🧾 **Order Receipt #{order['id']}**\n"
        f"──────────────────────────\n"
        f"{eta_text + chr(10) if eta_text else ''}"
        f"🏠 Pickup: {order.get('pickup','N/A')}\n"
        f"📍 Drop‑off: {order.get('dropoff','N/A')}\n\n"
        f"🍴 Items:\n{items_str}\n\n"
        f"💰 Subtotal: {order.get('food_subtotal','0')} birr\n"
        f"🚚 Delivery Fee: {order.get('delivery_fee','0')} birr\n"
        f"💳 Payment: {order.get('payment_method','N/A')} ({order.get('payment_status','N/A')})\n"
        f"──────────────────────────\n"
        "✨ Thanks for ordering with Deliver AAU!"
    )

    # Inline keyboard: Back + Refresh in one row, Close in another
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ Back", callback_data=f"order:back:{order['id']}"),
                InlineKeyboardButton(text="🔁 Refresh", callback_data=f"order:refresh:{order['id']}")
            ],
        ]
    )

    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await callback.message.answer(text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()

# --- Back / Refresh / Close routing to single-order summary ---
@router.callback_query(F.data.startswith("order:back:") | F.data.startswith("order:refresh:") | F.data.startswith("order:close:"))
async def back_to_summary(callback: CallbackQuery):
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    order_id = int(parts[-1])

    if action == "close":
        try:
            await callback.message.delete()
        except Exception:
            try:
                await callback.message.edit_text("Closed.")
            except Exception:
                pass
        await callback.answer()
        return

    order = await db.get_order(order_id)
    if not order:
        await callback.answer()
        try:
            await callback.message.edit_text("❌ Order not found.")
        except Exception:
            await callback.message.answer("❌ Order not found.")
        return

    order = dict(order)
    status_text, stage = STATUS_MAP.get(order["status"], (order["status"], 1))

    dg_text = ""
    has_dg = False
    dg_user_id = None
    dg_phone = None
    last_lat = order.get("last_lat")
    last_lon = order.get("last_lon")

    breakdown = {}
    drop_lat = None
    drop_lon = None
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
        drop_lat = breakdown.get("drop_lat")
        drop_lon = breakdown.get("drop_lon")
    except Exception:
        pass

    if order.get("delivery_guy_id"):
        dg = await db.get_delivery_guy(order["delivery_guy_id"])
        if dg:
            has_dg = True
            dg_user_id = dg.get("user_id")
            dg_phone = dg.get("phone")
            dg_text = f"Delivery: {dg.get('name')} ({dg.get('campus','')})"
            last_lat = last_lat or dg.get("last_lat")
            last_lon = last_lon or dg.get("last_lon")

    if last_lat and last_lon and drop_lat and drop_lon:
        try:
            eta_text, map_url = await _compute_eta_and_map(last_lat, last_lon, drop_lat, drop_lon)
        except Exception:
            eta_text = "⏱ ETA: unavailable"
            map_url = None
    else:
        eta_text = "⌛ Waiting for live location..." if has_dg else "⌛ Waiting for a delivery partner..."
        map_url = None

    text = (
        f"{animated_dot(stage)} {status_text}\n\n"
        f"Progress: {render_progress(stage)}\n"
        f"{dg_text + chr(10) if dg_text else ''}"
        f"{eta_text + chr(10) if eta_text else ''}"
        f"\n🏠 Pickup: {order.get('pickup','N/A')}\n"
        f"📍 Drop-off: {order.get('dropoff','N/A')}\n\n"
        "✨ Hang tight — your campus meal is on its way!"
    )

    kb = build_track_keyboard(
        order["id"],
        map_url=map_url,
        has_dg=has_dg,
        dg_user_id=dg_user_id,
        dg_phone=dg_phone,
        is_ready=(order.get("status") == "ready"),
    )

    try:
        await callback.message.edit_text(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    await callback.answer()


# --- Inline Refresh / Close passthrough ---
@router.callback_query(F.data.startswith("order:refresh:"))
async def refresh_order_card(callback: CallbackQuery):
    await back_to_summary(callback)


@router.callback_query(F.data.startswith("order:close:"))
async def close_order_card(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        try:
            await callback.message.edit_text("Closed.")
        except Exception:
            pass
    await callback.answer()
