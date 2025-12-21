# handlers/student_track_order.py
import asyncio
from collections import Counter
import inspect
import json
from typing import Optional, Dict, Any, List, Tuple
from aiogram.fsm.context import FSMContext
from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from utils.helpers import time_ago

from aiogram.exceptions import TelegramBadRequest
from aiogram import Bot
from handlers.onboarding import main_menu
from database import db as db_module
from config import settings
from utils import helpers

router = Router()
from app_context import db

# Status mapping with stage index
STATUS_MAP = {
    "pending":    ("🕒 Meal request sent — waiting for confirmation…", 1),
    "assigned":   ("🙋 Delivery partner assigned", 2),
    "preparing":  ("👨‍🍳 The café is cooking up your order", 2),
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


async def notify_student(bot: Bot, student_chat_id: int, order_id: int):
    msg = (
        f"📦 Your order #{order_id} is ready!\n"
        f"⏳ Maximum pickup time: *15 minutes*.\n\n"
        "Tap below to track your order in real‑time:"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Track", callback_data=f"order:track:{order_id}")]
        ]
    )

    await safe_send(bot, student_chat_id, msg, parse_mode="Markdown", reply_markup=kb)
    
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
    paused: bool = False,   # NEW: pause flag
) -> InlineKeyboardMarkup:
    rows = []

    # Row 1: Details + Route if present
    left = InlineKeyboardButton(text="🔍 Details", callback_data=f"order:detail:{order_id}")
    close_btn = InlineKeyboardButton(text="❌ Close", callback_data=f"order:close:{order_id}")

    if map_url:
        right = InlineKeyboardButton(text="🗺 Route", url=map_url)
        rows.append([left, close_btn, right])
    else:
        rows.append([left, close_btn])

    # Row 2: DG actions (if DG assigned)
    refresh_btn = InlineKeyboardButton(text="🔁 Refresh", callback_data=f"order:refresh_order:{order_id}")
    if has_dg:
        call_btn = InlineKeyboardButton(text="📞 Contact D.G.", callback_data=f"track:call_dg:{order_id}")
        rows.append([call_btn, refresh_btn])
    else:
        rows.append([refresh_btn])
    # Row 3: Pause/Resume toggle
    # if paused:
    #     resume_btn = InlineKeyboardButton(text="▶️ Resume", callback_data=f"order:resume:{order_id}")
    #     rows.append([resume_btn])
    # else:
    #     pause_btn = InlineKeyboardButton(text="⏸ Pause", callback_data=f"order:pause:{order_id}")
    #     rows.append([pause_btn])

    return InlineKeyboardMarkup(inline_keyboard=rows)

# Pagination helpers
PAGE_SIZE = 3  # orders per page



@router.callback_query(F.data.startswith("order:refresh_order:"))
async def refresh_order_card(callback: CallbackQuery):
    order_id = int(callback.data.split(":")[-1])

    text, kb = await render_order_summary(order_id)
    if not kb:
        await callback.answer("❌ Order not found.", show_alert=True)
        return

    try:
        await callback.message.edit_text(
            text,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        # If edit succeeds, show a subtle toast
        await callback.answer("🔄 Order refreshed!")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Nothing changed, but acknowledge
            await callback.answer("✅ Already up to date")
        else:
            # Fallback: send a new message
            await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
            await callback.answer("🔄 Refreshed with new message")

@router.callback_query(F.data.startswith("track:call_dg:"))
async def handle_contact_dg(call: CallbackQuery):
    await call.answer()

    # Parse order_id correctly from colon-separated callback_data
    try:
        order_id = int(call.data.split(':')[-1])
    except ValueError:
        await call.answer("❌ Invalid order ID format.", show_alert=True)
        return

    order = await db.get_order(order_id)
    if not order:
        await call.answer("❌ Order not found.", show_alert=True)
        return

    if not order.get("delivery_guy_id"):
        await call.answer("⚠️ No delivery partner assigned yet.", show_alert=True)
        return

    dg = await db.get_delivery_guy(order["delivery_guy_id"])
    if not dg:
        await call.answer("⚠️ Delivery partner not found.", show_alert=True)
        return

    phone = dg.get("phone")
    first_name = dg.get("name", "Delivery Partner")
    last_name = ""  # optional

    if phone:
        # One message: name and phone, phone isolated on its own line
        contact_text = (
            f"👤 {first_name} {last_name}\n"
            f"{phone}"
        )
        await call.message.answer(contact_text)
        await call.answer("📱 Delivery partner contact shared.")
    else:
        await call.answer("❌ No phone number available for this delivery partner.", show_alert=True)


ACTIVE_STATUSES = ('pending', 'assigned', 'preparing', 'ready', 'in_progress')

async def _fetch_active_orders_page_for_user(user_internal_id: int, page: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    offset = page * PAGE_SIZE
    orders: List[Dict[str, Any]] = []
    total = 0
    try:
        async with db._open_connection() as conn:
            # Count total active orders
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM orders WHERE user_id = $1 AND status = ANY($2::text[])",
                user_internal_id, ACTIVE_STATUSES
            ) or 0

            # Fetch paginated orders
            rows = await conn.fetch(
                """
                SELECT * FROM orders
                WHERE user_id = $1 AND status = ANY($2::text[])
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                """,
                user_internal_id, ACTIVE_STATUSES, PAGE_SIZE, offset
            )
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
        async with db._open_connection() as conn:
            # Count delivered orders
            # Count delivered + cancelled
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM orders WHERE user_id = $1 AND status IN ('delivered', 'cancelled')",
                user_internal_id
            ) or 0

            # Fetch paginated delivered + cancelled orders
            rows = await conn.fetch(
                """
                SELECT * FROM orders
                WHERE user_id = $1 AND status IN ('delivered', 'cancelled')
                ORDER BY updated_at DESC
                LIMIT $2 OFFSET $3
                """,
                user_internal_id, PAST_PAGE_SIZE, offset
            )
            orders = [dict(r) for r in rows]

    except Exception:
        orders = []
        total = 0
    return orders, total


async def _fetch_single_past_order(order_id: int) -> Optional[Dict[str, Any]]:
    """Fetch one delivered order by ID."""
    try:
        async with db._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE id = $1 AND status IN ('delivered', 'cancelled')",
                order_id
            )
            return dict(row) if row else None
    except Exception:
        return None
    
    

@router.callback_query(lambda c: c.data.startswith("past:view:"))
async def handle_past_order_view(callback: CallbackQuery):
    """Show full receipt-like breakdown of one past order (delivered or cancelled)."""
    try:
        _, _, order_id_str = callback.data.split(":")
        order_id = int(order_id_str)
    except Exception:
        await callback.answer("Invalid order ID.", show_alert=True)
        return

    order = await _fetch_single_past_order(order_id)
    if not order:
        await callback.answer("Order not found.", show_alert=True)
        return

    # Parse breakdown
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
        items = breakdown.get("items", [])
    except Exception:
        items = []

    # Status handling
    status = (order.get("status") or "").lower()
    if status == "delivered":
        status_str = "✅ Delivered"
    elif status == "cancelled":
        status_str = "❌ Cancelled"
    else:
        status_str = status.capitalize() or "—"

    # Receipt text layout
    text_lines = [
        "🧾 **Order Receipt**",
        "──────────────────────────",
        f"**Order ID:** #{order_id}",
        f"**Status:** {status_str}",
    ]

    if status == "delivered" and order.get("delivered_at"):
        text_lines.append(f"**Delivered At:** {order['delivered_at']}")
    if status == "cancelled" and order.get("updated_at"):
        text_lines.append(f"**Cancelled At:** {order['updated_at']}")

    text_lines.append("\n🍴 **Items:**")
    if items:
        counts = Counter(items)
        for name, count in counts.items():
            text_lines.append(f" • {name} x{count}" if count > 1 else f" • {name}")
    else:
        text_lines.append(" • (details unavailable)")

    subtotal = order.get("food_subtotal") or 0
    delivery_fee = order.get("delivery_fee") or 0
    total = subtotal + delivery_fee if subtotal or delivery_fee else order.get("total", 0)

    text_lines.append(
        f"\n💳 **Payment:** {order.get('payment_method', 'N/A').capitalize()}\n"
        f"💰 **Total:** {total} birr"
    )

    if subtotal and delivery_fee:
        text_lines.append(f"\nSubtotal: {subtotal} birr\nDelivery Fee: {delivery_fee} birr")

    # Footer
    if status == "delivered":
        text_lines.append("\nThank you for choosing DeliverAAU! 🌍")
        text_lines.append("\n⭐ Please rate your overall experience:")
    else:
        text_lines.append("\nThis order was cancelled. ❌")

    # Inline buttons
    kb_rows = []
    if status == "delivered":
        kb_rows.append([
            InlineKeyboardButton(text="⭐1", callback_data=f"rate_vendor:{order_id}:1"),
            InlineKeyboardButton(text="⭐2", callback_data=f"rate_vendor:{order_id}:2"),
            InlineKeyboardButton(text="⭐3", callback_data=f"rate_vendor:{order_id}:3"),
            InlineKeyboardButton(text="⭐4", callback_data=f"rate_vendor:{order_id}:4"),
            InlineKeyboardButton(text="⭐5", callback_data=f"rate_vendor:{order_id}:5"),
        ])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="past:back")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

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
    try:
        await callback.answer()
    except Exception:
        pass


@router.callback_query(lambda c: c.data.startswith("past:page:"))
async def handle_past_orders_pagination(callback: CallbackQuery):
    try:
        # Always take the last part as the page number
        page_str = callback.data.split(":")[-1]
        page = int(page_str)
    except Exception as e:
        await callback.answer("Invalid page.", show_alert=True)
        return

    user = await db.get_user(callback.from_user.id)
    if not user:
        print(f"[Pagination] No user found for Telegram id={callback.from_user.id}")
        await callback.message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return

    # Fetch orders and total
    orders, total = await _fetch_past_orders_page_for_user(user["id"], page)
    total_pages = max(1, (total + PAST_PAGE_SIZE - 1) // PAST_PAGE_SIZE)
    
    # Clamp page to valid range
    if page < 0:
        page = 0
    elif page >= total_pages:
        page = total_pages - 1

    await send_past_orders_page(callback, user["id"], page)
    try:
        await callback.answer()
    except Exception as e:
        print(f"[Pagination] callback.answer failed: {e}")

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
        await callback.message.answer("⚠️ Couldn't load your past orders right now. Please try again.")
async def send_past_orders_page(message_or_callback, user_id: int, page: int):
    """Display paginated past orders (delivered + cancelled) — calmer, receipt-style design (HTML mode)."""
    orders, total = await _fetch_past_orders_page_for_user(user_id, page)
    if not orders:
        text = "🧾 No past orders yet.\n\n✨ Once your meals are delivered or cancelled, they'll appear here."
        if isinstance(message_or_callback, CallbackQuery):
            try:
                await message_or_callback.answer()
            except Exception:
                pass
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

        if items:
            counts = Counter(items)
            items_preview = " • ".join(
                f"{name} x{count}" if count > 1 else name
                for name, count in list(counts.items())[:3]
            )
        else:
            items_preview = "…"

        payment_method = o.get("payment_method", "N/A").capitalize()
        total_cost = (
            int(o.get("food_subtotal", 0)) + int(o.get("delivery_fee", 0))
            if o.get("food_subtotal") and o.get("delivery_fee")
            else o.get("total", "—")
        )

        # Status handling
        status = o.get("status", "").lower()
        if status == "delivered":
            status_str = "✅ Delivered"
        elif status == "cancelled":
            status_str = "❌ Cancelled"
        else:
            status_str = status.capitalize() or "—"

        lines.append(
            f"#{o['id']} — {status_str}\n"
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
        try:
            await message_or_callback.answer()
        except Exception:
            pass
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
        if isinstance(message_or_callback, CallbackQuery):
            await back_to_summary(message_or_callback)
        else:
            async def fake_answer(*a, **k):
                return True

            fake_cb = type(
                "Fake",
                (),
                {
                    "data": f"order:view:{single['id']}",
                    "message": message_or_callback,
                    "answer": fake_answer,
                    "from_user": type("User", (), {"id": message_or_callback.from_user.id})(),
                },
            )()
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

        if items:
            # Extract names from dicts or use string directly
            names = [
                i["name"] if isinstance(i, dict) else str(i)
                for i in items
            ]
            counts = Counter(names)

            # Show up to 2 unique items with counts
            items_preview = " • ".join(
                f"{name} x{count}" if count > 1 else name
                for name, count in list(counts.items())[:2]
            )
        else:
            items_preview = "…"

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

@router.callback_query(lambda c: c.data.startswith("past:page:"))
async def handle_past_orders_pagination(callback: CallbackQuery):
    try:
        _, _, page_str = callback.data.split(":")
        page = int(page_str)
    except Exception:
        await callback.answer("Invalid page.", show_alert=True)
        return

    user = await db.get_user(callback.from_user.id)
    if not user:
        await callback.message.answer("⚠️ Please /start to register first.", reply_markup=main_menu())
        return

    try:
        await send_past_orders_page(callback, user["id"], page)
    except Exception as e:
        await callback.message.answer("⚠️ Couldn't load your past orders right now. Please try again.")

    try:
        await callback.answer()
    except Exception:
        pass


@router.callback_query(lambda c: c.data == "past:noop")
async def handle_past_orders_noop(callback: CallbackQuery):
    # Just acknowledge so Telegram doesn’t complain
    try:
        await callback.answer()
    except Exception:
        pass


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
    try:
            await cb.answer()  # acknowledge right away
    except Exception:
            pass 

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
    # Answer once, immediately
    try:
        await cb.answer("⏳ Tracking started…")
    except Exception:
        pass

    order_id = int(cb.data.split(":")[2])
    order = await db.get_order(order_id)
    if not order:
        try:
            await cb.message.edit_text("❌ Order not found.")
        except Exception:
            await cb.message.answer("❌ Order not found.")
        return

    editable_msg = None
    tick = 0
    refresh_interval = 40
    max_rounds = 120
    rounds = 0
    paused = False

    try:
        await cb.message.answer(
            "🔎 Tracking your order — live updates will refresh here.",
            reply_markup=track_menu_keyboard()
        )
    except Exception:
        pass

    while rounds < max_rounds and not paused:
        text, kb = await render_order_summary(order_id, tick, paused)

        try:
            if editable_msg is None:
                editable_msg = await cb.message.answer(
                    text, reply_markup=kb, disable_web_page_preview=True
                )
            else:
                try:
                    await editable_msg.edit_text(
                        text, reply_markup=kb, disable_web_page_preview=True
                    )
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e):
                        editable_msg = await cb.message.answer(
                            text, reply_markup=kb, disable_web_page_preview=True
                        )
        except Exception:
            break

        order = await db.get_order(order_id)
        if order and order.get("status") == "delivered":
            final_text = f"🎉 Order #{order_id} has been delivered! Enjoy your meal 💚"
            try:
                await editable_msg.edit_text(final_text, reply_markup=None)
            except Exception:
                await cb.message.answer(final_text, reply_markup=main_menu())
            break

        rounds += 1
        tick += 1

        if rounds == 12:
            refresh_interval = 10
        elif rounds == 36:
            refresh_interval = 30

        if paused:
            try:
                await editable_msg.edit_text(
                    text + "\n\n⏸ Live updates paused. Tap ▶️ Resume to continue.",
                    reply_markup=kb,
                    disable_web_page_preview=True
                )
            except Exception:
                pass
            break

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
    # 1) Acknowledge immediately to avoid "query is too old"
    try:
        await callback.answer()
    except Exception:
        pass  # ignore expired queries

    # 2) Parse order id safely
    parts = callback.data.split(":")
    if len(parts) < 3 or not parts[2].isdigit():
        # Fallback UX: edit or answer message
        try:
            await callback.message.edit_text("❌ Invalid order reference.")
        except Exception:
            await callback.message.answer("❌ Invalid order reference.")
        return
    order_id = int(parts[2])

    # 3) Fetch order
    order = await db.get_order(order_id)
    if not order:
        try:
            await callback.message.edit_text("❌ Order not found.")
        except Exception:
            await callback.message.answer("❌ Order not found.")
        return

    # 4) Breakdown parsing
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
    except Exception:
        breakdown = {}
    items_list = breakdown.get("items", [])
    if isinstance(items_list, list):
        counts = Counter(items_list)
        items_str = "\n".join(
            f"• {name} x{count}" if count > 1 else f"• {name}"
            for name, count in list(counts.items())[:10]
        )
        if not items_str:
            items_str = "• N/A"
    else:
        items_str = str(items_list or "N/A")

    # 5) ETA computation: wrap to prevent crash
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

    # 6) Time stamps (defensive against None)
    created = time_ago(order.get("created_at")) if order.get("created_at") else "N/A"
    accepted = time_ago(order.get("accepted_at")) if order.get("accepted_at") else "N/A"
    delivered = time_ago(order.get("delivered_at")) if order.get("delivered_at") else "N/A"

    # 7) Totals: ensure numeric
    food_subtotal = order.get("food_subtotal") or 0
    delivery_fee = order.get("delivery_fee") or 0
    dropoff = order.get('dropoff', 'N/A')
    campus_text = await db.get_user_campus_by_order(order['id'])
    dropoff = f"{dropoff} • {campus_text}" if campus_text else dropoff
    
    try:
        total_birr = (float(food_subtotal) if not isinstance(food_subtotal, (int, float)) else food_subtotal) + \
                     (float(delivery_fee) if not isinstance(delivery_fee, (int, float)) else delivery_fee)
    except Exception:
        total_birr = food_subtotal if isinstance(food_subtotal, (int, float)) else 0

    # 8) Use consistent parse_mode (Markdown vs HTML)
    # Your other screens use HTML; stick to HTML to avoid bold/underscore conflicts.
    text = (
        f"🧾 <b>Order Receipt #{order['id']}</b>\n"
        f"──────────────────────────\n"
        f"{eta_text}\n" if eta_text else ""       
        f"🏠 Pickup: {order.get('pickup','N/A')}\n"
        f"📍 Drop‑off: {dropoff}\n\n"
        f"🍴 Items:\n{items_str}\n\n"
        f"💰 Subtotal: {food_subtotal} birr\n"
        f"🚚 Delivery Fee: {delivery_fee} birr\n"
        f"──────────────────────────\n"
        f"💰 <b>Total:</b> {total_birr} birr\n"
        f"💳 Payment: {order.get('payment_method','N/A')} ({order.get('payment_status','N/A')})\n\n"
        f"⏱ Created: {created}  •  Accepted: {accepted}  •  Delivered: {delivered}\n\n"
        "✨ Thanks for ordering with UniBites Delivery!"
    )


    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ Back", callback_data=f"order:back:{order['id']}"),
                InlineKeyboardButton(text="🔁 Refresh", callback_data=f"order:refresh:{order['id']}")
            ],
        ]
    )

    # 9) Edit message first, fallback to answer
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

# import inspect
from aiogram.types import CallbackQuery
@router.callback_query(
    F.data.startswith("order:back:") |
    F.data.startswith("order:refresh:") |
    F.data.startswith("order:")
)
async def back_to_summary(callback: CallbackQuery):
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    # Safely parse order_id
    order_id = None
    if parts and parts[-1].isdigit():
        order_id = int(parts[-1])

    internal_user_id = await db.get_internal_user_id(callback.from_user.id)

    if action == "close":
        try:
            await send_orders_page(callback, user_id=internal_user_id, page=0)
        except Exception:
            try:
                await callback.message.edit_text("Closed.")
            except Exception:
                pass
        await callback.answer()
        return

    # Only render summary if we have a valid numeric id
    if order_id is not None:
        text, kb = await render_order_summary(order_id)
        if kb:
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
    # Acknowledge immediately to avoid timeout
    try:
        await callback.answer("⏳ Refreshing…")
    except Exception:
        pass

    # Then continue with your summary logic
    await back_to_summary(callback)



async def render_order_summary(order_id: int, tick: int = 0, paused: bool = False) -> tuple[str, InlineKeyboardMarkup]:
    order = await db.get_order(order_id)
    if not order:
        return "❌ Order not found.", None

    order = dict(order)
    status_text, stage = STATUS_MAP.get(order["status"], (order["status"], 1))

    # Vendor info
    vendor = await db.get_vendor(order["vendor_id"]) if order.get("vendor_id") else None
    vendor_name = vendor.get("name") if vendor else "Vendor"
    vendor_line = f"🏪 {vendor_name}"
    if vendor and vendor.get("rating_avg") is not None:
        vendor_line += f" • ★ {round(vendor.get('rating_avg',3.00),1)} ({vendor.get('rating_count',0)})"

    # DG info
    dg_text = ""
    has_dg = False
    dg_user_id = None
    dg_phone = None
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
            dg_text = f"🚴 {dg_name} ({dg_campus})"
            if dg_rating is not None:
                dg_text += f" • ★ {round(dg_rating,1)}"
            last_lat = last_lat or dg.get("last_lat")
            last_lon = last_lon or dg.get("last_lon")

    # breakdown / coords
    breakdown = {}
    drop_lat = drop_lon = None
    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
        drop_lat = breakdown.get("drop_lat")
        drop_lon = breakdown.get("drop_lon")
    except Exception:
        pass

    # ETA & map
    if last_lat and last_lon and drop_lat and drop_lon:
        try:
            eta_text, map_url = await _compute_eta_and_map(last_lat, last_lon, drop_lat, drop_lon)
        except Exception:
            eta_text, map_url = "⏱ ETA: unavailable", None
    else:
        eta_text = "⌛ Waiting for live location..." if has_dg else "⌛ Waiting for a delivery partner..."
        map_url = None

    # timestamps
    created = time_ago(order.get("created_at"))
    accepted = time_ago(order.get("accepted_at"))
    delivered = time_ago(order.get("delivered_at"))

    # items summary
    breakdown_items = breakdown.get("items") or []
    if isinstance(breakdown_items, list):
        names = [str(i) if isinstance(i, str) else i.get("name", "") for i in breakdown_items]
        counts = Counter(names)
        items_preview = "\n".join(
            f"• {name} x{count}" if count > 1 else f"• {name}"
            for name, count in list(counts.items())[:6]
        ) or "Items"
    else:
        items_preview = str(breakdown_items or "Items")

    text = (
        f"{animated_dot(stage, tick)} {status_text}\n\n"
        f"{render_progress(stage)}\n\n"
        f"{vendor_line}\n"
        f"{dg_text + chr(10) if dg_text else ''}"
        f"{eta_text + chr(10) if eta_text else ''}"
        f"\n🛒 Items:\n{items_preview}\n\n"
        f"💰 Total:{order.get('food_subtotal',0) + order.get('delivery_fee',0)} Birr\n"
        f"⏱ Created {created} • Accepted {accepted} \n • Delivered {delivered}\n\n"
        "✨ Hang tight — your campus meal is on its way!"
    )

    kb = build_track_keyboard(
        order["id"],
        has_dg=has_dg,
        dg_user_id=dg_user_id,
        dg_phone=dg_phone,
        is_ready=(order.get("status") == "ready"),
        map_url=map_url,
        paused=paused
    )

    return text, kb
