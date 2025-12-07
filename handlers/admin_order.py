# handlers/admin_orders.py
from collections import Counter
import json
import logging
import math
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

from aiogram import Router, F, Bot
import aiogram
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest

# --- Import Project Globals/Helpers ---
# Assuming these exist based on the prompt instructions.
from config import settings
from app_context import db
from utils.db_helpers import (
    notify_admin_log,
)
from utils.helpers import (assign_delivery_guy, time_ago)
# Assuming notify_student and safe_send are available in utils.helpers or similar
# If not, the user must implement them based on existing patterns.
from handlers.student_track_order import safe_send
# We need to import notify_student from the handler where it resides or a common util
from handlers.delivery_guy import notify_student 

# Configure Logger
log = logging.getLogger(__name__)

# Constants
ADMIN_GROUP_ID = settings.ADMIN_DAILY_GROUP_ID
PAGE_SIZE_ACTIVE = 6
PAGE_SIZE_PAST = 4

# Initialize Router
router = Router()

# -------------------------------------------------------------------------
# 1. Database Helper Wrappers (with TODOs for missing methods)
# -------------------------------------------------------------------------

async def _db_count_orders(filter_statuses: Optional[List[str]] = None, delivery_guy_null: Optional[bool] = None) -> int:
    """
    Counts orders based on status list and delivery_guy_id null status.
    """
    if hasattr(db, "count_orders"):
        return await db.count_orders(filter_statuses, delivery_guy_null)
    # Fallback/Mock for strictly following prompt if method missing (though prompt says include call)
    log.warning("db.count_orders missing. Returning 0.")
    return 0

async def _db_get_orders(filter_statuses: Optional[List[str]] = None, delivery_guy_null: Optional[bool] = None, limit: int = 10, offset: int = 0) -> List[Dict]:
    """
    Fetches orders list.
    
    """
    if hasattr(db, "get_orders"):
        return await db.get_orders(filter_statuses, delivery_guy_null, limit, offset)
    log.warning("db.get_orders missing. Returning [].")
    return []

async def _db_update_order_delivery_guy(order_id: int, delivery_guy_id: int, breakdown_json: Optional[str]) -> None:
    """
    Updates the assigned DG and breakdown info.

    """
    if hasattr(db, "update_order_delivery_guy"):
        await db.update_order_delivery_guy(order_id, delivery_guy_id, breakdown_json)
    else:
        # Fallback using raw execute if method missing but db.execute exists
        await db.execute(
            "UPDATE orders SET delivery_guy_id = $1, breakdown_json = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3",
            delivery_guy_id, breakdown_json, order_id
        )

async def _db_set_order_timestamp(order_id: int, field_name: str) -> None:
    """
    Sets a timestamp field to CURRENT_TIMESTAMP.

    """
    if hasattr(db, "set_order_timestamp"):
        await db.set_order_timestamp(order_id, field_name)
    else:
        # Fallback
        await db.execute(f"UPDATE orders SET {field_name} = CURRENT_TIMESTAMP WHERE id = $1", order_id)

async def _db_list_delivery_guys(limit: int, offset: int, active_only: bool = True) -> List[Dict]:
    """
    Lists delivery guys for manual assignment.

    """
    if hasattr(db, "list_delivery_guys"):
        return await db.list_delivery_guys(limit, offset, active_only)
    log.warning("db.list_delivery_guys missing. Returning [].")
    return []

# -------------------------------------------------------------------------
# 2. Pure Render Functions
# -------------------------------------------------------------------------
def render_admin_summary(counts: Dict[str, int]) -> Tuple[str, InlineKeyboardMarkup]:
    """Renders the top-level dashboard summary with emojis + color-coded UI."""
    
    text = (
        "📦 <b>Orders Dashboard</b>\n"
        "⚡️ Your command center — all order stats in one place.\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 <b>Active Orders:</b> {counts.get('active', 0)}\n"
        f"⏳ <b>Pending:</b> {counts.get('pending', 0)}\n"
        f"🍳 <b>Preparing:</b> {counts.get('preparing', 0)}\n"
        f"📍 <b>Ready:</b> {counts.get('ready', 0)}\n"
        f"🚫 <b>No Delivery Guy Assigned:</b> {counts.get('no_dg', 0)}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>Delivered:</b> {counts.get('delivered', 0)}\n"
        f"❌ <b>Cancelled:</b> {counts.get('cancelled', 0)}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📂 <i>Select a category below:</i>"
    )

    # 2-Column compact layout
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✨ Active Orders", callback_data="admin:orders:active"),
            InlineKeyboardButton(text="📜 Past Orders", callback_data="admin:orders:past")
        ],
        [
            InlineKeyboardButton(text="🔄 Refresh", callback_data="admin:orders:root")
        ]
    ])

    return text, kb

def render_orders_list(orders: List[Dict], page: int, total_count: int, page_size: int, filter_key: str, is_active_view: bool) -> Tuple[str, InlineKeyboardMarkup]:
    """Renders a paginated list of orders."""
    total_pages = max(1, math.ceil(total_count / page_size))
    
    # Title based on filter
    titles = {
        "pending": "🆕 Pending Orders",
        "preparing": "⚙️ Preparing Orders",
        "ready": "✅ Ready Orders",
        "no_dg": "🚴 No DG Assigned",
        "in_progress": "🚚 In Progress",
        "all": "📦 All Orders",
        "past": "🕑 Past Orders"
    }
    title = titles.get(filter_key, "📦 Orders")
    
    text_lines = [f"<b>{title}</b> (Page {page + 1}/{total_pages})", "──────────────────────────"]
    
    kb_rows = []
    
    if is_active_view:
        filter_row = [
            InlineKeyboardButton(
                text="🆕 Pending",
                callback_data="admin:orders:filter:pending:page:0"
            ),
            InlineKeyboardButton(
                text="⚙️ Preparing",
                callback_data="admin:orders:filter:preparing:page:0"
            ),
            InlineKeyboardButton(
                text="✅ Ready",
                callback_data="admin:orders:filter:ready:page:0"
            ),
            InlineKeyboardButton(
                text="🚴 No DG",
                callback_data="admin:orders:filter:no_dg:page:0"
            ),
            InlineKeyboardButton(
                text="🚚 In Progress",
                callback_data="admin:orders:filter:in_progress:page:0"
            ),
        ]
        kb_rows.append(filter_row)
 
    from utils.helpers import time_ago

    if not orders:
        text_lines.append("<i>📭 No orders found.</i>")
    else:
        order_buttons = []
        for order in orders:
            # Parse items for preview
            try:
                items = json.loads(order.get("items_json") or "[]")
                item_names = [i.get("name", "Item") if isinstance(i, dict) else str(i) for i in items]
                if len(item_names) > 2:
                    items_preview = f"{', '.join(item_names[:2])} +{len(item_names)-2}"
                else:
                    items_preview = ", ".join(item_names)
            except Exception:
                items_preview = "N/A"

            total = (order.get('food_subtotal') or 0) + (order.get('delivery_fee') or 0)
            created_at_dt = order.get('created_at')
            ready_at = order.get("ready_at") 
            ready_ago = time_ago(ready_at) if ready_at else None

            created_ago = time_ago(created_at_dt) if created_at_dt else "—"
            status = order.get('status', 'unknown').capitalize()
            delivered_at = order.get("delivered_at")
            delivered_ago = time_ago(delivered_at) if delivered_at else "_"
            time_line = f"⏰ Created: {created_ago}"
            if status.lower() == "ready" and ready_ago:
                time_line += f" | ✅ Ready: {ready_ago}"
            if status.lower() == 'delivered' and delivered_ago:
                time_line += f" | 🏁 Delivered: {time_ago(delivered_at)}"


            # One-line compact preview with emoji metrics
            line = f"📦 <b>Order #{order['id']}</b> — 📌{status}\n💵 {int(total)} birr | {time_line}\n"

            text_lines.append(line)

            # Button for viewing details
            order_buttons.append(
                InlineKeyboardButton(
                    text=f"👁 View #{order['id']}",
                    callback_data=f"admin:order:view:{order['id']}:page:{page}:filter:{filter_key}"
                )
            )

        # Group buttons into rows of 3
        for i in range(0, len(order_buttons), 3):
            kb_rows.append(order_buttons[i:i+3])

    # Pagination Row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"admin:orders:filter:{filter_key}:page:{page-1}"))
    
    nav_row.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="admin:orders:noop"))
    
    if (page + 1) * page_size < total_count:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"admin:orders:filter:{filter_key}:page:{page+1}"))
        
    kb_rows.append(nav_row)
    
    # Back to Root
    kb_rows.append([InlineKeyboardButton(text="🏠 Dashboard", callback_data="admin:orders:root")]) # Custom back logic handled by entry point re-call

    return "\n".join(text_lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)

def render_order_card(
    order: Dict,
    page: int,
    filter_key: str,
    vendor: Optional[Dict],
    customer: Optional[Dict],
    delivery_guy: Optional[Dict] = None,   # pass in DG info if available
) -> Tuple[str, InlineKeyboardMarkup]:
    """Renders the detailed view of a single order."""    
    
    # Data parsing
    order_id = order['id']
    status = order.get('status', 'unknown')
    total = (order.get('food_subtotal') or 0) + (order.get('delivery_fee') or 0)
    pickup = order.get('pickup', 'N/A')
    dropoff = order.get('dropoff', 'N/A')
    
    customer_name = customer.get('first_name', 'Unknown') if customer else 'Unknown'
    customer_phone = customer.get('phone', 'N/A') if customer else 'N/A'
    vendor_name = vendor.get('name', 'Unknown') if vendor else 'Unknown'
    
    try:
        items = json.loads(order.get("items_json") or "[]")
        item_lines = []
        from collections import Counter
        # Extract names safely
        names = [i.get("name", "Item") if isinstance(i, dict) else str(i) for i in items]
        counts = Counter(names)
        for name, count in counts.items():
            if count > 1:
                item_lines.append(f" • {name} x{count}")
            else:
                item_lines.append(f" • {name}")
        items_str = "\n".join(item_lines)
    except:
        items_str = " • Error parsing items"

    dg_info = ""
    if order.get('delivery_guy_id'):
        if delivery_guy:
            dg_name = delivery_guy.get("name", "Unknown")
            dg_phone = delivery_guy.get("phone", "N/A")
            dg_campus = delivery_guy.get("campus", "N/A")
            dg_info = f"\n👤 <b>Delivery Guy:</b> {dg_name} ({dg_phone}, {dg_campus})"
        else:
            dg_info = f"\n👤 <b>Delivery Guy ID:</b> {order['delivery_guy_id']}"
    
    created_ago = time_ago(order.get("created_at"))
    ready_at = order.get("ready_at")
    ready_ago = time_ago(ready_at) if ready_at else None

    status_emojis = {
    "pending": "🆕",
    "preparing": "⚙️",
    "ready": "✅",
    "in_progress": "🚚",
    "delivered": "🏁",
    "cancelled": "❌"
    }
    status_key = order.get("status", "unknown").lower()
    status_display = status_emojis.get(status_key, "📌") + f" <b>{order.get('status', 'Unknown').capitalize()}</b>"

    # Time line
    time_line = f"⏰ Created: {created_ago}"
    if status_key == "ready" and ready_ago:
        time_line += f" | ✅ Ready: {ready_ago}"

    text = (
        f"🔥 <b>ORDER #{order_id}</b>\n"
        "╔══════════════════════════╗\n"
        f"📡 <b>Status:</b> {status_display}{dg_info}\n"
        "╠══════════════════════════╣\n"
        f"👤 <b>Customer:</b> {customer_name} ({customer_phone})\n"
        f"🏪 <b>Vendor:</b> {vendor_name}\n"
        "╠══════════════════════════╣\n"
        f"📍 <b>Pickup:</b> {pickup}\n"
        f"🎯 <b>Dropoff:</b> {dropoff}\n"
        "╠══════════════════════════╣\n"
        "🍽️ <b>Items:</b>\n"
        f"{items_str}\n"
        "╠══════════════════════════╣\n"
        f"💰 <b>Total:</b> <b>{int(total)} birr</b>\n"
        "╠══════════════════════════╣\n"
        f"🕒 {time_line}\n"

    )


    # Actions based on status
    kb_rows = []
    
    base_cb = f":{order_id}:page:{page}:filter:{filter_key}"
    
    if status == 'pending':
        kb_rows.append([
            InlineKeyboardButton(text="✅ Accept", callback_data=f"admin:order:accept{base_cb}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"admin:order:cancel{base_cb}")
        ])
    elif status == 'preparing':
        kb_rows.append([
            InlineKeyboardButton(text="✅ Mark Ready", callback_data=f"admin:order:ready{base_cb}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"admin:order:cancel{base_cb}")
        ])
    elif status == 'ready':
        kb_rows.append([
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"admin:order:cancel{base_cb}")
        ])
    elif status == 'in_progress':
        kb_rows.append([
            InlineKeyboardButton(text="📦 Mark Delivered", callback_data=f"admin:order:delivered{base_cb}")
        ])
    elif status == 'delivered':
        kb_rows.append([
            InlineKeyboardButton(text="🧾 View Receipt", callback_data=f"admin:order:receipt:{order_id}")
        ])
        
    # Assignment Rows
    if status in ['preparing', 'ready', 'assigned', 'in_progress']:
        assign_label = "👤 Reassign DG" if order.get('delivery_guy_id') else "👤 Assign DG (Manual)"
        kb_rows.append([
            InlineKeyboardButton(text=assign_label, callback_data=f"admin:order:assign_open{base_cb}")
        ])

    # Back Button
    kb_rows.append([
        InlineKeyboardButton(text="⬅️ Back to List", callback_data=f"admin:orders:filter:{filter_key}:page:{page}")
    ])
    
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


def render_dg_list(
    candidates: List[Dict],
    order_id: int,
    page: int,
    total_count: int,
    page_size: int,
    parent_page: int,
    filter_key: str
) -> Tuple[str, InlineKeyboardMarkup]:
    """Renders a paginated Delivery Guy list for assignment with hype neon UI."""
    
    total_pages = max(1, math.ceil(total_count / page_size))

    # 🔥 Neon header
    text = (
        f"⚡️ <b>Assign Delivery Guy</b>\n"
        f"📦 Order <b>#{order_id}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📄 Page <b>{page + 1}</b> / {total_pages}\n"
        "🛵 Choose the best Delivery Guy below:\n"
    )

    kb_rows = []

    # 🔥 Delivery Guy rows
    for dg in candidates:
        name = dg.get("name", "Unknown")
        campus = dg.get("campus", "N/A")
        active_orders = dg.get("accepted_requests", 0)

        btn_text = (
            f"🛵 {name} • {campus} | 📊 Active: {active_orders}"
        )

        kb_rows.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=(
                    f"admin:order:assign_confirm:{order_id}:dg:{dg['id']}:page:{parent_page}:filter:{filter_key}"
                )
            )
        ])

    # 🔥 Pagination + Cancel
    nav_row = []

    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️ Prev",
                callback_data=f"admin:dg:list:{order_id}:page:{page-1}"
            )
        )

    nav_row.append(
        InlineKeyboardButton(
            text="❌ Cancel",
            callback_data=f"admin:order:view:{order_id}:page:{parent_page}:filter:{filter_key}"
        )
    )

    if (page + 1) * page_size < total_count:
        nav_row.append(
            InlineKeyboardButton(
                text="Next ➡️",
                callback_data=f"admin:dg:list:{order_id}:page:{page+1}"
            )
        )

    kb_rows.append(nav_row)

    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)

# -------------------------------------------------------------------------
# 3. Handlers
# -------------------------------------------------------------------------

@router.message(F.text == "📦 Orders")
@router.callback_query(F.data == "admin:orders:root")
async def admin_orders_menu(event: Message | CallbackQuery):
    """Top-level entry point for Admin Orders Dashboard."""
    is_callback = isinstance(event, CallbackQuery)
    message = event.message if is_callback else event
    user = event.from_user
    
    # 1. Fetch Counts
    try:
        active_count = await _db_count_orders(["pending", "assigned", "preparing", "ready", "in_progress"])
        pending_count = await _db_count_orders(["pending"])
        preparing_count = await _db_count_orders(["preparing"])
        ready_count = await _db_count_orders(["ready"])
        # No DG Assigned = Active AND delivery_guy_id IS NULL
        no_dg_count = await _db_count_orders(["assigned", "preparing", "ready"], delivery_guy_null=True)
        delivered_count = await _db_count_orders(["delivered"])
        cancelled_count = await _db_count_orders(["cancelled"])
        
        counts = {
            "active": active_count,
            "pending": pending_count,
            "preparing": preparing_count,
            "ready": ready_count,
            "no_dg": no_dg_count,
            "delivered": delivered_count,
            "cancelled": cancelled_count
        }
    except Exception as e:
        log.exception(f"Error fetching order counts: {e}")
        counts = {}

        # 2. Render
    text, kb = render_admin_summary(counts)

    if is_callback:
        try:
            await message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            await event.answer("🔄 Refreshed")
        except aiogram.exceptions.TelegramBadRequest as e:
            if "message is not modified" in str(e):
                # Show a popup instead of crashing
                await event.answer("✅ Already up to date")
            else:
                raise
    else:
        await message.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "admin:orders:active")
async def admin_active_root(cb: CallbackQuery):
    """Entry point for Active Orders (defaults to 'all' active filter)."""
    # Redirect to the main filter with page 0
    await admin_orders_filter_handler(cb, "all", 0)


@router.callback_query(F.data == "admin:orders:past")
async def admin_past_root(cb: CallbackQuery):
    """Entry point for Past Orders."""
    await admin_orders_filter_handler(cb, "past", 0)


@router.callback_query(F.data.startswith("admin:orders:filter:"))
async def admin_orders_filter(cb: CallbackQuery):
    """Handles pagination and filtering."""
    # admin:orders:filter:<filter_key>:page:<n>
    data_parts = cb.data.split(":")
    filter_key = data_parts[3]
    page = int(data_parts[5])
    
    await admin_orders_filter_handler(cb, filter_key, page)

async def admin_orders_filter_handler(cb: CallbackQuery, filter_key: str, page: int):
    """Core logic for fetching and rendering order lists."""
    try:
        page_size = PAGE_SIZE_ACTIVE if filter_key != "past" else PAGE_SIZE_PAST
        offset = page * page_size
        
        # Determine DB query params based on filter_key
        statuses = None
        dg_null = None
        
        if filter_key == "pending":
            statuses = ["pending"]
        elif filter_key == "preparing":
            statuses = ["preparing"]
        elif filter_key == "ready":
            statuses = ["ready"]
        elif filter_key == "no_dg":
            # Active statuses but no DG
            statuses = ["assigned", "preparing", "ready"]
            dg_null = True
        elif filter_key == "in_progress":
            statuses = ["in_progress"]
        elif filter_key == "past":
            statuses = ["delivered", "cancelled"]
        elif filter_key == "all":
            # General active view
            statuses = ["pending", "assigned", "preparing", "ready", "in_progress"]
            
        # Fetch
        orders = await _db_get_orders(statuses, dg_null, page_size, offset)
        total = await _db_count_orders(statuses, dg_null)
        
        is_active_view = (filter_key != "past")
        
        text, kb = render_orders_list(orders, page, total, page_size, filter_key, is_active_view)
        
        # Edit message
        # Use try/except for message not modified errors
        try:
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except TelegramBadRequest:
            pass # Ignore if identical
            
    except Exception as e:
        log.exception(f"Error in admin_orders_filter: {e}")
        await cb.answer("⚠️ Action failed. Check logs.", show_alert=True)
    
    await cb.answer()


@router.callback_query(F.data.startswith("admin:order:view:"))
async def admin_order_view(cb: CallbackQuery):
    """Detailed view of a single order."""
    # admin:order:view:<order_id>:page:<page>:filter:<filter_key>
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    filter_key = parts[7]
    
    try:
        order = await db.get_order(order_id)
        if not order:
            await cb.answer("Order not found.", show_alert=True)
            # Refresh list
            await admin_orders_filter_handler(cb, filter_key, page)
            return

        vendor = await db.get_vendor(order.get('vendor_id'))
        # user_id in orders table is internal ID, need to fetch user row
        customer = await db.get_user_by_id(order.get('user_id'))
        delivery_guy = None
        if order.get("delivery_guy_id"):
            delivery_guy = await db.get_delivery_guy(order["delivery_guy_id"])
         
        text, kb = render_order_card(order, page, filter_key, vendor, customer, delivery_guy)
        
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        
    except Exception as e:
        log.exception(f"Error rendering order card: {e}")
        await cb.answer("⚠️ Error loading order.", show_alert=True)


# -------------------------------------------------------------------------
# 4. Action Handlers
# -------------------------------------------------------------------------

async def _notify_admin_action(bot, user, action_text: str, order_id: int):
    """Helper to log actions."""
    msg = f"[ADMIN:{user.id}/{user.username or 'N/A'}] {action_text} Order #{order_id}"
    await notify_admin_log(bot, ADMIN_GROUP_ID, msg)
    
@router.callback_query(F.data.startswith("admin:order:accept"))
async def action_accept(cb: CallbackQuery):
    # admin:order:accept:<order_id>:page:<page>:filter:<filter_key>
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    filter_key = parts[7]

    try:
        order = await db.get_order(order_id)
        if not order or order['status'] != 'pending':
            await cb.answer("Order state changed or not found.", show_alert=True)
            await admin_orders_filter_handler(cb, filter_key, page)
            return

        # 1. Update Status
        await db.update_order_status(order_id, "preparing")
        await _db_set_order_timestamp(order_id, "accepted_at")

        # 2. Attempt to assign DG
        chosen = await assign_delivery_guy(db, order_id, bot=cb.bot, notify_student=False)

        # Refetch order to get updated DG info
        order = await db.get_order(order_id)

        # 3. Notify student
        student_chat_id = await db.get_student_chat_id(order)
        vendor = await db.get_vendor(order["vendor_id"])
        vendor_name = vendor["name"] if vendor else "Vendor"

        final_preview = (
            f"🎉 *Order #{order_id} Confirmed by Admin!* \n"
            "──────────────────────\n"
            "👨‍🍳 Your meal is now being prepared...\n"
        )

        if chosen:
            # DG assigned
            final_preview += (
                f"🚴 Delivery partner: {chosen['name']} ({chosen['campus']})\n\n"
                "🧭 Track every step in *📍 Track Order*."
            )
        else:
            # No DG yet
            final_preview += (
                "🚴 A delivery partner will be assigned soon.\n"
            )

        preview_kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📍 Track", callback_data=f"order:track:{order_id}")]]
        )

        if student_chat_id:
            await safe_send(cb.bot, student_chat_id, final_preview, reply_markup=preview_kb)

        # 4. Admin log
        if ADMIN_GROUP_ID:
            if chosen:
                admin_msg = (
                    f"✅ Admin {cb.from_user.username} accepted Order #{order_id}\n"
                    f"Start the assigning from --👤 Delivery Guy: {chosen['name']} ({chosen['campus']})"
                )
            else:
                admin_msg = (
                    f"⚠️ Admin {cb.from_user.username} accepted Order #{order_id}, "
                    "but no delivery guy was assigned."
                )
            await notify_admin_log(cb.bot, ADMIN_GROUP_ID, admin_msg)

        await cb.answer("Order accepted.")
        await admin_orders_filter_handler(cb, filter_key, page)

    except Exception as e:
        log.exception(f"Accept failed: {e}")
        await cb.answer("⚠️ Action failed.", show_alert=True)
        await admin_orders_filter_handler(cb, filter_key, page)

@router.callback_query(F.data.startswith("admin:order:cancel"))
async def action_cancel(cb: CallbackQuery):
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    filter_key = parts[7]

    try:
        order = await db.get_order(order_id)
        if not order:
             await cb.answer("Order not found.", show_alert=True)
             await admin_orders_filter_handler(cb, filter_key, page)
             return

        # 1. DB Update
        await db.update_order_status(order_id, "cancelled")
        
        # 2. Notify Student
        # Using a generic status or custom message logic based on existing notify_student capabilities
        # If 'cancelled' isn't handled by notify_student, we might need a direct message.
        # Assuming notify_student handles 'cancelled' status mapping.
        await notify_student(cb.bot, order, status="cancelled")
        
        # 3. Notify DG if assigned
        if order.get('delivery_guy_id'):
            dg = await db.get_delivery_guy(order['delivery_guy_id'])
            if dg:
                await safe_send(cb.bot, dg['telegram_id'], f"⚠️ Order #{order_id} has been CANCELLED by admin.")

        # 4. Log
        await _notify_admin_action(cb.bot, cb.from_user, "Cancelled", order_id)
        await cb.answer("Order Cancelled.")
        await admin_orders_filter_handler(cb, filter_key, page)

    except Exception as e:
        log.exception(f"Cancel failed: {e}")
        await cb.answer("⚠️ Action failed.", show_alert=True)
        await admin_orders_filter_handler(cb, filter_key, page)
@router.callback_query(F.data.startswith("admin:order:ready"))
async def action_ready(cb: CallbackQuery):
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    filter_key = parts[7]

    try:
        order = await db.get_order(order_id)
        if not order or order['status'] not in ('preparing', 'assigned'):
            await cb.answer("Order state invalid.", show_alert=True)
            await admin_orders_filter_handler(cb, filter_key, page)
            return

        # 1. DB Update
        await db.update_order_status(order_id, "ready")
        await _db_set_order_timestamp(order_id, "ready_at")

        # 2. Notify DG (cinematic, with items + buttons)
        if order.get("delivery_guy_id"):
            dg = await db.get_delivery_guy(order["delivery_guy_id"])
            if dg:
                vendor = await db.get_vendor(order["vendor_id"])
                vendor_name = vendor["name"] if vendor else "Vendor"
                pickup = order.get("pickup") or "Vendor location"
                dropoff = order.get("dropoff") or "Student location"

                try:
                    items = json.loads(order.get("items_json", "[]")) or []
                    names = [i.get("name", "") if isinstance(i, dict) else str(i) for i in items]
                    counts = Counter(names)
                    items_str = ", ".join(
                        f"{name} x{count}" if count > 1 else name
                        for name, count in counts.items()
                    )
                except Exception:
                    items_str = "Items"

                total_food = order.get("food_subtotal", 0)
                delivery_fee = order.get("delivery_fee", 0)

                dg_msg = (
                    f"🚨 NEW PICKUP ALERT 🚨\n\n"
                    f"📦 Order #{order_id} is READY\n"
                    f"🏪 Vendor: {vendor_name}\n"
                    f"📍 Pickup: {pickup}\n"
                    f"🎯 Dropoff: {dropoff}\n"
                    f"🛒 Items: {items_str}\n"
                    f"💵 Total: {total_food} Birr + Delivery Fee: {delivery_fee} Birr\n\n"
                    f"👉 GO NOW to collect this order."
                )

                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="▶️ Start Delivery", callback_data=f"start_order_{order_id}")],
                        [InlineKeyboardButton(text="💬 Contact User", callback_data=f"contact_user_{order_id}")]
                    ]
                )

                await safe_send(cb.bot, dg["telegram_id"], dg_msg, reply_markup=kb)

        else:
            vendor = await db.get_vendor(order["vendor_id"])
            vendor_name = vendor["name"] if vendor else "Vendor"
            await notify_admin_log(cb.bot, ADMIN_GROUP_ID,
                                   f"⚠️ Order #{order_id} from {vendor_name} is ready but no DG assigned.")

        # 3. Notify Student
        student_chat_id = await db.get_student_chat_id(order)
        if student_chat_id:
            from handlers.student_track_order import notify_student
            await notify_student(cb.bot, student_chat_id, order_id)

        # 4. Admin log
        vendor = await db.get_vendor(order["vendor_id"])
        vendor_name = vendor["name"] if vendor else "Vendor"
        await notify_admin_log(cb.bot, ADMIN_GROUP_ID,
                               f"✅ Admin {cb.from_user.username} marked Order #{order_id} as READY.")

        await cb.answer("Order marked Ready.")
        await admin_orders_filter_handler(cb, filter_key, page)

    except Exception as e:
        log.exception(f"Ready action failed: {e}")
        await cb.answer("⚠️ Action failed.", show_alert=True)
        await admin_orders_filter_handler(cb, filter_key, page)


@router.callback_query(F.data.startswith("admin:order:delivered"))
async def action_delivered(cb: CallbackQuery):
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    filter_key = parts[7]

    try:
        # 1. DB Update
        await db.update_order_status(order_id, "delivered")
        await _db_set_order_timestamp(order_id, "delivered_at")

        order = await db.get_order(order_id)

        # 2. Notify Student
        await notify_student(cb.bot, order, status="delivered")

        # 3. Log
        await _notify_admin_action(cb.bot, cb.from_user, "Marked Delivered", order_id)
        await cb.answer("Order marked Delivered.")
        await admin_orders_filter_handler(cb, filter_key, page)

    except Exception as e:
        log.exception(f"Delivered action failed: {e}")
        await cb.answer("⚠️ Action failed.", show_alert=True)
        await admin_orders_filter_handler(cb, filter_key, page)


# -------------------------------------------------------------------------
# 5. Manual DG Assignment Flow
# -------------------------------------------------------------------------

@router.callback_query(F.data.startswith("admin:order:assign_open"))
async def action_assign_open(cb: CallbackQuery):
    """Opens the DG selection list."""
    # admin:order:assign_open:<order_id>:page:<page>:filter:<filter_key>
    parts = cb.data.split(":")
    order_id = int(parts[3])
    parent_page = int(parts[5])
    filter_key = parts[7]
    
    # Store context in the DG list pagination callback so we can return
    # admin:dg:list:<order_id>:page:<page> (We lose filter key here to keep it short? 
    # Or we can stash state. For simplicity, we assume page 0 of DG list)
    
    # Actually, we need to preserve context. Let's use a temporary in-memory way or just encode it?
    # The DG list callback doesn't have room for everything. 
    # We will modify the DG list renderer to accept context or just use session.
    # For this task, we will try to make the list handler smart or reset to page 0.
    
    # Let's start the DG list at page 0.
    await render_dg_list_handler(cb, order_id, 0, parent_page, filter_key)


async def render_dg_list_handler(cb: CallbackQuery, order_id: int, dg_page: int, parent_page: int, filter_key: str):
    try:
        page_size = 5
        offset = dg_page * page_size

        candidates = await _db_list_delivery_guys(page_size, offset, active_only=True)

        # Use real count if available
        total_count = await db.count_active_delivery_guys()

        text, kb = render_dg_list(
            candidates,
            order_id,
            dg_page,
            total_count,
            page_size,
            parent_page,
            filter_key
        )

        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

    except Exception as e:
        log.exception(f"Assign open failed: {e}")
        await cb.answer("⚠️ Failed to load DG list.", show_alert=True)


@router.callback_query(F.data.startswith("admin:dg:list:"))
async def action_dg_list_paginate(cb: CallbackQuery):
    # admin:dg:list:<order_id>:page:<page>
    # Note: We lost the parent filter/page context in the button data to save space.
    # This implies "Back/Cancel" might reset to Active root or we need to infer.
    # To fix this within constraints, let's assume default return context or try to parse if we added it.
    
    # Actually, let's keep it simple: If pagination happens, we might lose precise parent context 
    # unless we encode it. Let's assume we return to "all" page 0 on cancel if context lost.
    
    parts = cb.data.split(":")
    order_id = int(parts[3])
    page = int(parts[5])
    
    # Context fallback
    parent_page = 0
    filter_key = "all"
    
    await render_dg_list_handler(cb, order_id, page, parent_page, filter_key)


@router.callback_query(F.data.startswith("admin:order:assign_confirm"))
async def action_assign_confirm(cb: CallbackQuery):
    # admin:order:assign_confirm:<order_id>:dg:<dg_id>:page:<page>:filter:<filter_key>
    parts = cb.data.split(":")
    order_id = int(parts[3])
    dg_id = int(parts[5])
    page = int(parts[7])
    filter_key = parts[9]
    
    try:
        order = await db.get_order(order_id)
        if not order:
             await cb.answer("Order not found.", show_alert=True)
             await admin_orders_filter_handler(cb, filter_key, page)
             return

        previous_dg_id = order.get('delivery_guy_id')

        # 1. Prepare JSON Log
        admin_meta = {
            "admin_id": cb.from_user.id,
            "admin_username": cb.from_user.username or "unknown",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Load existing breakdown or new
        try:
            breakdown = json.loads(order.get('breakdown_json') or "{}")
        except:
            breakdown = {}
            
        breakdown['manually_assigned_by_admin'] = admin_meta
        new_breakdown_json = json.dumps(breakdown)
        
        # 2. Update DB
        # Do not change status, only delivery_guy_id
        await _db_update_order_delivery_guy(order_id, dg_id, new_breakdown_json)
        
        # 3. Notify Student
        # Reload order to reflect changes
        order['delivery_guy_id'] = dg_id 
        # Need DG details for notification (name, etc.)
        dg = await db.get_delivery_guy(dg_id)
        if dg:
            order['delivery_guy_name'] = dg.get('name')
            order['campus'] = dg.get('campus')
            
        await notify_student(cb.bot, order, status="assigned")
        
        # 4. Notify New DG
        try:
                items = json.loads(order.get("items_json", "[]")) or []
                names = [i.get("name", "") if isinstance(i, dict) else str(i) for i in items]
                from collections import Counter
                counts = Counter(names)
                items_str = ", ".join(
                    f"{name} x{count}" if count > 1 else name
                    for name, count in counts.items()
                )
        except Exception:
                items_str = "Items unavailable"
        
        from handlers.delivery_guy import STATUS_LABELS
        status_label = STATUS_LABELS.get(order.get("status"), "ℹ️ Unknown status")


        if dg:  
            msg = (
            f"📦 Order #{order_id}\n"
            f"📌 Status: {order.get('status')}\n\n"
            "──────────────────────\n"
            f"🏠 Pickup: {order.get('pickup')}\n"
            f"📍 Drop-off: {order.get('dropoff')}\n"
            f"💰 Subtotal Fee: {order.get('food_subtotal')} birr\n"
            f"🚚 Delivery fee: {order.get('delivery_fee')} birr\n"
            "──────────────────────\n"
            f"💵 Total Payable: {order.get('food_subtotal') + order.get('delivery_fee')} birr\n\n"
            f"🛒 Items: {items_str}\n\n"
            "⚡ Manage this order below."
        )
          
            
            await safe_send(cb.bot, dg['telegram_id'], msg)
        # 5. Notify Previous DG (if reassign)
        if previous_dg_id and previous_dg_id != dg_id:
            prev_dg = await db.get_delivery_guy(previous_dg_id)
            if prev_dg:
                await safe_send(cb.bot, prev_dg['telegram_id'], f"⚠️ Order #{order_id} has been reassigned to another partner.")

        # 6. Log
        log_text = f"Manually Assigned DG {dg_id}"
        if previous_dg_id:
            log_text += f" (was {previous_dg_id})"
        await _notify_admin_action(cb.bot, cb.from_user, log_text, order_id)
        
        await cb.answer(f"DG Assigned: {dg.get('name', 'ID ' + str(dg_id))}")
        await admin_orders_filter_handler(cb, filter_key, page)

    except Exception as e:
        log.exception(f"Manual assignment failed: {e}")
        await cb.answer("⚠️ Action failed.", show_alert=True)
        await admin_orders_filter_handler(cb, filter_key, page)


# -------------------------------------------------------------------------
# 6. Receipt View
# -------------------------------------------------------------------------

@router.callback_query(F.data.startswith("admin:order:receipt:"))
async def admin_order_receipt(cb: CallbackQuery):
    # admin:order:receipt:<order_id>
    parts = cb.data.split(":")
    order_id = int(parts[3])
    
    try:
        order = await db.get_order(order_id)
        if not order:
            await cb.answer("Order not found.", show_alert=True)
            return

        # Render Receipt
        lines = [f"🧾 <b>Receipt — Order #{order_id}</b>", "──────────────────────────"]
        
        # Items
        try:
            items = json.loads(order.get("items_json") or "[]")
            if items:
                for item in items:
                    name = item.get("name", "Item") if isinstance(item, dict) else str(item)
                    price = item.get("price", 0) if isinstance(item, dict) else 0
                    lines.append(f"🍽 {name} — 💵 {price} birr")
            else:
                lines.append("• Items unavailable")
        except Exception:
            lines.append("• Items unavailable")
            
        # Totals
        subtotal = order.get("food_subtotal", 0)
        fee = order.get("delivery_fee", 0)
        total = subtotal + fee
        
        lines.append("──────────────────────────")
        lines.append(f"💰 Subtotal: {subtotal} birr")
        lines.append(f"🚚 Delivery Fee: {fee} birr")
        lines.append(f"🏆 <b>Total: {total} birr</b>")
        lines.append("⭐ Rating: Not rated yet")
        lines.append("──────────────────────────")
        
        # Timestamps
        created_at = order.get("created_at")
        ready_at = order.get("ready_at")
        delivered_at = order.get("delivered_at")
        
        lines.append(f"⏰ Created: {time_ago(created_at)}")
        if ready_at:
            lines.append(f"✅ Ready: {time_ago(ready_at)}")
        if delivered_at:
            lines.append(f"🏁 Delivered: {time_ago(delivered_at)}")
        
        text = "\n".join(lines)
        
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Close", callback_data="admin:orders:noop")]
            ]
        )
        
        await cb.message.answer(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer()
        
    except Exception as e:
        log.exception(f"Receipt view failed: {e}")
        await cb.answer("⚠️ Failed to view receipt.", show_alert=True)
        
        
@router.callback_query(F.data == "admin:orders:noop")
async def noop_handler(cb: CallbackQuery):
    await cb.answer()