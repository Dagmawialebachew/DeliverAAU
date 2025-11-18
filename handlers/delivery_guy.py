# handlers/delivery_guy.py - REVISED IMPORTS
"""
FINALIZED SYSTEM BLUEPRINT IMPLEMENTATION — "Delivery Partner 2030 System"

REVISION:
1. Dashboard/Menu Navigation uses ReplyKeyboardMarkup.
2. Order Flow uses InlineKeyboardMarkup.
3. Database helper functions are now imported from utils.db_helpers.
"""

import asyncio
import contextlib
import json
import logging
import math
from typing import Optional, Tuple, Dict, Any, List
import aiosqlite
from datetime import datetime
from aiogram import Bot
from aiogram import Router, F, types
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.exceptions import TelegramBadRequest
from utils.globals import PENDING_OFFERS, EXPIRY_SECONDS # NEW IMPORT
from database.db import Database
from config import settings

# ----------------------------------------------------
# ⬇️ UPDATED IMPORTS from newly created utils/db_helpers.py
# ----------------------------------------------------
from utils.db_helpers import (
    increment_skip, 
    record_daily_stat,
    notify_admin, # Not used here, but available
    calc_acceptance_rate,
    get_all_active_orders_for_dg,
    get_latest_active_order_for_dg,
    check_thresholds_and_notify,
    add_dg_to_blacklist
)

# Router + DB
router = Router()
db = Database(settings.DB_PATH)
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --------------------------
# Feature toggles / constants
# --------------------------
ENABLE_XP = True
XP_PER_DELIVERY = 10
ENABLE_COINS = True
COIN_RATIO = 0.05
ADMIN_GROUP_ID = settings.ADMIN_GROUP_ID 

# Callback Prefix Constants
CB_PREFIX_DASHBOARD = "dash"
CB_PREFIX_ORDER = "order"
CB_PREFIX_EARNINGS = "earn"
CB_PREFIX_PERFORMANCE = "perf"

# --------------------------
# DB helper shims (REMOVED - now using utils/db_helpers)
# --------------------------

async def _db_get_delivery_guy_by_user(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    """Try a few DB helper names; return a delivery_guy row dict or None."""
    try:
        # Assuming Database class has a method `get_delivery_guy_by_user`
        if hasattr(db, "get_delivery_guy_by_user"):
            return await db.get_delivery_guy_by_user(telegram_user_id)
        
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT * FROM delivery_guys WHERE user_id = ? LIMIT 1", (telegram_user_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
    except Exception:
        log.exception("_db_get_delivery_guy_by_user failed for %s", telegram_user_id)
        return None

async def _db_update_delivery_guy_coords(dg_id: int, lat: float, lon: float):
    """Call your DB helper if available, otherwise perform raw SQL update."""
    try:
        if hasattr(db, "update_delivery_guy_coords"):
            await db.update_delivery_guy_coords(dg_id, lat, lon) 
            return
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET last_lat = ?, last_lon = ?, last_online_at = CURRENT_TIMESTAMP WHERE id = ?", 
                (lat, lon, dg_id)
            )
            await conn.commit()
    except Exception:
        log.exception("_db_update_delivery_guy_coords failed for dg_id=%s", dg_id)

# get_all_active_orders_for_dg now uses the imported helper:
# from utils.db_helpers import get_all_active_orders_for_dg

# --------------------------
# UI / Keyboards (Revised for ReplyKeyboardMarkup Dashboard)
# --------------------------

# --- REPLY KEYBOARD ---
def dashboard_reply_keyboard(is_online: bool = False) -> ReplyKeyboardMarkup:
    """Dashboard menu (Section 4) — ReplyKeyboardMarkup."""
    status_label = "🔴 Go Offline" if is_online else "🟢 Go Online"

    keyboard = [
        [
            KeyboardButton(text="📦 My Orders"), 
            KeyboardButton(text="💰 Earnings"),
        ],
        [
            KeyboardButton(text="📊 Performance"), 
            KeyboardButton(text=status_label),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def menu_back_keyboard() -> ReplyKeyboardMarkup:
    """Standard back button for sub-menus."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Back to Dashboard")]
        ],
        resize_keyboard=True
    )

def location_request_keyboard() -> ReplyKeyboardMarkup:
    """Temporary keyboard to request location."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Share My Location (Required)", request_location=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

# --- INLINE KEYBOARD (For Orders ONLY) ---

def order_offer_keyboard(order_id: int, expiry_min: int) -> InlineKeyboardMarkup:
    """New Delivery Offer keyboard (Section 3)."""
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Accept", callback_data=f"accept_order_{order_id}"),
            InlineKeyboardButton(text="❌ Skip", callback_data=f"skip_order_{order_id}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def accepted_order_actions(order_id: int, status: str) -> InlineKeyboardMarkup:
    """Accepted Order Inline actions (Section 3)."""
    if status == "accepted":
        # Initial status after acceptance, ready to start/pickup
        buttons = [
            InlineKeyboardButton(text="▶️ Start Delivery", callback_data=f"start_order_{order_id}")
        ]
    elif status == "in_progress":
        # On the way status
        buttons = [
            InlineKeyboardButton(text="📍 Live Update", callback_data=f"update_location_{order_id}"),
            InlineKeyboardButton(text="📦 Mark Delivered", callback_data=f"delivered_{order_id}")
        ]
    else: 
        buttons = []

    action_row = [
        InlineKeyboardButton(text="💬 Contact User", callback_data=f"contact_user_{order_id}")
    ]

    return InlineKeyboardMarkup(inline_keyboard=[buttons, action_row])


# --------------------------
# Menu / Entrypoint (Now uses ReplyKeyboard for main navigation)
# --------------------------

@router.message(F.text == "/dashboard")
@router.message(F.text == "🏠 Back to Dashboard") # New entry point from sub-menus
async def show_dashboard(message: Message):
    """Entry point for the Dashboard (Section 4) using ReplyKeyboard."""
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg:
        # Clear any lingering ReplyKeyboardMarkup by sending a message with ReplyKeyboardRemove
        await message.answer("⚠️ You are not registered as a delivery partner.", reply_markup=ReplyKeyboardRemove())
        return

    await _send_dashboard_view(message.bot, dg["user_id"], dg)
    log.info("Delivery dashboard shown to dg id=%s tg=%s", dg.get("id"), dg.get("user_id"))


def reliability_badge(rate: float) -> str:
    score = int(rate)
    if score >= 80:
        return f"🥇 High ({score}%)"
    elif score >= 50:
        return f"⚖️ Medium ({score}%)"
    else:
        return f"⚠️ Low ({score}%)"

async def _send_dashboard_view(bot: Bot, user_id: int, dg: Dict[str, Any]):
    """Generates and sends the main dashboard view with ReplyKeyboard."""
    
    if dg.get("blocked", 0) == 1:
        await bot.send_message(user_id, "🛑 **Your account is currently blocked** due to reliability issues. Please contact admin.", reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown")
        return

    is_online = bool(dg.get("active", 0))
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db.db_path, dg["id"])
    reliability_score = reliability_badge(int(acceptance_rate))
    progress_bar = "▰" * int((dg.get('xp') % 100) / 10) + "▱" * (10 - int((dg.get('xp') % 100) / 10))

    
    dashboard_text = (
        "🆔 **Delivery Partner Card**\n"
        f"👤 {dg.get('name', 'N/A')}\n"
        f"🏛 {dg.get('campus', 'N/A')}\n"
        f"⚡ Status: {'🟢 Online' if is_online else '🔴 Offline'}\n"
        f"📦 Deliveries: {dg.get('total_deliveries', 0)}\n\n"
        f"💰 Coins: {dg.get('coins', 0)} • 🏆 XP: {dg.get('xp', 0)} • 🔰 Level: {dg.get('level', 1)}\n"
        f"📈 Reliability: {reliability_score}\n"
        f"{progress_bar}\n\n"
        "🚴 Keep hustling — every delivery powers your reputation ⚡"
    )
    
    kb = dashboard_reply_keyboard(is_online=is_online)
    
    try:
        await bot.send_message(user_id, dashboard_text, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        log.exception("Failed to send dashboard to dg %s", user_id)


# --------------------------
# Main Menu Reply Keyboard Handlers (Section 4)
# --------------------------

@router.message(F.text == "📦 My Orders")
async def handle_orders_menu(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg: return

    await _send_my_orders_view(message.bot, dg, message)


@router.message(F.text == "💰 Earnings")
async def handle_earnings_menu(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg: return

    await _send_earnings_view(message.bot, dg, message)


@router.message(F.text == "📊 Performance")
async def handle_performance_menu(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg: return

    await _send_performance_view(message.bot, dg, message)


@router.message(F.text.in_({"🟢 Go Online", "🔴 Go Offline"}))
async def handle_status_toggle(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg:
        return
    
    if message.text == "🟢 Go Online":
        # Just run online logic → show location keyboard
        await _go_online_logic(message, dg)

    elif message.text == "🔴 Go Offline":
        # Run offline logic
        await _go_offline_logic(message, dg)

        # After going offline, refresh dashboard
        updated_dg = await _db_get_delivery_guy_by_user(message.from_user.id)
        if updated_dg:
            await _send_dashboard_view(message.bot, updated_dg["user_id"], updated_dg)


# --------------------------
# Sub-Dashboard Views (Section 4)
# --------------------------

async def _send_my_orders_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Lists current + recent orders inline (Section 4)."""
    # 🔁 USING HELPER: get_all_active_orders_for_dg
    orders = await get_all_active_orders_for_dg(db.db_path, dg["id"])
    
    text = "📋 **Active Orders**\n\n"
    if not orders:
        text += "No active orders assigned to you."
    else:
        # List orders with Inline Buttons for action
        order_messages = []
        for order in orders:
            order_messages.append(
                f"**#{order['id']}** {order.get('pickup', 'N/A')} → {order.get('dropoff', 'N/A')} ({order.get('status', 'N/A')})"
            )
            items = json.loads(order['items_json'])
            item_names = [item["name"] for item in items]
            items_text = ", ".join(item_names)
            # Send each order card as a separate message with its Inline Keyboard
            status_for_kb = 'accepted' if order.get('status') == 'assigned' else order.get('status', 'accepted')
            
            order_text = (
            f"📦 *Order #{order['id']}*\n"
            "──────────────────────\n"
            f"🏠 Pickup: *{order.get('pickup')}*\n"
            f"📍 Drop-off: *{order.get('dropoff')}*\n"
            f"💰 Fee: *{int(order.get('delivery_fee', 0))} birr*\n"
            f"🛒 Items: {items_text}\n\n"            
            "⚡ Manage this order below."
        )

        await bot.send_message(
            dg["user_id"],
            order_text,
            reply_markup=accepted_order_actions(order["id"], status_for_kb),
            parse_mode="Markdown"
        )


    # Send a final status message with the back button
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")


async def _send_earnings_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Shows earnings data (Section 4)."""
    
    # NOTE: The database.db file must have get_daily_stats_for_dg and get_weekly_earnings_for_dg
    daily_stats = await db.get_daily_stats_for_dg(dg["id"], date=datetime.now().strftime('%Y-%m-%d')) if hasattr(db, "get_daily_stats_for_dg") else {"earnings": 580.0}
    weekly_earnings = await db.get_weekly_earnings_for_dg(dg["id"]) if hasattr(db, "get_weekly_earnings_for_dg") else 2340.0
    
    today_earnings = daily_stats.get('earnings', 580.0)
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db.db_path, dg["id"])
    
    text = (
        "💰 **Earnings Report**\n\n"
        f"**Today**: {int(today_earnings)} birr\n"
        f"**This Week**: {int(weekly_earnings)} birr\n"
        f"**Acceptance Rate**: {int(acceptance_rate)}%\n"
    )
    
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")


async def _send_performance_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Shows skip and reliability data (Section 4)."""
    
    skips = dg.get("skipped_requests", 0)
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db.db_path, dg["id"])
    
    reliability_score = "High 🚀" if acceptance_rate >= 90 else ("Good 👍" if acceptance_rate >= 80 else "Low ⚠️")
        
    text = (
        "📊 **Reliability Report**\n\n"
        f"**Skips**: {skips}\n"
        f"**Acceptance Rate**: {int(acceptance_rate)}%\n"
        f"**Reliability Score**: {reliability_score}\n"
    )
    
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")


# --------------------------
# Online / Offline Logic (Section 2)
# --------------------------

async def _go_online_logic(message: Message, dg: Dict[str, Any]):
    """Handles the DG going online."""
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute("UPDATE delivery_guys SET active = 1, last_online_at = CURRENT_TIMESTAMP WHERE id = ?", (dg["id"],))
            await conn.commit()
    except Exception:
        log.exception("Failed to set dg %s online", dg["id"])
        await message.answer("❌ Failed to go Online due to a server error.")
        return

    log.info("Delivery guy %s (id=%s) set to ONLINE", dg.get("name"), dg.get("id"))
    
    # Send confirmation and prompt for location (Section 2)
    # Location request MUST use a ReplyKeyboardMarkup (request_location=True)
    await message.answer(
        "✅ **You’re now online and ready to receive orders!**\n"
        "📍 Tap the button below to share your **Live Location**.\n"
        "This will automatically update your location for order assignment and tracking.",
        reply_markup=location_request_keyboard(),
        parse_mode="Markdown"
    )

async def _go_offline_logic(message: Message, dg: Dict[str, Any]):
    """Handles the DG going offline."""
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute("UPDATE delivery_guys SET active = 0, last_offline_at = CURRENT_TIMESTAMP WHERE id = ?", (dg["id"],))
            await conn.commit()
    except Exception:
        log.exception("Failed to set dg %s offline", dg["id"])
        await message.answer("❌ Failed to go Offline due to a server error.")
        return

    log.info("Delivery guy %s (id=%s) set to OFFLINE", dg.get("name"), dg.get("id"))
    
    await message.answer("💤 **You’re offline.** You won’t receive new delivery requests.", parse_mode="Markdown")


# --------------------------
# Location Handler (Section 2)
# --------------------------

@router.message(F.content_type == "location")
async def handle_location(message: Message):
    """Handles both one-time and live location updates."""
    log.info("[START] Received location update from Telegram user_id=%s", message.from_user.id)

    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg or dg.get("active", 0) == 0:
        log.warning("[ABORT] User is not a delivery guy or is offline: %s", message.from_user.id)
        # Clear the temporary location keyboard
        await message.answer("✅ Location received. Please go Online to activate tracking.", reply_markup=ReplyKeyboardRemove())
        return

    lat = message.location.latitude
    lon = message.location.longitude

    try:
        await _db_update_delivery_guy_coords(dg["id"], lat, lon)

        # 🔁 USING HELPER: get_latest_active_order_for_dg
        order = await get_latest_active_order_for_dg(db.db_path, dg["id"]) 
        
        if order:
            order_id = order["id"]
            # NOTE: We assume db.update_order_live exists in database.db
            if hasattr(db, "update_order_live"):
                await db.update_order_live(order_id, lat, lon)
            else:
                async with aiosqlite.connect(db.db_path) as conn:
                    await conn.execute("UPDATE orders SET last_lat = ?, last_lon = ? WHERE id = ?", (lat, lon, order_id))
                    await conn.commit()
            
            # NOTE: We assume db.create_location_log exists in database.db
            if hasattr(db, "create_location_log"):
                await db.create_location_log(order_id=order_id, delivery_guy_id=dg["id"], lat=lat, lon=lon)
            
        await message.answer(
            "📍 **Location updated!** ETA refreshed for students ⏱️",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown"
        )
        await _send_dashboard_view(message.bot, dg["user_id"], dg)
    except Exception:
        log.exception("DB update failed for DG id=%s", dg["id"])
        await message.answer("❌ Failed to update your location due to a server error.")
    
    log.info("[END] Location handling complete for DG id=%s", dg["id"])


# --------------------------
# Order Offer & Actions (Inline Callbacks)
# --------------------------

# Helper function to send the NEW order offer message (called by the assignment logic)
async def send_new_order_offer(bot: Bot, dg: Dict[str, Any], order: Dict[str, Any]) -> None:
    """
    Sends the inline order offer message, includes the countdown, 
    and registers the offer in the global tracker.
    """
    
    pickup_loc = order.get('pickup')
    dropoff_loc = order.get('dropoff')
    delivery_fee = order.get('delivery_fee', 0.0)
    order_id = order['id']
    
    # Calculate initial countdown display (3:00)
    initial_minutes = EXPIRY_SECONDS // 60
    initial_seconds = EXPIRY_SECONDS % 60

    try:
        breakdown = json.loads(order.get("breakdown_json") or "{}")
        drop_lat = breakdown.get("drop_lat")
        drop_lon = breakdown.get("drop_lon")
        dropoff_display = f"Live location ({drop_lat:.6f},{drop_lon:.6f})" if drop_lat and drop_lon else dropoff_loc
    except Exception:
        dropoff_display = dropoff_loc

    message_text = (
        "🚴‍♂️ **New Order Incoming!**\n\n"
        f"📍 **Pickup**: {pickup_loc}\n"
        f"🏠 **Drop-off**: {dropoff_display}\n"
        f"💰 **Delivery Fee**: {int(delivery_fee)} birr\n"
        f"⏳ **Expires in**: {initial_minutes:02d}:{initial_seconds:02d} (Live Countdown)\n" # Initial display
    )

    kb = order_offer_keyboard(order_id, EXPIRY_SECONDS) # Pass seconds

    try:
        # Capture the message object to get the message_id
        sent_message = await bot.send_message(
            dg["user_id"],
            message_text,
            reply_markup=kb,
            parse_mode="Markdown"
        )
        
        # --- NEW: Add offer to the global tracker ---
        PENDING_OFFERS[order_id] = {
            "chat_id": dg["user_id"],
            "message_id": sent_message.message_id,
            "assigned_at": datetime.now(),
            "expiry_seconds": EXPIRY_SECONDS,
            "order_id": order_id
        }

        log.info("Sent new order offer %s to DG %s and added to PENDING_OFFERS", order_id, dg["id"])
    except TelegramBadRequest as e:
        if "chat not found" in str(e):
            log.warning("DG %s cannot be contacted (chat not found). Skipping.", dg["id"])
        else:
            log.exception("Telegram error sending offer %s to DG %s", order_id, dg["id"])
    except Exception:
        log.exception("Unexpected error sending offer %s to DG %s", order_id, dg["id"])

@router.callback_query(F.data.startswith("accept_order_"))
async def handle_accept_order(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg = await _db_get_delivery_guy_by_user(call.from_user.id)
    if not dg:
        await call.answer("⚠️ Delivery profile not found.", show_alert=True)
        return

    # --- 1. Update order status ---
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute(
                """
                UPDATE orders
                SET status = 'pending',
                    accepted_at = CURRENT_TIMESTAMP
                WHERE id = ? AND delivery_guy_id = ? AND status = 'assigned'
                """,
                (order_id, dg["id"])
            )
            await conn.commit()
    except Exception:
        log.exception("Failed to accept order %s for DG %s", order_id, dg["id"])
        await call.answer("❌ Failed to accept order.", show_alert=True)
        return

    # --- 2. Stop scheduler countdown for this order ---
    PENDING_OFFERS.pop(order_id, None)   # 🔥 ensures scheduler stops tracking this order

    # --- 3. Edit the offer message to accepted view ---
    order = await db.get_order(order_id)
    if order and order.get("status") == "accepted":
        message_text = (
            "🎯 **Order Accepted!**\n"
            f"**Order #{order_id}**\n"
            f"🏠 Pickup: {order.get('pickup')}\n"
            f"📍 Drop-off: {order.get('dropoff')}\n"
            "Stay sharp and manage your delivery efficiently 👇"
        )
        try:
            await call.message.edit_text(
                message_text,
                reply_markup=accepted_order_actions(order_id, "accepted"),
                parse_mode="Markdown"
            )
            await call.answer("Order accepted!")
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                await call.answer("Order already accepted/message already updated.")
            else:
                log.warning("Failed to edit message after acceptance: %s", str(e))
                # Fallback: send a new message if edit fails
                await call.message.answer(
                    message_text,
                    reply_markup=accepted_order_actions(order_id, "accepted"),
                    parse_mode="Markdown"
                )
    else:
        await call.answer("Order status conflict or already processed.", show_alert=True)


async def get_student_chat_id(db: Database, order: Dict[str, Any]) -> Optional[int]:
    """Resolve internal user_id from orders → Telegram chat id."""
    student = await db.get_user_by_id(order["user_id"])
    return student.get("telegram_id") if student else None

@router.callback_query(F.data.startswith("skip_order_"))
async def handle_skip_order(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg = await _db_get_delivery_guy_by_user(call.from_user.id)

    if not dg:
        await call.answer("⚠️ Delivery profile not found.", show_alert=True)
        return

    # --- 0. Fetch Order ---
    order = await db.get_order(order_id)
    if not order:
        await call.answer("❌ Order not found or already processed.", show_alert=True)
        return

    dg_id = dg["id"]

    # --- 1. Update rejection list via helper ---
    try:
        from utils.db_helpers import add_dg_to_blacklist
        await add_dg_to_blacklist(db.db_path, order_id, dg_id)
    except Exception:
        log.exception("Failed to update blacklist for order %s", order_id)

    # --- 2. Update DG stats ---
    try:
        await increment_skip(db.db_path, dg_id)
    except Exception:
        log.exception("Failed to increment skip for DG %s", dg_id)

    # --- 3. Reset order back to pending ---
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute(
                """
                UPDATE orders
                SET status = 'pending', delivery_guy_id = NULL
                WHERE id = ? AND delivery_guy_id = ?
                """,
                (order_id, dg_id)
            )
            await conn.commit()
        log.debug("[DEBUG] Order %s reset to pending after skip by DG %s", order_id, dg_id)
    except Exception:
        log.exception("Failed to reset skipped order %s", order_id)
        await call.answer("❌ Error processing skip.", show_alert=True)
        return

    # --- 4. Stop scheduler countdown for this order ---
    PENDING_OFFERS.pop(order_id, None)   # 🔥 ensures scheduler stops editing countdown

    # --- 5. Edit the DG’s offer message gracefully ---
    try:
        await call.message.edit_text(
            "❌ **You skipped this order.** It will be reassigned to another partner.",
            parse_mode="Markdown",
            reply_markup=None
        )
    except Exception:
        log.warning("Failed to edit skip message for order %s", order_id)
    await call.answer("Order skipped. Next order will be sent soon.")

    # --- 6. Threshold checks ---
    try:
        await check_thresholds_and_notify(call.bot, db.db_path, dg_id, ADMIN_GROUP_ID)
    except Exception:
        log.exception("Threshold check failed for DG %s", dg_id)

    # --- 7. Immediate reassignment + notifications ---
    try:
        from utils.helpers import assign_delivery_guy, notify_student_reassignment

        # Re-fetch order with updated breakdown_json
        order = await db.get_order(order_id)
        chosen = await assign_delivery_guy(db.db_path, order_id, call.bot, current_order_data=order)

        if chosen and order:
            log.info("Order %s reassigned to DG %s", order_id, chosen["id"])

            # Notify new DG
            await send_new_order_offer(call.bot, chosen, order)

            # Notify student (edit existing message if possible)
            try:            
                await notify_student_reassignment(call.bot, db, order, chosen)
            except Exception:
                log.exception("Failed to notify student about reassignment for order %s", order_id)

            # Notify admin
            try:
                await call.bot.send_message(
                    ADMIN_GROUP_ID,
                    f"ℹ️ Order #{order_id} was skipped by DG {dg['name']} and reassigned to {chosen['name']}."
                )
            except Exception:
                log.exception("Failed to notify admin about reassignment for order %s", order_id)

        else:
            log.warning("Order %s could not be reassigned immediately", order_id)

            # Student fallback
            try:
                student_chat_id = await get_student_chat_id(db, order) if order else None
                if student_chat_id:
                    await call.bot.send_message(
                        student_chat_id,
                        "⚠️ Your order is pending reassignment. We’re finding the next available delivery partner.",
                        parse_mode="Markdown"
                    )
            except Exception:
                log.exception("Failed to notify student about pending order %s", order_id)

            # Admin fallback
            try:
                await call.bot.send_message(
                    ADMIN_GROUP_ID,
                    f"⚠️ Order #{order_id} was skipped by DG {dg['name']} but could not be reassigned immediately."
                )
            except Exception:
                log.exception("Failed to notify admin about failed reassignment for order %s", order_id)

    except Exception:
        log.exception("Reassignment failed for order %s", order_id)


# --------------------------
# Active Order Actions (Inline Callbacks)
# --------------------------

async def _validate_dg_order_simple_by_message(message: types.Message, order_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Helper to ensure DG is valid and order exists/is assigned to them."""
    dg = await _db_get_delivery_guy_by_user(message.chat.id)
    if not dg:
        return None, None
    
    # NOTE: db.get_order assumed to exist in database.db
    order = await db.get_order(order_id)
    
    if not order or order.get('delivery_guy_id') != dg['id']:
        return dg, None
    return dg, order


@router.callback_query(F.data.startswith("start_order_"))
async def handle_start_order(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg, order = await _validate_dg_order_simple_by_message(call.message, order_id)
    
    if not dg or not order:
        await call.answer("❌ This order is not assigned to you or doesn't exist.", show_alert=True)
        return

    # Update status to 'in_progress'
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute("UPDATE orders SET status = 'in_progress' WHERE id = ?", (order_id,))
            await conn.commit()
    except Exception:
        log.exception("Failed to mark order in_progress %s", order_id)
        await call.answer("❌ Failed to update order status.", show_alert=True)
        return
    
    # Notify student (Section 3)
    # NOTE: db.get_order assumed to exist in database.db
    updated_order = await db.get_order(order_id) 
    await notify_student(call.bot, updated_order, "on_the_way") 
    
    # Edit message to show new actions (Section 3)
    message_text = (
        "🚶 **Order In Progress!**\n"
        f"**Order #{order_id}**\n"
        f"🏠 Pickup: {order.get('pickup')}\n"
        f"📍 Drop-off: {order.get('dropoff')}\n"
        "You are now officially on the way! Send a **Live Update** frequently."
    )
    
    try:
        await call.message.edit_text(message_text, reply_markup=accepted_order_actions(order_id, "in_progress"), parse_mode="Markdown")
        await call.answer("Status updated to On the Way.")
    except TelegramBadRequest:
        await call.answer("Status updated.")
    

@router.callback_query(F.data.startswith("delivered_"))
async def handle_delivered(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg, order = await _validate_dg_order_simple_by_message(call.message, order_id)
    
    if not dg or not order:
        await call.answer("❌ This order is not assigned to you or doesn't exist.", show_alert=True)
        return

    # 1. Update DB: status = 'delivered', delivered_at = now(), total_deliveries += 1
    delivery_fee = float(order.get("delivery_fee") or 0)
    
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            await conn.execute(
                "UPDATE orders SET status = 'delivered', delivered_at = CURRENT_TIMESTAMP WHERE id = ?", 
                (order_id,)
            )
            await conn.execute(
                "UPDATE delivery_guys SET active = 1, total_deliveries = total_deliveries + 1, last_online_at = CURRENT_TIMESTAMP WHERE id = ?",
                (dg["id"],)
            )
            await conn.commit()
    except Exception:
        log.exception("Failed to mark delivered for order %s", order_id)
        await call.answer("❌ Failed to update order status.", show_alert=True)
        return
        
    # 2. Award XP/Coins (Section 8)
    xp_gained = 0
    coins_gained = 0.0
    try:
        if ENABLE_XP:
            xp_gained = XP_PER_DELIVERY
        if ENABLE_COINS:
            coins_gained = delivery_fee * COIN_RATIO
            
        if xp_gained > 0 or coins_gained > 0:
            async with aiosqlite.connect(db.db_path) as conn:
                await conn.execute(
                    "UPDATE delivery_guys SET xp = xp + ?, coins = coins + ? WHERE id = ?",
                    (xp_gained, coins_gained, dg["id"])
                )
                await conn.commit()
                
            updated_dg = await _db_get_delivery_guy_by_user(call.from_user.id)
            # NOTE: db.auto_compute_level assumed to exist in database.db
            if updated_dg and hasattr(db, "auto_compute_level"):
                new_level = await db.auto_compute_level(updated_dg["xp"]) 
                if new_level != updated_dg.get("level"):
                    async with aiosqlite.connect(db.db_path) as conn:
                        await conn.execute("UPDATE delivery_guys SET level = ? WHERE id = ?", (new_level, dg["id"]))
                        await conn.commit()
                        
    except Exception:
        log.exception("Failed to award XP/Coins for order %s", order_id)

    # 3. Record Daily Stat (Section 5)
    # 🔁 USING HELPER: record_daily_stat
    await record_daily_stat(db.db_path, dg["id"], delivery_fee, delivered=1)
    
    # 4. Notify student (Section 3)
    await notify_student(call.bot, order, "delivered")
    
    # 5. Send Daily Summary (Section 3) 
    
    # NOTE: db.get_daily_stats_for_dg assumed to exist in database.db
    today_stats = await db.get_daily_stats_for_dg(dg["id"], date=datetime.now().strftime('%Y-%m-%d')) if hasattr(db, "get_daily_stats_for_dg") else {"deliveries": 1, "earnings": delivery_fee}
    
    deliveries_today = today_stats.get("deliveries", 1)
    earnings_today = today_stats.get("earnings", delivery_fee)
    
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db.db_path, dg["id"])
    
    reliability = "Excellent 🚀" if acceptance_rate >= 90 else ("Good 👍" if acceptance_rate >= 80 else "Fair")

    summary_text = (
        f"🎉 **Delivery #{order_id} Complete!**\n"
        "──────────────────────\n"
        f"📦 Status: Delivered successfully\n\n"
        "📊 **Your Daily Progress**\n"
        f"🚚 Deliveries today: *{deliveries_today}*\n"
        f"💵 Earnings: *{int(earnings_today)} birr*\n"
        "🎁 **Rewards Earned**\n"
        f"✨ +{xp_gained} XP\n"
        f"💰 +{coins_gained:.2f} Coins\n\n"
        "⚡ Keep going strong! Use the menu below to head back to your dashboard."
    )


    try:
        # Edit the order card to the final summary, then prompt for the next step using the Reply Keyboard
        await call.message.edit_text(summary_text, reply_markup=None, parse_mode="Markdown")
        # Send the menu_back_keyboard as a new message so the user can easily navigate
        await call.message.answer("Task complete. What's next?", reply_markup=menu_back_keyboard())
        await call.answer("Delivery complete! 🎉")
    except TelegramBadRequest:
        await call.answer("Delivery complete! 🎉")
        await call.message.answer(summary_text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")
    
    
@router.callback_query(F.data.startswith("contact_user_"))
async def handle_contact_user(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg, order = await _validate_dg_order_simple_by_message(call.message, order_id)
    
    if not dg or not order:
        await call.answer("❌ Cannot contact user for this order.", show_alert=True)
        return

    # Lookup student record
    student = await db.get_user_by_id(order["user_id"])
    if not student:
        await call.answer("❌ Student not found in DB.", show_alert=True)
        return

    phone = student.get("phone")
    first_name = student.get("first_name", "Student")

    if phone:
        # Send Telegram contact card (includes Call button on mobile)
        await call.message.answer_contact(
            phone_number=phone,
            first_name=first_name
        )
        await call.answer("📱 Contact shared.")
    else:
        await call.answer("❌ No phone number available for this student.", show_alert=True)

   
@router.callback_query(F.data.startswith("update_location_"))
async def request_live_update(call: CallbackQuery):
    """Prompts the DG to manually send their location (Inline button triggering a Reply Keyboard for location)."""
    order_id = int(call.data.split('_')[-1])
    
    # Use the ReplyKeyboardMarkup for the location request
    temp_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=f"📍 Send Location for Order {order_id}", request_location=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await call.message.answer(
        f"Please send your **current location** now to update the student's ETA for Order **#{order_id}**.",
        reply_markup=temp_kb,
        parse_mode="Markdown"
    )
    await call.answer("Prompted for location update.")

# --------------------------
# Notification helper stub 
# --------------------------
async def _lookup_student_telegram(order: Dict[str, Any]) -> Optional[int]:
    """Placeholder for student lookup logic."""
    try:
        async with aiosqlite.connect(db.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            # Assuming 'users' table has a 'telegram_id' field linked by order.user_id (student ID)
            async with conn.execute("SELECT telegram_id FROM users WHERE id = ?", (order["user_id"],)) as cur:
                r = await cur.fetchone()
                return r["telegram_id"] if r else None
    except Exception:
        log.exception("Failed _lookup_student_telegram")
        return None

async def notify_student(bot, order: Dict[str, Any], status: str) -> None:
    """Sends status update to the student."""
    student_tg = await _lookup_student_telegram(order)
    if not student_tg:
        log.debug("notify_student: no telegram id found for order %s", order.get("id"))
        return

    eta_line = ""
    # Simplified ETA calculation for the stub
    if status == "on_the_way":
        eta_min = 10 # Hardcoded placeholder
        eta_line = f"\n⏱ ETA: ~{eta_min} min"

    try:
        if status == "on_the_way":
            await bot.send_message(student_tg, f"🚶 Your delivery partner is on the way!{eta_line}")
        elif status == "delivered":
            await bot.send_message(student_tg, "🎉 Order delivered! Enjoy your meal 🍽")
    except Exception:
        log.exception("notify_student: failed to send message to student %s", student_tg)