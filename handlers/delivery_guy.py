# handlers/delivery_guy.py - REVISED IMPORTS
"""
FINALIZED SYSTEM BLUEPRINT IMPLEMENTATION — "Delivery Partner 2030 System"

REVISION:
1. Dashboard/Menu Navigation uses ReplyKeyboardMarkup.
2. Order Flow uses InlineKeyboardMarkup.
3. Database helper functions are now imported from utils.db_helpers.
"""

import asyncio
from collections import Counter
import contextlib
import json
import logging
import math
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timedelta
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
    add_dg_to_blacklist
)
from utils.helpers import eta_and_distance

# Router + DB
router = Router()
from app_context import db
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --------------------------
# Feature toggles / constants
# --------------------------
ENABLE_XP = True
XP_PER_DELIVERY = 10
ENABLE_COINS = True
COIN_RATIO = 0.05
ADMIN_GROUP_ID = settings.ADMIN_DAILY_GROUP_ID

# Callback Prefix Constants
CB_PREFIX_DASHBOARD = "dash"
CB_PREFIX_ORDER = "order"
CB_PREFIX_EARNINGS = "earn"
CB_PREFIX_PERFORMANCE = "perf"

# --------------------------
# DB helper shims (REMOVED - now using utils/db_helpers)
# --------------------------

async def _db_get_delivery_guy_by_user(telegram_user_id: int) -> Optional[Dict[str, Any]]:
    try:
        return await db.get_delivery_guy_by_user(telegram_user_id)
    except Exception:
        log.exception("_db_get_delivery_guy_by_user failed for %s", telegram_user_id)
        return None


async def _db_update_delivery_guy_coords(dg_id: int, lat: float, lon: float):
    try:
        await db.update_delivery_guy_coords(dg_id, lat, lon)
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
            [KeyboardButton(text="📍 Share My Location (Optional)", request_location=True)],
            [KeyboardButton(text="🏠 Back to Dashboard")]

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

    
    if status == "in_progress":
        # Delivery is ongoing
        buttons = [
            InlineKeyboardButton(text="📍 Live Update", callback_data=f"update_location_{order_id}"),
            InlineKeyboardButton(text="📦 Mark Delivered", callback_data=f"delivered_{order_id}")

        ]
    elif status == "ready":
       buttons = [
            InlineKeyboardButton(text="▶️ Start Delivery", callback_data=f"start_order_{order_id}"),

        ]
    else:
        # For statuses like pending/assigned/preparing/delivered/cancelled
        buttons = []

    action_row = [
        InlineKeyboardButton(text="💬 Contact User", callback_data=f"contact_user_{order_id}"),
        InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_order_{order_id}")
    ]

    # Only include buttons row if not empty
    inline_keyboard = [buttons] if buttons else []
    inline_keyboard.append(action_row)

    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)


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

    is_online = bool(dg.get("active", False))
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db, dg["id"])
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
    tg_id = await db.get_delivery_guy_telegram_id(user_id)  # returns row with telegram_id
    
    try:
        await bot.send_message(tg_id, dashboard_text, reply_markup=kb, parse_mode="Markdown")
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
    if not dg:
        return

    today = datetime.now()
    stats = await db.get_daily_stats_for_dg(dg["id"], today)

    text = (
        "💰 **Earnings Snapshot**\n"
        "──────────────────────\n"
        f"🚚 Deliveries today: {stats['deliveries']}\n"
        f"💵 Earnings: {int(stats['earnings'])} birr\n"
        f"🎁 Rewards: +{stats['xp']} XP • +{stats['coins']:.2f} Coins\n\n"
        "⚡ Tap below to dive deeper."
    )

    await message.answer(text, reply_markup=earnings_reply_keyboard(), parse_mode="Markdown")



@router.message(F.text == "📊 Today's Stats")
async def handle_today_performance(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg:
        return

    today = datetime.now()
    stats = await db.get_daily_stats_for_dg(dg["id"], today)
    text = (
        "📊 **Today's Stats**\n"
        "──────────────────────\n"
        f"🚚 Deliveries: {stats['deliveries']}\n"
        f"💵 Earnings: {int(stats['earnings'])} birr\n"
        f"🎁 Rewards: +{stats['xp']} XP • +{stats['coins']:.2f} Coins\n\n"
        "🔥 Reliability builds your legend 🚴"
    )

    await message.answer(text, reply_markup=earnings_reply_keyboard(), parse_mode="Markdown")



@router.message(F.text == "📅 Weekly Stats")
async def handle_weekly_performance(message: Message):
    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg:
        return

    today = datetime.now()
    week_start = (today - timedelta(days=today.weekday()))
    week_end = (today + timedelta(days=6 - today.weekday()))

    breakdown = await db.get_weekly_earnings_for_dg(dg["id"], week_start, week_end)
    totals = await db.get_weekly_totals_for_dg(dg["id"], week_start, week_end)

    lines = []
    for day in breakdown:
        day_label = day["date"].strftime("%a")  # Mon, Tue, etc.
        lines.append(
            f"{day_label}: 🚚 {day['deliveries']} • 💵 {int(day['earnings'])} birr"
        )

    text = (
        "📅 **Weekly Earnings Report**\n"
        "──────────────────────\n"
        + "\n".join(lines) + "\n\n"
        f"🏆 Total: {totals['deliveries']} deliveries • {int(totals['earnings'])} birr\n"
        f"🎁 Rewards: +{totals['xp']} XP • +{totals['coins']:.2f} Coins\n\n"
        "⚡ Keep pushing — greatness is built one day at a time."
    )

    await message.answer(text, reply_markup=earnings_reply_keyboard(), parse_mode="Markdown")



def earnings_reply_keyboard() -> ReplyKeyboardMarkup:
    """ReplyKeyboard for Earnings submenu."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Today's Stats"), KeyboardButton(text="📅 Weekly Stats")],
            [KeyboardButton(text="🏠 Back to Dashboard")]
        ],
        resize_keyboard=True
    )




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
        await _go_online_logic(message, dg)

    elif message.text == "🔴 Go Offline":
        await _go_offline_logic(message, dg)
        updated_dg = await _db_get_delivery_guy_by_user(message.from_user.id)
        if updated_dg:
            await _send_dashboard_view(message.bot, updated_dg["user_id"], updated_dg)


# Inline button handler → reuse the same function
@router.callback_query(F.data.in_({"dg:go_online", "dg:go_offline"}))
async def handle_status_toggle_inline(cb: CallbackQuery):
    dg = await _db_get_delivery_guy_by_user(cb.from_user.id)
    if not dg:
        await cb.answer("Not registered as a delivery guy.")
        return

    if cb.data == "dg:go_online":
        await _go_online_logic(cb.message, dg)
    else:
        await _go_offline_logic(cb.message, dg)
        updated_dg = await _db_get_delivery_guy_by_user(cb.from_user.id)
        if updated_dg:
            await _send_dashboard_view(cb.bot, updated_dg["user_id"], updated_dg)

    await cb.answer()

# --------------------------
# Sub-Dashboard Views (Section 4)
# --------------------------

STATUS_LABELS = {
    "pending": "⏳ Awaiting vendor confirmation",
    "assigned": "📌 Assigned (waiting vendor acceptance)",
    "preparing": "👨‍🍳 Vendor is preparing",
    "ready": "✅ Ready for pickup",
    "accepted": "👨‍🍳 Vendor is preparing",
    "in_progress": "🚚 On the way",
    "delivered": "🎉 Delivered",
    "cancelled": "❌ Cancelled",
}


async def _send_my_orders_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Lists current + recent orders inline (Section 4)."""
    orders = await get_all_active_orders_for_dg(db, dg["id"])

    text = "📋 **Active Orders**\n\n"
    if not orders:
        text += "No active orders assigned to you."
    else:
        for order in orders:
            items = json.loads(order['items_json'])
            item_names = [item["name"] for item in items]
            counts = Counter(item_names)
            items_text = ", ".join(
                f"{name} x{count}" if count > 1 else name
                for name, count in counts.items()
            )

            status_for_kb = 'accepted' if order.get('status') == 'assigned' else order.get('status', 'accepted')
            subtotal_fee = int(order.get('food_subtotal', 0))   # ✅ fixed
            delivery_fee = int(order.get('delivery_fee', 0))
            status_label = STATUS_LABELS.get(order.get("status"), "ℹ️ Unknown status")

            order_text = (
                f"📦 *Order #{order['id']}*\n"
                f"📌 Status: {status_label}\n\n"
                "──────────────────────\n"
                f"🏠 Pickup: *{order.get('pickup')}*\n"
                f"📍 Drop-off: *{order.get('dropoff')}*\n"
                f"💰 Subtotal Fee: *{subtotal_fee} birr*\n"
                f"🚚 _Delivery fee:_ *{delivery_fee} birr*\n"
                "──────────────────────\n"
                f"💵 *Total Payable: {subtotal_fee + delivery_fee} birr*\n\n"
                f"🛒 Items: {items_text}\n\n"
                "⚡ Manage this order below."
            )

            # ✅ send each order separately inside the loop
            await bot.send_message(
                dg["telegram_id"],
                order_text,
                reply_markup=accepted_order_actions(order["id"], status_for_kb),
                parse_mode="Markdown"
            )

    # Final summary message
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")


async def _send_earnings_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Shows earnings data (Section 4)."""
    
    # NOTE: The database.db file must have df and get_weekly_earnings_for_dg
    daily_stats = await db.df(dg["id"], date=datetime.now()) if hasattr(db, "df") else {"earnings": 580.0}
    weekly_earnings = await db.get_weekly_earnings_for_dg(dg["id"]) if hasattr(db, "get_weekly_earnings_for_dg") else 2340.0
    
    today_earnings = daily_stats.get('earnings', 580.0)
    # 🔁 USING HELPER: calc_acceptance_rate
    acceptance_rate = await calc_acceptance_rate(db, dg["id"])
    
    text = (
        "💰 **Earnings Report**\n\n"
        f"**Today**: {int(today_earnings)} birr\n"
        f"**This Week**: {int(weekly_earnings)} birr\n"
        f"**Acceptance Rate**: {int(acceptance_rate)}%\n"
    )
    
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")




import random

TAGLINES_PERFORMANCE = [
    "Reliability is your badge of honor 🥇",
    "Consistency is your superpower ⚡",
    "Trust is built one delivery at a time 🤝",
    "Momentum is everything — keep the wheels turning 🔥",
]

def get_random_performance_tagline() -> str:
    return random.choice(TAGLINES_PERFORMANCE)


async def _send_performance_view(bot: Bot, dg: Dict[str, Any], message: Message):
    """Shows skip and reliability data in a cinematic style."""
    
    skips = dg.get("skipped_requests", 0)
    acceptance_rate = await calc_acceptance_rate(db, dg["id"])
    
    # Reliability tier
    if acceptance_rate >= 90:
        reliability_score = "🥇 High Reliability 🚀"
    elif acceptance_rate >= 80:
        reliability_score = "⚖️ Good Reliability 👍"
    else:
        reliability_score = "⚠️ Low Reliability — needs improvement"
    
    # Progress bar (10 blocks)
    filled = int(acceptance_rate / 10)
    progress_bar = "▰" * filled + "▱" * (10 - filled)
    
    tagline = get_random_performance_tagline()
    
    text = (
        "📊 **Performance Report**\n"
        "──────────────────────\n"
        f"🚫 Skips: {skips}\n"
        f"✅ Acceptance Rate: {int(acceptance_rate)}%\n"
        f"{progress_bar}\n"
        f"{reliability_score}\n\n"
        f"{tagline}"
    )
    
    await message.answer(text, reply_markup=menu_back_keyboard(), parse_mode="Markdown")


# --------------------------
# Online / Offline Logic (Section 2)
# --------------------------

async def _go_online_logic(message: Message, dg: Dict[str, Any]):
    """Handles the DG going online (Postgres/asyncpg version)."""
    try:
        # Use the Database method instead of raw SQL
        await db.set_delivery_guy_online(dg["id"])
    except Exception:
        log.exception("Failed to set dg %s online", dg["id"])
        await message.answer("❌ Failed to go Online due to a server error.")
        return

    log.info("Delivery guy %s (id=%s) set to ONLINE", dg.get("name"), dg.get("id"))

    # Send confirmation and prompt for location
    await message.answer(
        "✅ **You’re now online and ready to receive orders!**\n"
        "📍 Tap the button below to share your **Live Location**.\n"
        "This will automatically update your location for order assignment and tracking.",
        reply_markup=location_request_keyboard(),
        parse_mode="Markdown"
    )


async def _go_offline_logic(message: Message, dg: Dict[str, Any]):
    """Handles the DG going offline (Postgres/asyncpg version)."""
    try:
        # Use the Database method instead of raw SQL
        await db.set_delivery_guy_offline(dg["id"])
    except Exception:
        log.exception("Failed to set dg %s offline", dg["id"])
        await message.answer("❌ Failed to go Offline due to a server error.")
        return

    log.info("Delivery guy %s (id=%s) set to OFFLINE", dg.get("name"), dg.get("id"))

    await message.answer(
        "💤 **You’re offline.** You won’t receive new delivery requests.",
        parse_mode="Markdown"
    )

# --------------------------
# Location Handler (Section 2)
# --------------------------
@router.message(F.content_type == "location")
async def handle_location(message: Message):
    """Handles both one-time and live location updates (Postgres/asyncpg version)."""
    log.info("[START] Received location update from Telegram user_id=%s", message.from_user.id)

    dg = await _db_get_delivery_guy_by_user(message.from_user.id)
    if not dg or not dg.get("active", False):  # Postgres returns booleans
        log.warning("[ABORT] User is not a delivery guy or is offline: %s", message.from_user.id)
        # Clear the temporary location keyboard
        await message.answer(
            "✅ Location received. Please go Online to activate tracking.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    lat = message.location.latitude
    lon = message.location.longitude

    try:
        # Update DG coordinates
        await db.update_delivery_guy_coords(dg["id"], lat, lon)

        # Get latest active order for this DG
        order = await db.get_latest_active_order_for_dg(dg["id"])
        
        if order:
            order_id = order["id"]

            # Update order live location
            await db.update_order_live(order_id, lat, lon)

            # Log location update
            await db.create_location_log(
                order_id=order_id,
                delivery_guy_id=dg["id"],
                lat=lat,
                lon=lon
            )
            
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

import logging
log = logging.getLogger(__name__)

# Helper function to send the NEW order offer message (called by the assignment logic)
async def send_new_order_offer(bot: Bot, dg: Dict[str, Any], order: Dict[str, Any]) -> None:
    order_id = order['id']
    pickup_loc = order.get('pickup')
    dropoff_loc = order.get('dropoff')
    delivery_fee = order.get('delivery_fee', 0.0)

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
        f"⏳ **Expires in**: {initial_minutes:02d}:{initial_seconds:02d} (Live Countdown)\n"
    )

    kb = order_offer_keyboard(order_id, EXPIRY_SECONDS)


    try:
        sent_message = await bot.send_message(
            dg["telegram_id"],
            message_text,
            reply_markup=kb,
            parse_mode="Markdown"
        )
        PENDING_OFFERS[order_id] = {
            "chat_id": dg["telegram_id"],
            "message_id": sent_message.message_id,
            "assigned_at": datetime.now(),
            "expiry_seconds": EXPIRY_SECONDS,
            "order_id": order_id
        }
        log.info("[OFFER SENT] Order %s → DG %s (msg_id=%s)", order_id, dg["id"], sent_message.message_id)
        await db.increment_total_requests(dg["id"])


        # --- NEW: Notify admin group ---
        vendor_name = order.get("vendor_name", "Unknown Vendor")
        admin_text = (
            f"🚴 Order {order_id} Sent to DG: {dg.get('name','Unknown')}\n"
        )
        try:
            await bot.send_message(settings.ADMIN_DAILY_GROUP_ID, admin_text, parse_mode="Markdown")
            log.info("[ADMIN NOTIFY] Order %s offer sent to DG %s, notified admin group", order_id, dg["id"])
        except Exception:
            log.exception("[ADMIN NOTIFY FAIL] Could not notify admin group for order %s", order_id)

    except TelegramBadRequest as e:
        if "chat not found" in str(e):
            log.warning("[OFFER FAIL] DG %s cannot be contacted (chat not found) for order %s", dg["id"], order_id)
        else:
            log.exception("[OFFER FAIL] Telegram error sending order %s to DG %s: %s", order_id, dg["id"], str(e))
    except Exception:
        log.exception("[OFFER FAIL] Unexpected error sending order %s to DG %s", order_id, dg["id"])

@router.callback_query(F.data.startswith("accept_order_"))
async def handle_accept_order(call: CallbackQuery):
    try:
        await call.answer("Processing acceptance…")
    except Exception:
        pass

    order_id = int(call.data.split("_")[-1])
    dg = await _db_get_delivery_guy_by_user(call.from_user.id)
    if not dg:
        await call.answer("⚠️ Delivery profile not found.", show_alert=True)
        return

    # --- 1. Check current order status before updating ---
    order = await db.get_order(order_id)
    if not order:
        await call.answer("❌ Order not found.", show_alert=True)
        return

    try:
        if order["status"] == "ready":
            if order.get("delivery_guy_id") is None:
                # Vendor already prepared, no DG yet → assign DG but keep status = ready
                await db.update_order_status(order_id, "ready", dg["id"])
                log.info("DG %s accepted order %s (status stays READY)", dg["id"], order_id)
            else:
                # Already assigned → expire this offer
                await call.answer("❌ Order already assigned to another DG.", show_alert=True)
                return
        elif order["status"] == "preparing" and order.get("delivery_guy_id") is None:
            # Normal flow → assign DG and keep status = preparing
            await db.update_order_status(order_id, "preparing", dg["id"])
            log.info("DG %s accepted order %s (status PREPARING)", dg["id"], order_id)
        else:
            await call.answer("Order status conflict or already processed.", show_alert=True)
            return
    except Exception:
        log.exception("Failed to accept order %s for DG %s", order_id, dg["id"])
        await call.answer("❌ Failed to accept order.", show_alert=True)
        return

    # --- 2. Stop scheduler countdown for this order ---
    PENDING_OFFERS.pop(order_id, None)

    # --- 3. Notify student & update message ---
    order = await db.get_order(order_id)
    if order:
        subtotal = order.get("food_subtotal", 0)
        delivery_fee = order.get("delivery_fee", 0)
        total_payable = subtotal + delivery_fee
        order["delivery_guy_name"] = dg["name"]
        order["campus"] = dg.get("campus")
        await notify_student(call.bot, order, status="assigned")

        try:
            items = json.loads(order.get("items_json", "[]")) or []
            from collections import Counter
            names = [i.get("name", "") if isinstance(i, dict) else str(i) for i in items]
            counts = Counter(names)
            items_str = ", ".join(
                f"{name} x{count}" if count > 1 else name
                for name, count in counts.items()
            )
        except Exception:
            items_str = "Items unavailable"

        message_text = (
            f"📦 Order #{order_id}\n"
            f"📌 Status: {'✅ Ready for pickup' if order['status']=='ready' else '👨‍🍳 Vendor is preparing...'}\n\n"
            "──────────────────────\n"
            f"🏠 Pickup: {order.get('pickup')}\n"
            f"📍 Drop-off: {order.get('dropoff')}\n"
            f"💰 Subtotal Fee: {subtotal} birr\n"
            f"🚚 Delivery fee: {delivery_fee} birr\n"
            "──────────────────────\n"
            f"💵 Total Payable: {total_payable} birr\n\n"
            f"🛒 Items: {items_str}\n\n"
            "⚡ Manage this order below.\n\n"
            "For robust and fast use My Orders in the dashboard."
        )

        try:
            status = order["status"]

            # For preparing, reuse accepted actions
            if status == "preparing":
                action_key = "accepted"
            elif status == "ready":
                action_key = "ready"
            else:
                action_key = "accepted"  # fallback

            await call.message.edit_text(
                message_text,
                reply_markup=accepted_order_actions(order_id, action_key),
                parse_mode="Markdown"
            )
            await db.increment_accepted_requests(dg["id"])
            await call.answer("Order accepted!")
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                await call.answer("Order already accepted/message already updated. Check it in My Orders.")
            else:
                log.warning("Failed to edit message after acceptance: %s", str(e))
                await call.message.answer(
                    message_text,
                    reply_markup=accepted_order_actions(order_id, "accepted"),
                    parse_mode="Markdown"
                )


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
        # Update helper to accept Database instance instead of db_path
        await add_dg_to_blacklist(db, order_id, dg_id)
    except Exception:
        log.exception("Failed to update blacklist for order %s", order_id)

    # --- 2. Update DG stats ---
    try:
        await db.increment_skip(dg_id)
    except Exception:
        log.exception("Failed to increment skip for DG %s", dg_id)

    # --- 3. Reset order back to pending ---
    try:
        await db.update_order_status(order_id, "pending")  # delivery_guy_id reset handled inside method
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
        # Update helper to accept Database instance instead of db_path
        await db.check_thresholds_and_notify(call.bot, dg_id, ADMIN_GROUP_ID)    except Exception:
        log.exception("Threshold check failed for DG %s", dg_id)

    # --- 7. Immediate reassignment + notifications ---
    try:
        from handlers.delivery_guy import send_new_order_offer
        from utils.db_helpers import add_dg_to_blacklist

        # Re-fetch order with updated breakdown_json
        order = await db.get_order(order_id)
        if not order:
            log.warning("[REASSIGN] Order %s not found when trying to re-offer", order_id)
            return

        # Find next candidate (helper below). It returns a DG dict or None.
        from utils.helpers import find_next_candidate
        chosen = await find_next_candidate(db, order_id, order)

        if chosen:
            log.info("[REASSIGN] Offering Order %s to next DG %s", order_id, chosen["id"])

            # Send offer to the chosen DG (this registers the offer in PENDING_OFFERS)
            try:
                await send_new_order_offer(call.bot, chosen, order)
                log.debug("[NOTIFY] Sent offer for order %s to DG %s", order_id, chosen["id"])
            except Exception:
                log.exception("[NOTIFY] Failed to send offer for order %s to DG %s", order_id, chosen["id"])
                # If sending fails, blacklist and try next candidate
                try:
                    await add_dg_to_blacklist(db, order_id, chosen["id"])
                except Exception:
                    log.exception("[BLACKLIST] Failed to blacklist DG %s after send failure for order %s", chosen["id"], order_id)

            # Student: inform that we're offering to a new partner (soft message)
            try:
                student_chat_id = await get_student_chat_id(db, order)
                if student_chat_id:
                    
                    log.debug("[NOTIFY] Student informed about re-offer for order %s", order_id)
            except Exception:
                log.exception("[NOTIFY] Failed to inform student about re-offer for order %s", order_id)

            # Admin: log the skip + re-offer
            try:
                await call.bot.send_message(
                    ADMIN_GROUP_ID,
                    f"ℹ️ Order #{order_id} was skipped by DG {dg['name']} and re-offered to {chosen['name']}."
                )
            except Exception:
                log.exception("[NOTIFY] Failed to notify admin about re-offer for order %s", order_id)

        else:
            log.warning("[REASSIGN] No eligible DG found to offer order %s immediately", order_id)

            # Student fallback: pending reassignment
            try:
                student_chat_id = await get_student_chat_id(db, order)
                if student_chat_id:
                    await call.bot.send_message(
                        student_chat_id,
                        "⚠️ Your order is pending reassignment. We’re finding the next available delivery partner.",
                        parse_mode="Markdown"
                    )
            except Exception:
                log.exception("[NOTIFY] Failed to notify student about pending reassignment for order %s", order_id)

            # Admin fallback: escalate
            try:
                await call.bot.send_message(
                    ADMIN_GROUP_ID,
                    f"⚠️ Order #{order_id} was skipped by DG {dg['name']} and could not be re-offered automatically."
                )
            except Exception:
                log.exception("[NOTIFY] Failed to notify admin about failed re-offer for order %s", order_id)

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
        await db.update_order_status(order_id, "in_progress")
    except Exception:
        log.exception("Failed to mark order in_progress %s", order_id)
        await call.answer("❌ Failed to update order status.", show_alert=True)
        return
    
    # Notify student
    updated_order = await db.get_order(order_id) 
    await notify_student(call.bot, updated_order, "on_the_way") 
    
    # Edit message to show new actions
    message_text = (
        "🚶 **Order In Progress!**\n"
        f"**Order #{order_id}**\n"
        "──────────────────────\n"
        f"🏠 Pickup: {order.get('pickup')}\n"
        f"📍 Drop-off: {order.get('dropoff')}\n"
        f"💵 Total: {order.get('food_subtotal',0) + order.get('delivery_fee',0)} birr\n\n"
        "⚡ You’re on the move — send **Live Updates** to keep students in the loop!"
    )

    
    try:
        await call.message.edit_text(
            message_text,
            reply_markup=accepted_order_actions(order_id, "in_progress"),
            parse_mode="Markdown"
        )
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

    delivery_fee = float(order.get("delivery_fee") or 0)
    
    try:
        # Update order status to delivered
        await db.update_order_status(order_id, "delivered", dg["id"])
        await db.increment_total_deliveries(dg["id"])
        # Update DG stats (total deliveries, active flag)
        await db.set_delivery_guy_online(dg["id"])
    except Exception:
        log.exception("Failed to mark delivered for order %s", order_id)
        await call.answer("❌ Failed to update order status.", show_alert=True)
        return
        
    # Award XP/Coins
    xp_gained = XP_PER_DELIVERY if ENABLE_XP else 0
    coins_gained = delivery_fee * COIN_RATIO if ENABLE_COINS else 0.0
    try:
        if xp_gained > 0 or coins_gained > 0:
            await db.record_daily_stat_delivery(dg["id"], datetime.now().strftime('%Y-%m-%d'), delivery_fee, xp_gained, coins_gained)
            updated_dg = await _db_get_delivery_guy_by_user(call.from_user.id)
            if updated_dg and hasattr(db, "auto_compute_level"):
                new_level = await db.auto_compute_level(updated_dg["xp"])
                if new_level != updated_dg.get("level"):
                    # Update level
                    await db.update_delivery_guy_level(dg["id"], new_level)
    except Exception:
        log.exception("Failed to award XP/Coins for order %s", order_id)

    # Record Daily Stat
    await db.record_daily_stat_delivery(dg["id"], datetime.now().strftime('%Y-%m-%d'), delivery_fee)

    # Notify student
    await notify_student(call.bot, order, "delivered")
    
    # Daily summary
    today_stats = await db.get_daily_stats(
    dg["id"], datetime.now().strftime("%Y-%m-%d")
) or {"deliveries": 0, "earnings": 0.0, "skipped": 0, "assigned": 0, "acceptance_rate": 0.0}
    deliveries_today = today_stats.get("deliveries", 1)
    earnings_today = today_stats.get("earnings", delivery_fee)
    acceptance_rate = await db.calc_vendor_reliability_for_day(dg["id"])
    
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
        await call.message.edit_text(summary_text, reply_markup=None, parse_mode="Markdown")
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

    student = await db.get_user_by_id(order["user_id"])
    if not student:
        await call.answer("❌ Student not found in DB.", show_alert=True)
        return

    phone = student.get("phone")
    first_name = student.get("first_name", "Student")

    if phone:
        await call.message.answer_contact(phone_number=phone, first_name=first_name)
        await call.answer("📱 Contact shared.")
    else:
        await call.answer("❌ No phone number available for this student.", show_alert=True)


@router.callback_query(F.data.startswith("refresh_order_"))
async def handle_refresh_order(call: CallbackQuery):
    order_id = int(call.data.split('_')[-1])
    dg = await _db_get_delivery_guy_by_user(call.from_user.id)
    if not dg:
        await call.answer("⚠️ Delivery profile not found.", show_alert=True)
        return

    # Fetch latest order data
    order = await db.get_order(order_id)
    if not order:
        await call.answer("❌ Order not found or already processed.", show_alert=True)
        return

    # Build updated message text (similar to accept_order handler)
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

    subtotal = order.get("food_subtotal", 0)
    delivery_fee = order.get("delivery_fee", 0)
    total_payable = subtotal + delivery_fee
    status_label = STATUS_LABELS.get(order.get("status"), "ℹ️ Unknown status")

    message_text = (
        f"📦 Order #{order_id}\n"
        f"📌 Status: {status_label}\n\n"
        "──────────────────────\n"
        f"🏠 Pickup: {order.get('pickup')}\n"
        f"📍 Drop-off: {order.get('dropoff')}\n"
        f"💰 Subtotal Fee: {subtotal} birr\n"
        f"🚚 Delivery fee: {delivery_fee} birr\n"
        "──────────────────────\n"
        f"💵 Total Payable: {total_payable} birr\n\n"
        f"🛒 Items: {items_str}\n\n"
        "⚡ Manage this order below."
    )

    # Edit the existing message instead of sending new
    try:
    # Prevent Telegram “message is not modified” error
        old_text = call.message.text
        old_markup = call.message.reply_markup

        new_markup = accepted_order_actions(order_id, order.get("status"))

        # If nothing changed → skip edit
        if old_text == message_text and str(old_markup) == str(new_markup):
            await call.answer("🔄 Order already up-to-date!")
            return

        await call.message.edit_text(
            message_text,
            reply_markup=new_markup,
            parse_mode="Markdown"
        )
        await call.answer("🔄 Order refreshed!")

    except Exception as e:
        await call.answer("❌ Failed to refresh order.", show_alert=True)
        log.exception("Failed to refresh order %s: %s", order_id, str(e))


@router.callback_query(F.data.startswith("update_location_"))
async def request_live_update(call: CallbackQuery):
    """Prompts the DG to manually send their location."""
    order_id = int(call.data.split('_')[-1])
    temp_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=f"📍 Send Location for Order {order_id}", request_location=True)]],
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
    """Postgres version of student lookup logic."""
    try:
        student = await db.get_user_by_id(order["user_id"])
        return student.get("telegram_id") if student else None
    except Exception:
        log.exception("Failed _lookup_student_telegram")
        return None


async def notify_student(bot, order: Dict[str, Any], status: str) -> None:
    """Sends status update to the student with cinematic flair + track button."""
    student_tg = await _lookup_student_telegram(order)
    if not student_tg:
        log.debug("notify_student: no telegram id found for order %s", order.get("id"))
        return

    order_id = order.get("id")
    eta_line = ""

    # If we have coords, compute ETA dynamically
    if status == "on_the_way":
        vendor_coords = order.get("vendor_coords")
        drop_coords = order.get("drop_coords")
        
        eta_line = "\n⏱ ETA: ~10 min"

    # Inline keyboard with Track Order
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Track", callback_data=f"order:track:{order_id}")]
        ]
    )

    try:
        if status == "on_the_way":
            msg = f"🚶 Your delivery partner is on the way!{eta_line}"
            await bot.send_message(student_tg, msg, reply_markup=kb)
        elif status == "assigned":
            dg_name = order.get("delivery_guy_name", "Delivery Partner")
            campus = order.get("campus", "")
            msg = (
    f"🚴 *Delivery Partner Assigned!*\n"
    "──────────────────────\n"
    f"📦 *Order #{order.get('id')}*\n"
    f"👤 Partner: *{dg_name}* ({campus})\n"
    f"📍 Drop-off: {order.get('dropoff')}\n\n"
    "🧭 Track every step in *📍 Track Order*.\n\n"
    "✨ Sit back, relax — your food is on its way!"
)

            await bot.send_message(student_tg, msg, reply_markup=kb, parse_mode="Markdown")
        elif status == "delivered":
    # Reward student immediately for completing the order
            order_id = order.get("id")
            student_id = order.get("user_id")
            if student_id:
                async with db._open_connection() as conn:
                    await conn.execute(
                        "UPDATE users SET xp = xp + 10 WHERE id=$1",
                        student_id
                    )
                await bot.send_message(
                    student_tg,
                    f"🎉 Order #{order_id} delivered!\n"
                    f"🔥 You earned +10 XP for ordering with our bot."
                )

            # Rating prompt
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(text="⭐1", callback_data=f"rate_delivery:{order_id}:1"),
                    InlineKeyboardButton(text="⭐2", callback_data=f"rate_delivery:{order_id}:2"),
                    InlineKeyboardButton(text="⭐3", callback_data=f"rate_delivery:{order_id}:3"),
                    InlineKeyboardButton(text="⭐4", callback_data=f"rate_delivery:{order_id}:4"),
                    InlineKeyboardButton(text="⭐5", callback_data=f"rate_delivery:{order_id}:5"),
                ]]
            )
            await bot.send_message(
                student_tg,
                f"🍽 Enjoy your meal!\n\n⭐ Please rate the delivery:",
                reply_markup=kb
            )
    except Exception:
        log.exception("notify_student: failed to send message to student %s", student_tg)