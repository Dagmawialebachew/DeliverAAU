"""
Shared async helper utilities.
"""

import asyncio
import logging
from typing import Any, Dict, Optional
from aiogram import Bot
from aiogram.types import Message
from datetime import datetime
import aiosqlite
import json
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError 
from database import db
from utils.db_helpers import get_student_chat_id
from handlers import delivery_guy

async def send_typing_action(bot: Bot, chat_id: int, duration: float = 0.8) -> None:
    """
    Send typing action indicator, then pause briefly (non-blocking).
    """
    try:
        await bot.send_chat_action(chat_id, "typing")
    except Exception:
        pass
    await asyncio.sleep(duration)


async def typing_pause(message: Message, text: str, delay: float = 0.6) -> None:
    """
    Convenience: show typing, wait, then send message.
    """
    await send_typing_action(message.bot, message.chat.id, duration=delay)
    await message.answer(text)


def format_phone_number(phone: str) -> str:
    """
    Normalize Ethiopian phone numbers to +251XXXXXXXXX.
    """
    cleaned = "".join(filter(str.isdigit, phone or ""))
    if not cleaned:
        return ""

    if cleaned.startswith("251"):
        return f"+{cleaned}"

    if cleaned.startswith("0"):
        cleaned = "251" + cleaned[1:]
    else:
        cleaned = "251" + cleaned

    return f"+{cleaned}"


def calculate_level(xp: int) -> int:
    """
    Level grows by 100 * current_level XP per level.
    """
    level = 1
    remaining = max(xp, 0)
    while remaining >= level * 100:
        remaining -= level * 100
        level += 1
    return level


def xp_for_next_level(current_xp: int, current_level: int) -> int:
    """
    XP needed to reach next level threshold.
    """
    needed = (current_level * 100) - current_xp
    return max(needed, 0)


def format_datetime(dt_str: str) -> str:
    """
    Format ISO datetime to user-friendly string.
    """
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return dt_str or ""


async def safe_delete_message(message: Message) -> bool:
    """
    Delete message safely; returns False on failure.
    """
    try:
        await message.delete()
        return True
    except Exception:
        return False


async def safe_edit_message(message: Message, text: str, **kwargs) -> Optional[Message]:
    """
    Edit message safely; returns None on failure.
    """
    try:
        return await message.edit_text(text, **kwargs)
    except Exception:
        return None


from math import log, radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371000  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c



# Async function to assign a delivery guy to an order based on proximity or campus


import aiosqlite, json
from math import radians, sin, cos, sqrt, atan2

# helpers.py
from math import radians, sin, cos, sqrt, atan2

async def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Returns the distance in meters between two points using the Haversine formula.
    """
    R = 6371000  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


async def distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Returns distance in kilometers.
    """
    meters = await haversine(lat1, lon1, lat2, lon2)
    return meters / 1000


async def estimate_eta(distance_km: float, speed_kmh: float = 40.0) -> int:
    """
    Estimate ETA in minutes given distance in km and speed in km/h.
    """
    if speed_kmh <= 0:
        return -1
    return int((distance_km / speed_kmh) * 60)


async def eta_and_distance(lat1: float, lon1: float, lat2: float, lon2: float, speed_kmh: float = 40.0) -> dict:
    """
    Convenience helper returning both distance and ETA.
    """
    dist_km = await distance_km(lat1, lon1, lat2, lon2)
    eta_min = await estimate_eta(dist_km, speed_kmh)
    return {"distance_km": dist_km, "eta_min": eta_min}




logging.basicConfig(level=logging.INFO)




async def assign_delivery_guy(
    db_path: str,
    order_id: int,
    bot=None,
    current_order_data: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Assigns the best available delivery guy to an order.
    - Excludes DGs who recently skipped this specific order (blacklist).
    - Allows up to 5 concurrent active orders per DG.
    - Excludes DGs who already have any 'in_progress' order.
    - Ranks by proximity (Haversine distance) or campus fallback.
    """
    logging.info(f"[START] Assigning delivery guy for Order ID: {order_id}")

    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row

        # --- Fetch or use provided order data ---
        if current_order_data:
            order = current_order_data
        else:
            async with conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cur:
                row = await cur.fetchone()
            if not row:
                logging.warning(f"[ABORT] Order ID {order_id} not found")
                return None
            order = dict(row)

        logging.info(f"[ORDER] Fetched order: {order}")

        # --- Identify DGs who have rejected this order (Blacklist) ---
        rejected_dg_ids = []
        try:
            breakdown = json.loads(order.get("breakdown_json") or "{}")
            rejected_dg_ids = breakdown.get("rejected_by_dg_ids", [])
            logging.info(f"[BLACKLIST] Order {order_id} rejected by DGs: {rejected_dg_ids}")
        except Exception:
            logging.error(f"[ERROR] Failed to parse breakdown_json for blacklist: {order_id}")
            breakdown = {}

        # --- Fetch active delivery guys ---
        async with conn.execute("SELECT * FROM delivery_guys WHERE active = 1 AND blocked = 0") as cur:
            candidates = [dict(c) for c in await cur.fetchall()]
        if not candidates:
            logging.warning("[ABORT] No active delivery guys")
            return None
        logging.info(f"[CANDIDATES] {len(candidates)} active delivery guys found")

        # --- Filter by blacklist, capacity, and in-progress ---
        eligible = []
        for dg in candidates:
            dg_id = dg["id"]

            # Skip if DG is blacklisted
            if dg_id in rejected_dg_ids:
                logging.info(f"[DG FILTER] {dg['name']} (ID: {dg_id}) skipped this order. Skipping.")
                continue

            # Capacity filter
            async with conn.execute(
                "SELECT COUNT(*) FROM orders WHERE delivery_guy_id = ? AND status IN ('assigned','accepted','in_progress')",
                (dg_id,)
            ) as cur:
                active_count = (await cur.fetchone())[0]

            # In-progress filter
            async with conn.execute(
                "SELECT COUNT(*) FROM orders WHERE delivery_guy_id = ? AND status = 'in_progress'",
                (dg_id,)
            ) as cur:
                in_progress_count = (await cur.fetchone())[0]

            logging.info(f"[DG CHECK] {dg['name']} active_count={active_count}, in_progress={in_progress_count}")

            if active_count < 5 and in_progress_count == 0:
                eligible.append(dg)

        if not eligible:
            logging.warning("[ABORT] All eligible DGs are either blacklisted, at capacity, or busy.")
            return None

        # --- Extract dropoff coordinates ---
        drop_lat, drop_lon = breakdown.get("drop_lat"), breakdown.get("drop_lon")
        logging.info(f"[COORDS] Drop-off coords: {drop_lat}, {drop_lon}")

        # --- Choose best DG ---
        chosen: Optional[Dict[str, Any]] = None
        if drop_lat and drop_lon:
            logging.info("[MATCH] Ranking by distance")
            min_dist = float("inf")
            for dg in eligible:
                if dg.get("last_lat") and dg.get("last_lon"):
                    dist = await haversine(dg["last_lat"], dg["last_lon"], drop_lat, drop_lon)
                    logging.info(f"[DIST] DG {dg['name']} distance: {dist:.2f} m")
                    if dist < min_dist:
                        min_dist = dist
                        chosen = dg
            if not chosen:
                chosen = eligible[0]
                logging.info(f"[FALLBACK] No distance data available, pick first: {chosen['name']}")
        else:
            logging.info("[MATCH] Campus fallback")
            for dg in eligible:
                if dg["campus"] == order.get("pickup"):
                    chosen = dg
                    logging.info(f"[MATCH] Campus match: {chosen['name']}")
                    break
            if not chosen:
                chosen = eligible[0]
                logging.info(f"[FALLBACK] No campus match, pick first: {chosen['name']}")

        if not chosen:
            logging.warning("[ABORT] No delivery guy chosen after filtering and matching.")
            return None

        # --- Assign DG to order ---
        await conn.execute(
            "UPDATE orders SET delivery_guy_id = ?, status = 'assigned' WHERE id = ?",
            (chosen["id"], order_id)
        )
        await conn.execute(
            "UPDATE delivery_guys SET total_requests = total_requests + 1 WHERE id = ?",
            (chosen["id"],)
        )
        await conn.commit()
        logging.info(f"[ASSIGN] Assigned DG {chosen['name']} (ID: {chosen['id']}) to order {order_id}")

        # --- Enrich chosen with order info ---
        chosen.update({
            "order_id": order_id,
            "pickup": order.get("pickup"),
            "dropoff": order.get("dropoff"),
            "food_subtotal": order.get("food_subtotal"),
            "delivery_fee": order.get("delivery_fee"),
        })
        logging.info(f"[CHOSEN] DG enriched: {chosen}")

        # --- Notifications ---
        if bot:
            logging.info("[NOTIFY] Sending notifications...")
            try:
                await delivery_guy.send_new_order_offer(bot, chosen, order)
                await delivery_guy.notify_student(bot, order, status="assigned")
            except Exception:
                logging.exception("[ERROR] Notification failed for order %s", order_id)

        logging.info("[END] Assignment complete")
        return chosen
    
    



# Global tracker for student notifications
STUDENT_NOTIFICATIONS: Dict[int, Dict[str, Any]] = {}

async def notify_student_reassignment(bot: Bot, db: db.Database, order: Dict[str, Any], chosen: Dict[str, Any]) -> None:
    """
    Notify the student about reassignment without spamming.
    - If a notification already exists, edit it.
    - If not, send a new one and track its message_id.
    """
    # Resolve the student's Telegram chat_id from the order
    student_chat_id = await db.get_student_chat_id(order)
    if not student_chat_id:
        log.warning("No valid Telegram ID found for order %s", order["id"])
        return

    new_text = (
        "🔄 **Update on Your Order**\n\n"
        f"📦 Order #{order['id']} is still on track!\n"
        f"A new delivery partner has been assigned:\n"
        f"👤 {chosen.get('name', 'Partner')} • 🏛 {chosen.get('campus', 'N/A')}\n\n"
        "✅ No action needed — your delivery is being handled smoothly."
    )

    notif = STUDENT_NOTIFICATIONS.get(order["id"])
    try:
        if notif:
            # Try editing the existing message
            await bot.edit_message_text(
                chat_id=notif["chat_id"],
                message_id=notif["message_id"],
                text=new_text,
                parse_mode="Markdown"
            )
        else:
            # Send a new message and track it
            sent_message = await bot.send_message(student_chat_id, new_text, parse_mode="Markdown")
            STUDENT_NOTIFICATIONS[order["id"]] = {
                "chat_id": student_chat_id,
                "message_id": sent_message.message_id
            }
    except TelegramBadRequest:
        # If editing fails (deleted, etc.), send fresh message
        sent_message = await bot.send_message(student_chat_id, new_text, parse_mode="Markdown")
        STUDENT_NOTIFICATIONS[order["id"]] = {
            "chat_id": student_chat_id,
            "message_id": sent_message.message_id
        }
