"""
Shared async helper utilities.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
from aiogram import Bot
from aiogram.types import Message
import datetime
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
    db,
    order_id: int,
    bot=None,
    current_order_data: Optional[Dict[str, Any]] = None,
    max_active_orders: int = 5
) -> Optional[Dict[str, Any]]:
    """
    Assigns the best available delivery guy to an order using PostgreSQL.
    """
    logging.info(f"[START] Assigning delivery guy for Order ID: {order_id}")

    async with db._open_connection() as conn:
        # --- Fetch Order Data ---
        if current_order_data:
            order = current_order_data
        else:
            row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            if not row:
                logging.warning(f"[ABORT] Order ID {order_id} not found")
                return None
            order = dict(row)   # replace self._row_to_dict with dict()

        logging.info(f"[ORDER] Fetched order: {order}")

        # --- Identify DGs who have rejected this order (Blacklist) ---
        rejected_dg_ids: List[int] = []
        breakdown: Dict[str, Any] = {}
        try:
            breakdown = order.get("breakdown_json") or {}
            if isinstance(breakdown, str):
                breakdown = json.loads(breakdown)
            rejected_dg_ids = breakdown.get("rejected_by_dg_ids", [])
            logging.info(f"[BLACKLIST] Order {order_id} rejected by DGs: {rejected_dg_ids}")
        except Exception:
            logging.error(f"[ERROR] Failed to parse breakdown_json for blacklist: {order_id}")
            breakdown = {}

        # --- Fetch and Filter Active Delivery Guys ---
        query = """
        WITH dg_active_counts AS (
            SELECT 
                delivery_guy_id AS dg_id, 
                COUNT(*) FILTER (WHERE status IN ('assigned','preparing','ready','in_progress')) AS active_count,
                COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress_count
            FROM orders
            GROUP BY delivery_guy_id
        )
        SELECT 
            dg.*,
            COALESCE(dac.active_count, 0) AS active_orders,
            COALESCE(dac.in_progress_count, 0) AS in_progress_orders
        FROM delivery_guys dg
        LEFT JOIN dg_active_counts dac ON dg.id = dac.dg_id
       WHERE 
        dg.active = TRUE
        AND dg.blocked = FALSE
        AND (
            COALESCE(array_length($1::int[], 1), 0) = 0
            OR dg.id != ALL($1::int[])
        )
        AND COALESCE(dac.active_count, 0) < $2
        AND COALESCE(dac.in_progress_count, 0) = 0;
        """

        candidates = [dict(r) for r in await conn.fetch(query, rejected_dg_ids, max_active_orders)]

        if not candidates:
            logging.warning("[ABORT] No delivery guy is eligible after filtering.")
            return None
        logging.info(f"[CANDIDATES] {len(candidates)} eligible delivery guys found")

        # --- Choose best DG ---
        drop_lat, drop_lon = breakdown.get("drop_lat"), breakdown.get("drop_lon")
        chosen: Optional[Dict[str, Any]] = None

        if drop_lat and drop_lon:
            logging.info("[MATCH] Ranking by distance")
            min_dist = float("inf")
            for dg in candidates:
                if dg.get("last_lat") and dg.get("last_lon"):
                    dist = await haversine(dg["last_lat"], dg["last_lon"], drop_lat, drop_lon)
                    if dist < min_dist:
                        min_dist = dist
                        chosen = dg
            if not chosen:
                chosen = candidates[0]
                logging.info(f"[FALLBACK] No distance data, picked first eligible: {chosen['name']}")
        else:
            logging.info("[MATCH] Campus fallback")
            student = await db.get_user_by_id(order["user_id"])
            student_campus = student.get("campus")

            logging.info("[MATCH] Campus fallback")
            for dg in candidates:
                if dg.get("campus") == student_campus:
                    chosen = dg
                    logging.info(f"[MATCH] Campus match: {chosen['name']} for campus {student_campus}")
                    break

            if not chosen and candidates:
                chosen = candidates[0]
                logging.info(f"[FALLBACK] No campus match, picked first eligible: {chosen['name']}")
        if not chosen:
            logging.warning("[ABORT] No delivery guy chosen after filtering and matching.")
            return None

        dg_id = chosen["id"]

        # --- Assign DG to order & Update metrics ---
        await conn.execute(
            "UPDATE orders SET delivery_guy_id = $1, status = 'assigned' WHERE id = $2",
            dg_id, order_id
        )
        await conn.execute(
            "UPDATE delivery_guys SET total_requests = total_requests + 1 WHERE id = $1",
            dg_id
        )
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        await conn.execute(
            """
            INSERT INTO daily_stats (dg_id, date, assigned, updated_at)
            VALUES ($1, $2, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(dg_id, date) DO UPDATE SET
                assigned = daily_stats.assigned + 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            dg_id, today_str
        )

        logging.info(f"[ASSIGN] Assigned DG {chosen['name']} (ID: {chosen['id']}) to order {order_id}")

        chosen.update({
            "order_id": order_id,
            "pickup": order.get("pickup"),
            "dropoff": order.get("dropoff"),
            "food_subtotal": order.get("food_subtotal"),
            "delivery_fee": order.get("delivery_fee"),
            "user_id": order.get("user_id")
        })

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



import datetime

def time_ago(dt: datetime.datetime) -> str:
    if not dt:
        return "—"

    # Normalize: if dt has no tzinfo, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    now = datetime.datetime.now(datetime.timezone.utc)
    diff = now - dt

    seconds = diff.total_seconds()
    minutes = int(seconds // 60)
    hours = int(seconds // 3600)
    days = int(seconds // 86400)

    if seconds < 60:
        return f"{int(seconds)}s ago"
    elif minutes < 60:
        return f"{minutes}m ago"
    elif hours < 24:
        return f"{hours}h ago"
    elif days == 1:
        return "yesterday"
    else:
        return f"{days}d ago"
