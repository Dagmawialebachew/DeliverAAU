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


def render_cart(cart_counts: dict, menu: list) -> tuple[str, float]:
    """
    Build a human-readable cart summary and compute subtotal.
    Returns (cart_text, subtotal).
    """
    lines = []
    subtotal = 0
    for item_id, qty in cart_counts.items():
        item = next((m for m in menu if m["id"] == item_id), None)
        if not item:
            continue
        subtotal += item["price"] * qty
        if qty > 1:
            lines.append(f"• {item['name']} x{qty}")
        else:
            lines.append(f"• {item['name']}")
    cart_text = "\n".join(lines) or "—"
    return cart_text, subtotal



async def rank_candidates(db, order: Dict[str, Any], candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply dorm+gender preference, shuffle, and reliability scoring to candidates.
    Returns a sorted list of candidates with detailed logging.
    """
    import random

    logging.info(f"[RANK] Starting candidate ranking for Order {order.get('id')} with {len(candidates)} candidates")

    # Shuffle to avoid bias
    random.shuffle(candidates)
    logging.info("[RANK] Candidates shuffled to avoid bias")

    # Dorm + gender preference
    pickup_text = (order.get("pickup") or "").lower()
    dropoff_text = (order.get("dropoff") or "").lower()
    is_dorm_related = any(word in pickup_text for word in ["dorm", "dormitory", "residence", "hall"]) \
                      or any(word in dropoff_text for word in ["dorm", "dormitory", "residence", "hall"])

    student = await db.get_user_by_id(order["user_id"])
    student_gender = (student.get("gender") or "").lower()
    logging.info(f"[RANK] Student gender={student_gender}, dorm_related={is_dorm_related}")

    if is_dorm_related and student_gender == "female":
        female_candidates = [dg for dg in candidates if (dg.get("gender") or "").lower() == "female"]
        logging.info(f"[RANK] Dorm+female rule applied → {len(female_candidates)} female DGs found")
        if female_candidates:
            candidates = female_candidates
        else:
            logging.info("[RANK] No female DGs available, fallback to general pool")

    # Reliability scoring
    for dg in candidates:
        total = dg.get("total_requests", 0)
        accepted = dg.get("accepted_requests", 0)
        deliveries = dg.get("total_deliveries", 0)
        skipped = dg.get("skipped_requests", 0)

        acceptance_rate = (accepted / total) if total > 0 else 1.0
        # Weighted score: 50% acceptance, 30% deliveries, -20% skips
        score = (acceptance_rate * 50) + (deliveries * 0.3) - (skipped * 0.2)
        dg["score"] = score

        logging.info(
            f"[RANK] DG {dg.get('name','?')} "
            f"(id={dg.get('id')}) → total={total}, accepted={accepted}, skipped={skipped}, deliveries={deliveries}, "
            f"acceptance_rate={acceptance_rate:.2f}, score={score:.2f}"
        )

    # Sort by score descending
    candidates.sort(key=lambda x: x["score"], reverse=True)
    logging.info("[RANK] Candidates sorted by reliability score")

    return candidates



def campus_priority_order(student_campus: str) -> list[str]:
    priority_map = {
        "6kilo": ["6kilo", "FBE", "5kilo", "4kilo"],
        "FBE":   ["FBE", "6kilo", "5kilo", "4kilo"],
        "5kilo": ["5kilo", "4kilo", "6kilo", "FBE"],
        "4kilo": ["4kilo", "5kilo", "6kilo", "FBE"],
    }
    return priority_map.get(student_campus, [])


async def assign_delivery_guy(
    db,
    order_id: int,
    bot=None,
    current_order_data: Optional[Dict[str, Any]] = None,
    max_active_orders: int = 5,
    notify_student: bool = False   # <-- IMPORTANT FLAG
) -> Optional[Dict[str, Any]]:
    """
    Assigns the best available delivery guy to an order.
    Safe for scheduler. Does NOT notify student unless explicitly asked.
    """

    logging.info(f"[START] Assigning delivery guy for Order ID: {order_id}")

    async with db._open_connection() as conn:

        # -----------------------------
        # 1. Fetch Order
        # -----------------------------
        if current_order_data:
            order = current_order_data
        else:
            row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            if not row:
                logging.warning(f"[ABORT] Order ID {order_id} not found")
                return None
            order = dict(row)

        logging.info(f"[ORDER] {order}")

        # -----------------------------
        # 2. Parse blacklist (DGs who rejected this order)
        # -----------------------------
        try:
            breakdown = order.get("breakdown_json") or {}
            if isinstance(breakdown, str):
                breakdown = json.loads(breakdown)
            rejected_dg_ids = breakdown.get("rejected_by_dg_ids", [])
        except Exception:
            logging.error(f"[ERROR] breakdown_json corrupted for order {order_id}")
            rejected_dg_ids = []

        logging.info(f"[BLACKLIST] Rejected DGs: {rejected_dg_ids}")

        # -----------------------------
        # 3. Fetch eligible delivery guys
        # -----------------------------
        query = """
        WITH dg_active_counts AS (
            SELECT 
                delivery_guy_id AS dg_id,
                COUNT(*) FILTER (WHERE status IN 
                    ('assigned','preparing','ready','in_progress')
                ) AS active_count,
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
            AND (COALESCE(array_length($1::int[], 1),0) = 0 OR dg.id != ALL($1::int[]))
            AND COALESCE(dac.active_count,0) < $2
            AND COALESCE(dac.in_progress_count,0) = 0
        """
        candidates = [dict(r) for r in await conn.fetch(query, rejected_dg_ids, max_active_orders)]
        logging.info(f"[QUERY] rejected_dg_ids={rejected_dg_ids} max_active_orders={max_active_orders}")
        rows = await conn.fetch(query, rejected_dg_ids, max_active_orders)
        logging.info(f"[QUERY] returned {len(rows)} candidates")
        for r in rows:
            dg = dict(r)
            logging.info(
                f"[CAND] id={dg.get('id')} name={dg.get('name')} campus={dg.get('campus')} "
                f"gender={dg.get('gender')} active={dg.get('active')} blocked={dg.get('blocked')} "
                f"active_orders={dg.get('active_orders')} in_progress_orders={dg.get('in_progress_orders')}"
            )

        if not candidates:
            logging.warning(f"[NO CANDIDATES] Order {order_id}")
            return None

        logging.info(f"[CANDIDATES] {len(candidates)} DGs available")

        # -----------------------------
        # 4. Rank candidates (shuffle + dorm/gender + reliability score)
        # -----------------------------
        candidates = await rank_candidates(db, order, candidates)

        # -----------------------------
        # 5. Match Logic (GPS or campus)
        # -----------------------------
        chosen = None
        breakdown = breakdown or {}
        student = await db.get_user_by_id(order["user_id"])
        if not student:
            logging.warning(f"[MATCH] No student found for order {order_id}")
            return None
        drop_lat, drop_lon = breakdown.get("drop_lat"), breakdown.get("drop_lon")

        if drop_lat and drop_lon:
            logging.info("[MATCH] Using distance")
            best_dist = float("inf")
            for dg in candidates:
                if dg.get("last_lat") and dg.get("last_lon"):
                    d = await haversine(dg["last_lat"], dg["last_lon"], drop_lat, drop_lon)
                    logging.info(f"[MATCH] DG {dg.get('name')} distance={d:.2f} km")
                    if d < best_dist:
                        best_dist = d
                        chosen = dg
            if not chosen and candidates:
                chosen = candidates[0]
                logging.info(f"[MATCH] No GPS match, fallback to {chosen.get('name')}")
        else:
            logging.info("[MATCH] Campus fallback")
            student_campus = student.get("campus") if student else None
            priority = campus_priority_order(student_campus)

            for campus in priority:
                logging.info("[CAMPUS_CHECK] Trying campus=%s", campus)

                campus_candidates = [
                    dg for dg in candidates if dg.get("campus") == campus
                ]

                logging.info(
                    "[CAMPUS_CHECK] Found %d candidate(s) in campus=%s",
                    len(campus_candidates),
                    campus
                )

                for dg in campus_candidates:
                    logging.info(
                        "[CAMPUS_CANDIDATE] id=%s name=%s active_orders=%s score=%.2f",
                        dg.get("id"),
                        dg.get("name"),
                        dg.get("active_orders"),
                        dg.get("score", 0.0),
                    )

                    chosen = dg
                    logging.info(
                        "[CAMPUS_SELECTED] DG %s (%s) selected via campus=%s",
                        dg.get("name"),
                        dg.get("id"),
                        campus
                    )
                    break

                if chosen:
                    break
            if not chosen and candidates:
                chosen = candidates[0]
                logging.info(f"[MATCH] No campus match, fallback to {chosen.get('name')}")

        if not chosen:
            logging.warning(f"[ABORT] No DG chosen for order {order_id}")
            return None

        dg_id = chosen["id"]

        # -----------------------------
        # 5. Assign DG to order
        # -----------------------------
        breakdown.setdefault("pending_offer_dg_ids", []).append(dg_id)

        await conn.execute(
            "UPDATE orders SET breakdown_json = $1 WHERE id = $2",
            json.dumps(breakdown), order_id
        )
        # await conn.execute(
        #     "UPDATE delivery_guys SET total_requests = total_requests + 1 WHERE id = $1",
        #     dg_id
        # )

        today = datetime.date.today()
        await conn.execute(
            """
            INSERT INTO daily_stats (dg_id, date, assigned, updated_at)
            VALUES ($1, $2, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (dg_id, date)
            DO UPDATE SET assigned = daily_stats.assigned + 1,
                          updated_at = CURRENT_TIMESTAMP
            """,
            dg_id, today
        )

        logging.info(f"[ASSIGNED] DG {chosen['name']} → Order {order_id}")

        chosen.update({
            "order_id": order_id,
            "pickup": order.get("pickup"),
            "dropoff": order.get("dropoff"),
            "user_id": order.get("user_id"),
            "food_subtotal": order.get("food_subtotal"),
            "delivery_fee": order.get("delivery_fee"),
        })

    # -----------------------------
    # 6. SEND NOTIFICATIONS  
    # -----------------------------
    if bot:
        try:
            # DG ALWAYS gets offer
            await delivery_guy.send_new_order_offer(bot, chosen, order)

            # Student ONLY if explicitly asked (vendor_accept_order)
            if notify_student:
                await delivery_guy.notify_student(bot, order, status="assigned")

        except Exception:
            logging.exception(f"[ERROR] Notification failure for order {order_id}")

    logging.info(f"[END] Assignment for order {order_id}")
    return chosen


async def find_next_candidate(db, order_id: int, order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns the next eligible delivery guy dict to offer the order to,
    excluding those in the order's rejected_by_dg_ids list and respecting
    active order limits. Does NOT assign the DG.
    """
    async with db._open_connection() as conn:
        # Load blacklist from order
        row = await conn.fetchrow("SELECT breakdown_json FROM orders WHERE id = $1", order_id)
        breakdown = {}
        if row:
            raw = row.get("breakdown_json") or {}
            if isinstance(raw, str):
                try:
                    breakdown = json.loads(raw)
                except Exception:
                    breakdown = {}
            elif isinstance(raw, dict):
                breakdown = raw
        rejected = breakdown.get("rejected_by_dg_ids", [])

        # Query candidates similar to assign_delivery_guy but do not update DB
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
            AND (COALESCE(array_length($1::int[], 1),0) = 0 OR dg.id != ALL($1::int[]))
            AND COALESCE(dac.active_count,0) < $2
            AND COALESCE(dac.in_progress_count,0) = 0
        """
        candidates = [dict(r) for r in await conn.fetch(query, rejected, 5)]

        if not candidates:
            return None

        logging.info(f"[CANDIDATES] {len(candidates)} DGs available for retry")

        # 🔎 NEW STEP: rank candidates (shuffle + dorm/gender + reliability scoring)
        candidates = await rank_candidates(db, order, candidates)

        # -----------------------------
        # Matching logic: prefer distance if coords exist, else campus match
        # -----------------------------
        try:
            breakdown = breakdown or {}
            drop_lat, drop_lon = breakdown.get("drop_lat"), breakdown.get("drop_lon")
            chosen = None
            if drop_lat and drop_lon:
                best_dist = float("inf")
                for dg in candidates:
                    if dg.get("last_lat") and dg.get("last_lon"):
                        d = await haversine(dg["last_lat"], dg["last_lon"], drop_lat, drop_lon)
                        logging.info(f"[MATCH] DG {dg.get('name')} distance={d:.2f} km")
                        if d < best_dist:
                            best_dist = d
                            chosen = dg
                if not chosen and candidates:
                    chosen = candidates[0]
                    logging.info(f"[MATCH] No GPS match, fallback to {chosen.get('name')}")
            else:
                # campus fallback
                student = await db.get_user_by_id(order["user_id"])
                student_campus = student.get("campus") if student else None
                priority = campus_priority_order(student_campus)
                
                logging.info(
    "[CAMPUS_FALLBACK] Student campus=%s | Priority order=%s",
    student_campus,
    " → ".join(priority) if priority else "NONE"
)


                for campus in priority:
                    logging.info("[CAMPUS_CHECK] Trying campus=%s", campus)

                    campus_candidates = [
                        dg for dg in candidates if dg.get("campus") == campus
                    ]

                    logging.info(
                        "[CAMPUS_CHECK] Found %d candidate(s) in campus=%s",
                        len(campus_candidates),
                        campus
                    )

                    for dg in campus_candidates:
                        logging.info(
                            "[CAMPUS_CANDIDATE] id=%s name=%s active_orders=%s score=%.2f",
                            dg.get("id"),
                            dg.get("name"),
                            dg.get("active_orders"),
                            dg.get("score", 0.0),
                        )

                        chosen = dg
                        logging.info(
                            "[CAMPUS_SELECTED] DG %s (%s) selected via campus=%s",
                            dg.get("name"),
                            dg.get("id"),
                            campus
                        )
                        break

                    if chosen:
                        break
                if not chosen and candidates:
                    chosen = candidates[0]
                    logging.info(f"[MATCH] No campus match, fallback to {chosen.get('name')}")

            if chosen:
                logging.info(
                    f"[CHOSEN] Next candidate → DG {chosen.get('name')} "
                    f"(id={chosen.get('id')}, score={chosen.get('score',0):.2f})"
                )
            return chosen
        except Exception:
            logging.exception("[FIND_CANDIDATE] Error selecting candidate for order %s", order_id)
            return candidates[0] if candidates else None

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




def time_ago_am(dt: datetime.datetime) -> str:
    if not dt:
        return "—"

    # Normalize: if dt has no tzinfo, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    now = datetime.datetime.now(datetime.timezone.utc)
    diff = now - dt

    seconds = int(diff.total_seconds())
    if seconds < 0:
        return "አሁን"

    minutes = seconds // 60
    hours = seconds // 3600
    days = seconds // 86400

    if seconds < 60:
        return f"{seconds} ሰከንድ በፊት"
    elif minutes < 60:
        return f"{minutes} ደቂቃ በፊት"
    elif hours < 24:
        return f"{hours} ሰአት በፊት"
    elif days == 1:
        return "ትናንትና"
    else:
        return f"{days} ቀን በፊት"


import json

def calculate_commission(items_json: str) -> dict:
    try:
        items = json.loads(items_json)
    except Exception:
        items = []

    total_commission = 0
    subtotal = 0

    for item in items:
        price = item.get("price", 0)
        qty = item.get("qty", 1)  # default to 1 if missing

        subtotal += price * qty

        # Commission per item logic (apply per unit)
        for _ in range(qty):
            if 100 <= price < 200:
                total_commission += 10
            elif 200 <= price < 300:
                total_commission += 15
            elif price >= 300:
                total_commission += 20
            # else: no commission

    return {
        "platform_share": total_commission,
        "vendor_share": subtotal - total_commission,
        "items": [f"{i.get('name')} x{i.get('qty',1)}" for i in items],
        "subtotal": subtotal
    }