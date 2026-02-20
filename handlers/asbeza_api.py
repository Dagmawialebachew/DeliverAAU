import datetime
from aiohttp import web
from typing import List, Dict
import math
import os
import uuid
import aiofiles

import app
from config import settings

ALLOWED_IMAGE_MIMES = {"image/jpeg", "image/png", "image/webp", "image/jpg"}


async def get_asbeza_items(request: web.Request) -> web.Response:
    """
    GET /api/asbeza/items
    Returns active items with their variants
    """
    db = request.app["db"]
    async with db._open_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT i.id, i.name, i.description, i.base_price, i.image_url,
                   COALESCE(
                     json_agg(
                       json_build_object(
                         'id', v.id,
                         'name', v.name,
                         'price', v.price,
                         'stock', v.stock,
                         'image_url', v.image_url
                       )
                     ) FILTER (WHERE v.id IS NOT NULL),
                     '[]'
                   ) AS variants
            FROM asbeza_items i
            LEFT JOIN asbeza_variants v ON v.item_id = i.id
            WHERE i.active = TRUE
            GROUP BY i.id
            ORDER BY i.id;
            """
        )
    items = [dict(r) for r in rows]
    return web.json_response({"items": items})


async def asbeza_checkout(request: web.Request) -> web.Response:
    """
    POST /api/asbeza/checkout
    Expects JSON:
    {
      "user_id": 12345,
      "items": [
        {"variant_id": 1, "quantity": 2},
        ...
      ],
      // optional client-side totals (will be validated server-side)
      "delivery_fee": 80,
      "total_price": 480,
      "upfront_paid": 192,
      // optional: payment_proof_url (public URL) or payment_proof_base64 (not required)
    }
    Returns:
    { "status": "ok", "order_id": 42, "upfront": 192 }
    """
    db = request.app["db"]

    # Delivery fee rule (server-side) — keep existing logic
    def compute_delivery_fee(subtotal: float) -> float:
        base = 80.0
        if subtotal >= 500:
            return base + round(subtotal * 0.01)
        return base

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "invalid json"}, status=400)

    user_id = int(payload.get("user_id"))
    items: List[Dict] = payload.get("items", [])
    if not user_id:
      return web.json_response({"status": "error", "message": "user_id is required"}, status=400)


    if items is None or not isinstance(items, list) or len(items) == 0:
        return web.json_response({"status": "error", "message": "items are required"}, status=400)

    # Validate item structure and collect variant ids
    variant_ids: List[int] = []
    validated_items_input: List[Dict] = []
    for idx, it in enumerate(items):
        try:
            variant_id = int(it.get("variant_id"))
            quantity = int(it.get("quantity", 1))
            if quantity <= 0:
                raise ValueError()
        except Exception:
            return web.json_response({"status": "error", "message": f"invalid item at index {idx}"}, status=400)
        variant_ids.append(variant_id)
        validated_items_input.append({"variant_id": variant_id, "quantity": quantity})

    # Optional client-provided fields (for validation only)
    client_delivery = payload.get("delivery_fee")
    client_total = payload.get("total_price")
    client_upfront = payload.get("upfront_paid")

    # Optional payment proof URL (frontend may upload and send this)
    payment_proof_url = payload.get("payment_proof_url")  # e.g., "/uploads/abcd.jpg" or full URL
    payment_proof_base64 = payload.get("payment_proof_base64")
    

    # All DB operations must happen while connection is open
    try:
        async with db._open_connection() as conn:
            # fetch variants in one query
            rows = await conn.fetch(
                "SELECT id, price FROM asbeza_variants WHERE id = ANY($1::int[])", variant_ids
            )
            # build map
            price_map = {r["id"]: float(r["price"]) for r in rows}

            # ensure all variant ids exist
            missing = [vid for vid in variant_ids if vid not in price_map]
            if missing:
                return web.json_response({"status": "error", "message": f"variant(s) not found: {missing}"}, status=400)

            # compute subtotal using DB prices (server authoritative)
            subtotal = 0.0
            for it in validated_items_input:
                vid = it["variant_id"]
                qty = it["quantity"]
                unit_price = price_map[vid]
                subtotal += unit_price * qty

            # compute delivery fee, total, upfront
            delivery_fee = compute_delivery_fee(subtotal)
            total_price = subtotal + delivery_fee
            upfront_paid = int(math.floor(total_price * 0.4))

            # If client provided totals, validate they match server computation
            if client_delivery is not None:
                try:
                    if float(client_delivery) != float(delivery_fee):
                        return web.json_response({"status": "error", "message": "delivery_fee mismatch"}, status=400)
                except Exception:
                    return web.json_response({"status": "error", "message": "invalid delivery_fee"}, status=400)

            if client_total is not None:
                try:
                    if float(client_total) != float(total_price):
                        return web.json_response({"status": "error", "message": "total_price mismatch"}, status=400)
                except Exception:
                    return web.json_response({"status": "error", "message": "invalid total_price"}, status=400)

            if client_upfront is not None:
                try:
                    if int(client_upfront) != int(upfront_paid):
                        return web.json_response({"status": "error", "message": "upfront_paid mismatch"}, status=400)
                except Exception:
                    return web.json_response({"status": "error", "message": "invalid upfront_paid"}, status=400)

            # Insert order (server authoritative totals)
            order_id = await conn.fetchval(
                """
                INSERT INTO asbeza_orders (user_id, total_price, delivery_fee, upfront_paid, status)
                VALUES ($1, $2, $3, $4, 'pending')
                RETURNING id
                """,
                user_id, total_price, delivery_fee, upfront_paid
            )

            # Insert order items using DB prices (store price per item)
            for it in validated_items_input:
                vid = it["variant_id"]
                qty = it["quantity"]
                unit_price = price_map[vid]
                await conn.execute(
                    """
                    INSERT INTO asbeza_order_items (order_id, variant_id, quantity, price)
                    VALUES ($1, $2, $3, $4)
                    """,
                    order_id, vid, qty, unit_price
                )
            
            if payment_proof_base64:
                await conn.execute(
                    """
                    INSERT INTO asbeza_order_payments (order_id, user_id, amount, payment_proof_url, method, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    """,
                    order_id, user_id, upfront_paid, payment_proof_base64, "base64"
                )
            elif payment_proof_url:
                await conn.execute(
                    """
                    INSERT INTO asbeza_order_payments (order_id, user_id, amount, payment_proof_url, method, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    """,
                    order_id, user_id, upfront_paid, payment_proof_url, "screenshot"
                )

            # --- Notify admin group here (consistent with other flows) ---
            # Build a compact items string for the admin message
            try:
                items_lines = []
                for it in validated_items_input:
                    vid = it["variant_id"]
                    qty = it["quantity"]
                    unit_price = price_map[vid]
                    items_lines.append(f"#{vid} ×{qty} @ {unit_price:.2f} birr")
                items_str = "\n".join(items_lines)

                if settings.ADMIN_DAILY_GROUP_ID:
                    admin_msg = (
                        f"📢 <b>New Asbeza Order: #{order_id}</b>\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"👤 User ID: {user_id}\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🛒 Items:\n{items_str}\n\n"
                        f"💵 Total: {total_price:.2f} birr\n"
                        f"🚚 Delivery: {delivery_fee:.2f} birr\n"
                        f"⚡ Upfront: {upfront_paid:.2f} birr\n\n"
                        "Check the admin web app for full details."
                    )
                    # Use the bot instance stored on the app (must be set during app startup)
                    try:
                        bot = request.app.get("bot")
                        if bot:
                            await bot.send_message(settings.ADMIN_DAILY_GROUP_ID, admin_msg, parse_mode="HTML")
                        else:
                            request.app.logger.warning("Bot instance not available on app; skipping admin notification.")
                    except Exception:
                        request.app.logger.exception(f"Failed to send admin notification for order {order_id}")
            except Exception:
                # Ensure admin notification building doesn't break checkout
                request.app.logger.exception("Failed to build/send admin notification")

            # --- If user has Telegram id, send them a confirmation image/message ---
            # If frontend provided a payment_proof_url, send that image back to the user with a short caption.
            try:
                bot = request.app.get("bot")
                if bot and user_id:
                    # prefer payment_proof_url if provided
                    if payment_proof_url:
                        # If the URL is relative (starts with '/'), build absolute URL if app has public base
                        public_base = request.app.get("public_base_url", "").rstrip("/")
                        if payment_proof_url.startswith("/"):
                            photo_url = f"{public_base}{payment_proof_url}" if public_base else payment_proof_url
                        else:
                            photo_url = payment_proof_url
                        caption = "✅ Your Asbeza order has been received and is pending confirmation. You can track it in the Asbeza admin web app."
                        try:
                            # send_photo may accept a URL depending on bot library; keep try/except to avoid breaking flow
                            await bot.send_photo(user_id, photo=photo_url, caption=caption, parse_mode="HTML")
                        except Exception:
                            # fallback to sending a text message if photo send fails
                            try:
                                await bot.send_message(user_id, caption, parse_mode="HTML")
                            except Exception:
                                request.app.logger.exception(f"Failed to notify user {user_id} with photo/message for order {order_id}")
                    else:
                        # No proof image provided — send a short text confirmation
                        try:
                            await bot.send_message(
                                user_id,
                                f"✅ Your Asbeza order #{order_id} has been received and is pending confirmation. Track it in the Asbeza admin web app.",
                                parse_mode="HTML"
                            )
                        except Exception:
                            request.app.logger.exception(f"Failed to send user notification for order {order_id}")
                else:
                    # No bot or no user_id — skip user notification
                    if not bot:
                        request.app.logger.debug("Bot instance not available; skipping user notification.")
                    if not user_id:
                        request.app.logger.debug("No user_id provided; skipping user notification.")
            except Exception:
                request.app.logger.exception("User notification failed (non-fatal)")

    except Exception:
        request.app.logger.exception("Checkout DB error")
        return web.json_response({"status": "error", "message": "internal server error"}, status=500)

    return web.json_response({"status": "ok", "order_id": order_id, "upfront": upfront_paid})


def setup_asbeza_routes(app: web.Application):
    # --- Public Routes ---
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)
    app.router.add_post("/api/asbeza/upload_screenshot", upload_screenshot)
    
    # --- Admin Auth ---
    app.router.add_post("/api/admin/login", admin_login)
    
    # --- Admin Management ---
    # Orders
    app.router.add_get("/api/admin/orders", list_orders)  # with filters & pagination
    app.router.add_get("/api/admin/orders/{id}", get_order_details) 
    app.router.add_post("/api/admin/orders/{id}/status", update_order_status)
    app.router.add_get("/api/admin/latest-orders", latest_orders)

    # Inventory
    app.router.add_post("/api/admin/add_item", add_item)

    # Users
    app.router.add_get("/api/admin/users/{id}", get_user_details)

    # --- Analytics Dashboard ---
    app.router.add_get("/api/admin/dashboard/stats", dashboard_stats)
    app.router.add_get("/api/admin/dashboard/order-status-breakdown", order_status_breakdown)
    app.router.add_get("/api/admin/dashboard/payment-method-split", payment_method_split)
    app.router.add_get("/api/admin/dashboard/fulfillment-speed", fulfillment_speed)
    app.router.add_get("/api/admin/dashboard/stock-alerts", stock_alerts)
    app.router.add_get("/api/admin/dashboard/order-heatmap", order_heatmap)
    app.router.add_get("/api/admin/dashboard/campus-distribution", campus_distribution)

import base64

async def upload_screenshot(request: web.Request) -> web.Response:
    """
    POST /api/asbeza/upload_screenshot
    Accepts multipart/form-data with field 'file'
    Returns: { "status":"ok", "url": "<data-url>" }
    """
    max_bytes = request.app.get("upload_max_bytes", 6 * 1024 * 1024)

    reader = await request.multipart()
    part = await reader.next()
    if part is None or part.name != "file":
        return web.json_response({"status": "error", "message": "file field is required"}, status=400)

    # Basic content-type check
    content_type = part.headers.get("Content-Type", "").lower()
    if content_type.split(";")[0] not in ALLOWED_IMAGE_MIMES:
        return web.json_response({"status": "error", "message": "unsupported file type"}, status=400)

    # Read file into memory with size limit
    data = b""
    size = 0
    while True:
        chunk = await part.read_chunk()
        if not chunk:
            break
        size += len(chunk)
        if size > max_bytes:
            return web.json_response({"status": "error", "message": "file too large"}, status=413)
        data += chunk

    # Encode as base64
    b64 = base64.b64encode(data).decode()
    data_url = f"data:{content_type};base64,{b64}"

    # Return inline data URL
    return web.json_response({"status": "ok", "url": data_url})




#Admin Page
import os, jwt, bcrypt, json
from aiohttp import web
from functools import wraps

SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "supersecret")

# --- AUTH MIDDLEWARE HELPER ---
def admin_required(f):
    @wraps(f)
    async def decorated(request, *args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return web.json_response({"status": "error", "message": "Unauthorized"}, status=401)
        
        token = auth_header.split(" ")[1]
        try:
            # In a real app, you'd check 'decoded' against your admin list
            jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        except:
            return web.json_response({"status": "error", "message": "Invalid session"}, status=401)
        return await f(request, *args, **kwargs)
    return decorated

# --- AUTH ENDPOINTS ---
async def admin_login(request: web.Request) -> web.Response:
    data = await request.json()
    username, password = data.get("username"), data.get("password")

    async with request.app["db"]._open_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM admins WHERE username=$1", username)
        if row and bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
            token = jwt.encode({"username": username}, SECRET_KEY, algorithm="HS256")
            return web.json_response({"status": "ok", "token": token})
    
    return web.json_response({"status": "error", "message": "Access Denied"}, status=401)

# --- INVENTORY MANAGEMENT (The "Insert Items" Logic) ---
@admin_required
async def add_item(request: web.Request) -> web.Response:
    data = await request.json()
    name = data.get("name")
    desc = data.get("description")
    price = data.get("base_price")
    img = data.get("image_url")
    variants = data.get("variants", []) # Expecting list of {name, price, stock}

    async with request.app["db"]._open_connection() as conn:
        async with conn.transaction():
            # 1. Insert Item
            item_id = await conn.fetchval("""
                INSERT INTO asbeza_items (name, description, base_price, image_url)
                VALUES ($1, $2, $3, $4) RETURNING id
            """, name, desc, price, img)

            # 2. Insert Variants
            for v in variants:
                await conn.execute("""
                    INSERT INTO asbeza_variants (item_id, name, price, stock)
                    VALUES ($1, $2, $3, $4)
                """, item_id, v['name'], v['price'], v.get('stock', 0))

    return web.json_response({"status": "ok", "message": "Product deployed to system"})

    
import datetime
from aiohttp import web

# -------------------------
# Helper: safe isoformat
# -------------------------
def iso(dt):
    return dt.isoformat() if isinstance(dt, (datetime.datetime, datetime.date)) else None

# -------------------------
# 1. Expanded dashboard stats
# GET /admin/dashboard/stats
# -------------------------
@admin_required
async def dashboard_stats(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        net_revenue = await conn.fetchval(
            "SELECT COALESCE(SUM(total_price),0) FROM asbeza_orders WHERE status != 'cancelled'"
        )
        pending_count = await conn.fetchval(
            "SELECT COUNT(*) FROM asbeza_orders WHERE status = 'pending'"
        )
        live_items = await conn.fetchval(
            "SELECT COUNT(*) FROM asbeza_items WHERE active = TRUE"
        )
        total_customers = await conn.fetchval(
            "SELECT COUNT(DISTINCT user_id) FROM asbeza_orders WHERE user_id IS NOT NULL"
        )
        repeat_customers = await conn.fetchval("""
            SELECT COUNT(*) FROM (
              SELECT user_id FROM asbeza_orders WHERE user_id IS NOT NULL GROUP BY user_id HAVING COUNT(*) > 1
            ) t
        """)
        aov = await conn.fetchval(
            "SELECT COALESCE(AVG(total_price),0) FROM asbeza_orders WHERE total_price IS NOT NULL"
        )

        # 7-day revenue trend (last 7 days including today)
        rows = await conn.fetch("""
            SELECT DATE(created_at) AS date, COALESCE(SUM(total_price),0) AS total
            FROM asbeza_orders
            WHERE created_at >= CURRENT_DATE - INTERVAL '6 days'
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at)
        """)
        trend = [{"date": r["date"].isoformat(), "total": float(r["total"])} for r in rows]

        # Top selling items by quantity
        top = await conn.fetch("""
            SELECT i.id AS item_id, i.name, COALESCE(SUM(oi.quantity),0) AS qty_sold
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            GROUP BY i.id, i.name
            ORDER BY qty_sold DESC
            LIMIT 6
        """)
        top_selling = [dict(r) for r in top]

    return web.json_response({
        "status": "ok",
        "kpis": {
            "net_revenue": float(net_revenue or 0),
            "pending_orders": int(pending_count or 0),
            "live_items": int(live_items or 0),
            "total_customers": int(total_customers or 0),
            "repeat_customers": int(repeat_customers or 0),
            "repeat_pct": (float(repeat_customers or 0) / max(1, float(total_customers or 1))) * 100,
            "aov": float(aov or 0)
        },
        "trend": trend,
        "top_selling": top_selling
    })


# -------------------------
# 2. Order status breakdown (pie)
# GET /admin/dashboard/order-status-breakdown
# -------------------------
@admin_required
async def order_status_breakdown(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT COALESCE(status,'unknown') AS status, COUNT(*) AS count
            FROM asbeza_orders
            GROUP BY COALESCE(status,'unknown')
        """)
    return web.json_response({"status": "ok", "data": [dict(r) for r in rows]})


# -------------------------
# 3. Payment method split
# GET /admin/dashboard/payment-method-split
# -------------------------
@admin_required
async def payment_method_split(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT COALESCE(method,'unknown') AS method, COUNT(*) AS count, COALESCE(SUM(amount),0) AS total_amount
            FROM asbeza_order_payments
            GROUP BY COALESCE(method,'unknown')
        """)
    return web.json_response({"status": "ok", "data": [dict(r) for r in rows]})


# -------------------------
# 4. Fulfillment speed
# GET /admin/dashboard/fulfillment-speed
# -------------------------
@admin_required
async def fulfillment_speed(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        avg_hours = await conn.fetchval("""
            SELECT AVG(EXTRACT(EPOCH FROM (delivered_at - created_at)))/3600.0
            FROM asbeza_orders
            WHERE delivered_at IS NOT NULL
        """)
        # median and p95 (if Postgres supports percentile_cont)
        median_hours = await conn.fetchval("""
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - created_at))/3600.0)
            FROM asbeza_orders WHERE delivered_at IS NOT NULL
        """)
        p95_hours = await conn.fetchval("""
            SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (delivered_at - created_at))/3600.0)
            FROM asbeza_orders WHERE delivered_at IS NOT NULL
        """)
    return web.json_response({
        "status": "ok",
        "avg_hours": float(avg_hours or 0),
        "median_hours": float(median_hours or 0),
        "p95_hours": float(p95_hours or 0)
    })


# -------------------------
# 5. Stock alerts
# GET /admin/dashboard/stock-alerts?threshold=5
# -------------------------
@admin_required
async def stock_alerts(request: web.Request) -> web.Response:
    threshold = int(request.query.get("threshold", 5))
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT v.id, v.item_id, i.name AS item_name, v.name AS variant_name, v.stock
            FROM asbeza_variants v
            JOIN asbeza_items i ON v.item_id = i.id
            WHERE v.stock <= $1
            ORDER BY v.stock ASC
            LIMIT 200
        """, threshold)
    return web.json_response({"status": "ok", "threshold": threshold, "alerts": [dict(r) for r in rows]})


# -------------------------
# 6. Order heatmap (hourly)
# GET /admin/dashboard/order-heatmap?days=7
# -------------------------
@admin_required
async def order_heatmap(request: web.Request) -> web.Response:
    days = int(request.query.get("days", 7))
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch(f"""
            SELECT EXTRACT(HOUR FROM created_at)::int AS hour, COUNT(*) AS orders
            FROM asbeza_orders
            WHERE created_at >= CURRENT_DATE - INTERVAL '{days - 1} days'
            GROUP BY hour
            ORDER BY hour
        """)
    # Build full 0-23 array
    counts = {r["hour"]: r["orders"] for r in rows}
    hourly = [{"hour": h, "orders": int(counts.get(h, 0))} for h in range(24)]
    return web.json_response({"status": "ok", "days": days, "hourly": hourly})


# -------------------------
# 7. Campus distribution
# GET /admin/dashboard/campus-distribution
# -------------------------
@admin_required
async def campus_distribution(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT COALESCE(u.campus, 'Unknown') AS campus, COUNT(o.id) AS orders
            FROM asbeza_orders o
            LEFT JOIN users u ON o.user_id = u.telegram_id
            GROUP BY COALESCE(u.campus, 'Unknown')
            ORDER BY orders DESC
        """)
    return web.json_response({
        "status": "ok",
        "data": [dict(r) for r in rows]
    })


# -------------------------
# 8. Orders list with filters & pagination
# GET /admin/orders?status=&limit=50&offset=0
# -------------------------
@admin_required
async def list_orders(request: web.Request) -> web.Response:
    status = request.query.get("status")
    limit = int(request.query.get("limit", 50))
    offset = int(request.query.get("offset", 0))

    async with request.app["db"]._open_connection() as conn:
        # Fetch orders
        rows = await conn.fetch("""
            SELECT o.*, p.payment_proof_url, p.method as payment_method
            FROM asbeza_orders o
            LEFT JOIN asbeza_order_payments p ON o.id = p.order_id
            WHERE ($1::text IS NULL OR o.status = $1)
            ORDER BY o.created_at DESC
            LIMIT $2 OFFSET $3
        """, status, limit, offset)

        orders = [dict(r) for r in rows]
        for o in orders:
            if o.get("created_at"):
                o["created_at"] = o["created_at"].isoformat()

        # ✅ total_count must be inside the same connection context
        total_count = await conn.fetchval(
            "SELECT COUNT(*) FROM asbeza_orders WHERE ($1::text IS NULL OR status = $1)",
            status
        )

    return web.json_response({
        "status": "ok",
        "orders": orders,
        "total": int(total_count or 0)
    })

# -------------------------
# 9. Order details expanded
# GET /admin/orders/{id}
# -------------------------
@admin_required
async def get_order_details(request: web.Request) -> web.Response:
    order_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        order = await conn.fetchrow("SELECT * FROM asbeza_orders WHERE id=$1", order_id)
        if not order:
            return web.json_response({"status":"error","message":"Order not found"}, status=404)

        items = await conn.fetch("""
            SELECT oi.*, v.name as variant_name, i.name as item_name, i.image_url
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            WHERE oi.order_id = $1
        """, order_id)

        payments = await conn.fetch("""
            SELECT * FROM asbeza_order_payments WHERE order_id=$1 ORDER BY created_at DESC
        """, order_id)

        user = None
        if order['user_id']:
            # orders.user_id is a Telegram ID → match against users.telegram_id
            user = await conn.fetchrow("""
                SELECT telegram_id, first_name, phone, campus
                FROM users WHERE telegram_id = $1
            """, order['user_id'])

        # Helper to convert datetimes
        def to_dict(record):
            d = dict(record)
            for k, v in d.items():
                if isinstance(v, (datetime.date, datetime.datetime)):
                    d[k] = v.isoformat()
            return d

        order_out = to_dict(order)
        items_out = [to_dict(r) for r in items]
        payments_out = [to_dict(r) for r in payments]

        # Add summary stats
        total_qty = sum(i['quantity'] for i in items_out)
        order_out['total_items'] = len(items_out)
        order_out['total_quantity'] = total_qty
        order_out['delivery_fee'] = order.get('delivery_fee', 0)

        user_out = to_dict(user) if user else None

        return web.json_response({
            "status":"ok",
            "order": order_out,
            "items": items_out,
            "payments": payments_out,
            "user": user_out
        })

# -------------------------
# 10. Update order status (set delivered_at when delivered)
# POST /admin/orders/{id}/status
# -------------------------
@admin_required
async def update_order_status(request: web.Request) -> web.Response:
    try:
        order_id = int(request.match_info['id'])
        data = await request.json()
        new_status = data.get("status")
        if not new_status:
            return web.json_response({"status": "error", "message": "Missing status"}, status=400)

        async with request.app["db"]._open_connection() as conn:
            if new_status == 'delivered':
                result = await conn.execute("""
                    UPDATE asbeza_orders SET status = $1, delivered_at = NOW() WHERE id = $2
                """, new_status, order_id)
            else:
                result = await conn.execute("""
                    UPDATE asbeza_orders SET status = $1 WHERE id = $2
                """, new_status, order_id)

            if result == "UPDATE 0":
                return web.json_response({"status": "error", "message": "Order not found"}, status=404)

        return web.json_response({"status": "ok", "message": f"Order {order_id} updated to {new_status}"})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)


# -------------------------
# 11. User details
# GET /admin/users/{id}
# -------------------------
@admin_required
async def get_user_details(request: web.Request) -> web.Response:
    user_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        user = await conn.fetchrow("""
            SELECT id, telegram_id, role, first_name, phone, campus, coins, xp, level, status
            FROM users WHERE id = $1
        """, user_id)
        if not user:
            return web.json_response({"status": "error", "message": "User not found"}, status=404)

        summary = await conn.fetchrow("""
            SELECT COUNT(*) AS total_orders, COALESCE(SUM(total_price),0) AS lifetime_value
            FROM asbeza_orders WHERE user_id = $1
        """, user_id)

        recent_orders = await conn.fetch("""
            SELECT id, total_price, status, created_at
            FROM asbeza_orders WHERE user_id = $1
            ORDER BY created_at DESC LIMIT 10
        """, user_id)

        favorites = await conn.fetch("""
            SELECT i.id, i.name, COALESCE(SUM(oi.quantity),0) AS qty
            FROM asbeza_order_items oi
            JOIN asbeza_orders o ON oi.order_id = o.id
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            WHERE o.user_id = $1
            GROUP BY i.id, i.name
            ORDER BY qty DESC LIMIT 5
        """, user_id)

    return web.json_response({
        "status": "ok",
        "user": dict(user),
        "summary": {"total_orders": int(summary["total_orders"]), "lifetime_value": float(summary["lifetime_value"])},
        "recent_orders": [{**dict(r), "created_at": r["created_at"].isoformat()} for r in recent_orders],
        "favorites": [dict(r) for r in favorites]
    })


# -------------------------
# 12. Latest orders feed
# GET /admin/latest-orders?limit=5
# -------------------------
@admin_required
async def latest_orders(request: web.Request) -> web.Response:
    limit = int(request.query.get("limit", 5))
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT o.id, o.user_id, o.total_price, o.status, o.created_at, p.method as payment_method
            FROM asbeza_orders o
            LEFT JOIN asbeza_order_payments p ON o.id = p.order_id
            ORDER BY o.created_at DESC
            LIMIT $1
        """, limit)
        data = []
        for r in rows:
            d = dict(r)
            d["created_at"] = d["created_at"].isoformat() if d.get("created_at") else None
            data.append(d)
    return web.json_response({"status": "ok", "orders": data})
