import asyncio
import datetime
from aiohttp import web
from typing import List, Dict
import math
import os
import uuid
import aiofiles
from numpy import record

import app
from config import settings

ALLOWED_IMAGE_MIMES = {"image/jpeg", "image/png", "image/webp", "image/jpg"}

def _int(v):
    try: return int(v)
    except: return None


# GET /api/auth/role?user_id=123
async def get_user_role(request: web.Request) -> web.Response:
    user_id = request.query.get("user_id")
    if not user_id:
        return web.json_response({"status": "error", "message": "Missing user_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        # Check delivery_guys table first
        dg = await conn.fetchrow("SELECT id FROM delivery_guys WHERE telegram_id=$1", int(user_id))
        if dg:
            return web.json_response({"status": "ok", "role": "delivery"})

        # Otherwise check users table
        u = await conn.fetchrow("SELECT id FROM users WHERE id=$1", int(user_id))
        if u:
            return web.json_response({"status": "ok", "role": "user"})

    return web.json_response({"status": "error", "message": "User not found"}, status=404)


# In your Python backend:
async def get_asbeza_items(request: web.Request) -> web.Response:
    db = request.app["db"]
    async with db._open_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT i.id, i.name, i.description, i.base_price, i.image_url, i.category, -- ADDED i.category
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
    Fast checkout: trust frontend totals to minimize latency while preserving
    existing admin/user notification logic.

    POST /api/asbeza/checkout
    Expects JSON:
    {
      "user_id": 12345,
      "items": [
        {"variant_id": 1, "quantity": 2, "price": 120.0},
        ...
      ],
      // optional client-side totals (trusted if provided)
      "delivery_fee": 80,
      "total_price": 480,
      "upfront_paid": 192,
      // optional: payment_proof_url (public URL) or payment_proof_base64
    }

    Returns:
    { "status": "ok", "order_id": 42, "upfront": 192 }
    """
    db = request.app["db"]

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "invalid json"}, status=400)

    # Required minimal validation: user_id and items
    raw_user_id = payload.get("user_id")
    if raw_user_id is None:
        return web.json_response({
            "status": "error",
            "message": "User identification missing. Please restart the app."
        }, status=400)

    items: List[Dict] = payload.get("items", [])
    try:
        user_id = int(raw_user_id)
    except (ValueError, TypeError):
        return web.json_response({"status": "error", "message": "Invalid User ID format"}, status=400)

    if not isinstance(items, list) or len(items) == 0:
        return web.json_response({"status": "error", "message": "items are required"}, status=400)

    # Validate item structure (lightweight): ensure variant_id and quantity present and sane
    validated_items_input: List[Dict] = []
    for idx, it in enumerate(items):
        try:
            variant_id = int(it.get("variant_id"))
            quantity = int(it.get("quantity", 1))
            if quantity <= 0:
                raise ValueError()
            # Accept client-provided price (trusted). Default to 0.0 if missing.
            price = float(it.get("price", 0.0))
        except Exception:
            return web.json_response({"status": "error", "message": f"invalid item at index {idx}"}, status=400)
        validated_items_input.append({"variant_id": variant_id, "quantity": quantity, "price": price})

    # Trust frontend totals if provided (fast path)
    delivery_fee = payload.get("delivery_fee")
    total_price = payload.get("total_price")
    upfront_paid = payload.get("upfront_paid")

    # Normalize numeric totals to floats/ints where possible (but do not recompute)
    try:
        if delivery_fee is not None:
            delivery_fee = float(delivery_fee)
        else:
            delivery_fee = 0.0
    except Exception:
        return web.json_response({"status": "error", "message": "invalid delivery_fee"}, status=400)

    try:
        if total_price is not None:
            total_price = float(total_price)
        else:
            total_price = 0.0
    except Exception:
        return web.json_response({"status": "error", "message": "invalid total_price"}, status=400)

    try:
        if upfront_paid is not None:
            upfront_paid = int(upfront_paid)
        else:
            upfront_paid = int(math.floor(total_price * 0.4)) if total_price else 0
    except Exception:
        return web.json_response({"status": "error", "message": "invalid upfront_paid"}, status=400)

    payment_proof_url = payload.get("payment_proof_url")
    payment_proof_base64 = payload.get("payment_proof_base64")

    # Fast DB insertion trusting client-provided totals and item prices
    try:
        async with db._open_connection() as conn:
            order_id = await conn.fetchval(
                """
                INSERT INTO asbeza_orders (user_id, total_price, delivery_fee, upfront_paid, status)
                VALUES ($1, $2, $3, $4, 'pending')
                RETURNING id
                """,
                user_id, total_price, delivery_fee, upfront_paid
            )

            # Insert items using client-provided price
            for it in validated_items_input:
                await conn.execute(
                    """
                    INSERT INTO asbeza_order_items (order_id, variant_id, quantity, price)
                    VALUES ($1, $2, $3, $4)
                    """,
                    order_id, it["variant_id"], it["quantity"], it["price"]
                )

            # Store payment proof if provided
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

            # --- Admin notification (preserve original logic) ---
            try:
                items_lines = []
                for it in validated_items_input:
                    vid = it["variant_id"]
                    qty = it["quantity"]
                    unit_price = it["price"]
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
                    try:
                        bot = request.app.get("bot")
                        if bot:
                            await bot.send_message(settings.ADMIN_DAILY_GROUP_ID, admin_msg, parse_mode="HTML")
                        else:
                            request.app.logger.warning("Bot instance not available on app; skipping admin notification.")
                    except Exception:
                        request.app.logger.exception(f"Failed to send admin notification for order {order_id}")
            except Exception:
                request.app.logger.exception("Failed to build/send admin notification")

            # --- User notification (preserve original logic) ---
            try:
                bot = request.app.get("bot")
                if bot and user_id:
                    if payment_proof_url:
                        public_base = request.app.get("public_base_url", "").rstrip("/")
                        if payment_proof_url.startswith("/"):
                            photo_url = f"{public_base}{payment_proof_url}" if public_base else payment_proof_url
                        else:
                            photo_url = payment_proof_url
                        caption = "✅ Your Asbeza order has been received and is pending confirmation. You can track it in the Asbeza admin web app."
                        try:
                            await bot.send_photo(user_id, photo=photo_url, caption=caption, parse_mode="HTML")
                        except Exception:
                            try:
                                await bot.send_message(user_id, caption, parse_mode="HTML")
                            except Exception:
                                request.app.logger.exception(f"Failed to notify user {user_id} with photo/message for order {order_id}")
                    else:
                        try:
                            await bot.send_message(
                                user_id,
                                f"✅ Your Asbeza order #{order_id} has been received and is pending confirmation. Track it in the Asbeza admin web app.",
                                parse_mode="HTML"
                            )
                        except Exception:
                            request.app.logger.exception(f"Failed to send user notification for order {order_id}")
                else:
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
    app.router.add_get("/api/asbeza/orders", get_user_orders)

    # Inventory
    app.router.add_get("/api/admin/items", list_items_admin)              # list items with variant counts
    app.router.add_get("/api/admin/items/{id}", get_item_admin)           # get item + variants
    app.router.add_post("/api/admin/add_items", add_item)                     # add new item
    app.router.add_put("/api/admin/items/{id}", update_item_admin)        # update item
    app.router.add_delete("/api/admin/items/{id}", delete_item_admin)     # delete item + variants
    app.router.add_put("/api/admin/variants/{id}", update_variant_admin)  # update variant
    app.router.add_delete("/api/admin/variants/{id}", delete_variant_admin) # delete variant
    app.router.add_post("/api/asbeza/variants", create_variant_admin) # create variant

    # Users
    app.router.add_get("/api/admin/users", list_users)
    app.router.add_get("/api/admin/users/{id}", get_user_details)


    # --- Analytics Dashboard ---
    app.router.add_get("/api/admin/dashboard/stats", dashboard_stats)
    app.router.add_get("/api/admin/dashboard/order-status-breakdown", order_status_breakdown)
    app.router.add_get("/api/admin/dashboard/payment-method-split", payment_method_split)
    app.router.add_get("/api/admin/dashboard/fulfillment-speed", fulfillment_speed)
    app.router.add_get("/api/admin/dashboard/order-heatmap", order_heatmap)
    app.router.add_get("/api/admin/dashboard/campus-distribution", campus_distribution)
    
    #DeliverGuy
    app.router.add_get("/api/delivery/my_orders", get_my_orders)
    app.router.add_get("/api/delivery/my_stats", get_my_stats)
    app.router.add_post("/api/delivery/update_status_delivery", update_order_status_delivery)
    app.router.add_get("/api/delivery/settings", get_settings)
    app.router.add_post("/api/admin/orders/{order_id}/assign", assign_courier)
    app.router.add_get("/api/admin/delivery-guys", list_delivery_guys) # now only active & not blocked
    app.router.add_get("/api/auth/role", get_user_role)
    app.router.add_get('/api/delivery/order_details_for_dg/{order_id}/{delivery_guy_id}', get_rider_order_details)
    app.router.add_get("/api/delivery/food_stats", get_food_stats)
    app.router.add_get("/api/delivery/asbeza_stats", get_asbeza_stats)
    app.router.add_get("/api/delivery/delivery_guy_id", get_delivery_guy_id)




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
    desc = data.get("description", "")
    price = data.get("base_price", 0)
    img = data.get("image_url", "")
    category = data.get("category", "General") # New field
    variants = data.get("variants", []) 

    async with request.app["db"]._open_connection() as conn:
        async with conn.transaction():
            # 1. Insert Item with Category
            item_id = await conn.fetchval("""
                INSERT INTO asbeza_items (name, description, base_price, image_url, category)
                VALUES ($1, $2, $3, $4, $5) RETURNING id
            """, name, desc, price, img, category)

            # 2. Insert Variants with Auto-Cost
            for v in variants:
                v_price = v.get('price', price)
                # If admin didn't provide cost_price, we use the selling price
                v_cost = v.get('cost_price', v_price) 
                
                await conn.execute("""
                    INSERT INTO asbeza_variants (item_id, name, price, cost_price, stock, image_url)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, item_id, v['name'], v_price, v_cost, v.get('stock', 0), v.get('image_url', img))

    return web.json_response({"status": "ok", "message": "Product deployed successfully!"})
    
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
            SELECT 
                DATE(o.created_at) AS date, 
                COALESCE(SUM(oi.price * oi.quantity), 0) AS daily_revenue,
                COALESCE(SUM((oi.price - v.cost_price) * oi.quantity), 0) AS daily_profit
            FROM asbeza_orders o
            JOIN asbeza_order_items oi ON o.id = oi.order_id
            JOIN asbeza_variants v ON oi.variant_id = v.id
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '6 days'
              AND o.status != 'cancelled'
              AND o.status = 'completed'
            GROUP BY DATE(o.created_at)
            ORDER BY DATE(o.created_at)
        """)
        
        trend = [{
            "date": r["date"].isoformat(), 
            "total": float(r["daily_revenue"]),
            "profit": float(r["daily_profit"])
        } for r in rows]
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
        total_profit = await conn.fetchval("""
            SELECT SUM((oi.price - v.cost_price) * oi.quantity)
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_orders o ON oi.order_id = o.id
            WHERE o.status != 'cancelled'
        """)

    return web.json_response({
        "status": "ok",
        "kpis": {
            "net_revenue": float(net_revenue or 0),
            "pending_orders": int(pending_count or 0),
            "total_profit": float(total_profit or 0),
            "live_items": int(live_items or 0),
            "margin_pct": (float(total_profit or 0) / max(1, float(net_revenue or 1))) * 100,
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
# @admin_required
# async def stock_alerts(request: web.Request) -> web.Response:
#     threshold = int(request.query.get("threshold", 5))
#     async with request.app["db"]._open_connection() as conn:
#         rows = await conn.fetch("""
#             SELECT v.id, v.item_id, i.name AS item_name, v.name AS variant_name, v.stock
#             FROM asbeza_variants v
#             JOIN asbeza_items i ON v.item_id = i.id
#             WHERE v.stock <= $1
#             ORDER BY v.stock ASC
#             LIMIT 200
#         """, threshold)
#     return web.json_response({"status": "ok", "threshold": threshold, "alerts": [dict(r) for r in rows]})


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

def serialize_record(record):
    from datetime import datetime
    d = dict(record)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


@admin_required
async def list_orders(request: web.Request) -> web.Response:
    status = request.query.get("status")
    limit = int(request.query.get("limit", 50))
    offset = int(request.query.get("offset", 0))

    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT o.*, 
                   p.payment_proof_url, 
                   p.method as payment_method,
                   u.first_name, 
                   u.campus
            FROM asbeza_orders o
            LEFT JOIN asbeza_order_payments p ON o.id = p.order_id
            LEFT JOIN users u ON o.user_id = u.telegram_id
            WHERE ($1::text IS NULL OR o.status = $1)
            ORDER BY o.created_at DESC
            LIMIT $2 OFFSET $3
        """, status, limit, offset)

        orders = [serialize_record(r) for r in rows]

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
import logging
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

@admin_required
async def update_order_status(request: web.Request) -> web.Response:
    """
    Update an order's status and notify the user via bot with an inline
    Track button that opens the web app with user_id and order_id.
    """
    try:
        order_id = int(request.match_info['id'])
        data = await request.json()
        new_status = data.get("status")
        if not new_status:
            logging.warning(f"Order {order_id}: Missing status in request payload")
            return web.json_response({"status": "error", "message": "Missing status"}, status=400)

        logging.info(f"Order {order_id}: Updating status to '{new_status}'")

        async with request.app["db"]._open_connection() as conn:
            # Update order status
            if new_status == "delivered":
                result = await conn.execute(
                    "UPDATE asbeza_orders SET status = $1, delivered_at = NOW() WHERE id = $2",
                    new_status, order_id
                )
            else:
                result = await conn.execute(
                    "UPDATE asbeza_orders SET status = $1 WHERE id = $2",
                    new_status, order_id
                )

            logging.debug(f"Order {order_id}: DB update result = {result}")

            if result == "UPDATE 0":
                logging.error(f"Order {order_id}: Not found in DB")
                return web.json_response({"status": "error", "message": "Order not found"}, status=404)

            # Fetch Telegram ID
            order_row = await conn.fetchrow("SELECT user_id FROM asbeza_orders WHERE id = $1", order_id)
            if not order_row:
                logging.error(f"Order {order_id}: Row missing after update")
                return web.json_response({"status": "error", "message": "Order not found after update"}, status=404)

            telegram_id = int(order_row["user_id"])
            logging.info(f"Order {order_id}: Found telegram_id = {telegram_id}")

        # Status messages
        status_messages = {
            "pending":    f"🛒 Your Asbeza order #{order_id} has been placed and is waiting to be processed.",
            "processing": f"📦 Your Asbeza order #{order_id} is accepted and being prepared.",
            "shipped":    f"🚚 Your Asbeza order #{order_id} is on the way to you.",
            "completed":  f"✅ Your Asbeza order #{order_id} has been successfully completed. Thank you for shopping with us!",
            "cancelled":  f"❌ Your Asbeza order #{order_id} has been cancelled.",
            "delivered":  f"🏠 Your Asbeza order #{order_id} has been delivered. Enjoy your items!"
        }
        message_text = status_messages.get(new_status, f"ℹ️ Your Asbeza order #{order_id} status is now: {new_status}")

        # Inline keyboard
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

        def build_tracking_keyboard(user_id: int, order_id: int) -> InlineKeyboardMarkup:
            url = f"https://unibites-asbeza.vercel.app?user_id={user_id}&order_id={order_id}"
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="🧺 Track My Order 🧺",
                        web_app=WebAppInfo(url=url)
                    )]
                ]
            )
            return keyboard


        keyboard = build_tracking_keyboard(telegram_id, order_id)

        # Send bot message
        bot = request.app.get("bot")
        if bot:
            try:
                logging.info(f"Order {order_id}: Sending Telegram message to {telegram_id}")
                await bot.send_message(chat_id=telegram_id, text=message_text, reply_markup=keyboard)
                logging.info(f"Order {order_id}: Telegram message sent successfully")
            except Exception as e:
                logging.exception(f"Order {order_id}: Failed to send Telegram message to {telegram_id}")

        return web.json_response({"status": "ok", "message": f"Order {order_id} updated to {new_status}"})
    except Exception as e:
        logging.exception(f"Order {order_id if 'order_id' in locals() else 'unknown'}: Unexpected error")
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# -------------------------
# 11. User details
# GET /admin/users/{id}
# -------------------------
# async def get_user_details(request: web.Request) -> web.Response:
#     user_id = int(request.match_info['id'])
#     async with request.app["db"]._open_connection() as conn:
#         user = await conn.fetchrow("""
#             SELECT id, telegram_id, role, first_name, phone, campus, coins, xp, level, status
#             FROM users WHERE id = $1
#         """, user_id)
#         if not user:
#             return web.json_response({"status": "error", "message": "User not found"}, status=404)

#         summary = await conn.fetchrow("""
#             SELECT COUNT(*) AS total_orders, COALESCE(SUM(total_price),0) AS lifetime_value
#             FROM asbeza_orders WHERE user_id = $1
#         """, user_id)

#         recent_orders = await conn.fetch("""
#             SELECT id, total_price, status, created_at
#             FROM asbeza_orders WHERE user_id = $1
#             ORDER BY created_at DESC LIMIT 10
#         """, user_id)

#         favorites = await conn.fetch("""
#             SELECT i.id, i.name, COALESCE(SUM(oi.quantity),0) AS qty
#             FROM asbeza_order_items oi
#             JOIN asbeza_orders o ON oi.order_id = o.id
#             JOIN asbeza_variants v ON oi.variant_id = v.id
#             JOIN asbeza_items i ON v.item_id = i.id
#             WHERE o.user_id = $1
#             GROUP BY i.id, i.name
#             ORDER BY qty DESC LIMIT 5
#         """, user_id)

#     return web.json_response({
#         "status": "ok",
#         "user": dict(user),
#         "summary": {"total_orders": int(summary["total_orders"]), "lifetime_value": float(summary["lifetime_value"])},
#         "recent_orders": [{**dict(r), "created_at": r["created_at"].isoformat()} for r in recent_orders],
#         "favorites": [dict(r) for r in favorites]
#     })


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



@admin_required
async def list_items_admin(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT i.id, i.name, i.base_price, i.image_url,
                   COUNT(v.id) AS variant_count
            FROM asbeza_items i
            LEFT JOIN asbeza_variants v ON v.item_id = i.id
            GROUP BY i.id, i.name, i.base_price, i.image_url
            ORDER BY i.created_at DESC
        """)
    return web.json_response({"status": "ok", "items": [dict(r) for r in rows]})


def to_dict(record):
    d = dict(record)
    for k, v in d.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            d[k] = v.isoformat()
    return d


@admin_required
async def get_item_admin(request: web.Request) -> web.Response:
    item_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        item = await conn.fetchrow("SELECT * FROM asbeza_items WHERE id=$1", item_id)
        if not item:
            return web.json_response({"status":"error","message":"Item not found"}, status=404)

        variants = await conn.fetch("SELECT * FROM asbeza_variants WHERE item_id=$1", item_id)

    return web.json_response({
        "status": "ok",
        "item": to_dict(item),
        "variants": [to_dict(v) for v in variants]
    })


@admin_required
async def update_item_admin(request: web.Request) -> web.Response:
    item_id = int(request.match_info['id'])
    data = await request.json()
    
    async with request.app["db"]._open_connection() as conn:
        # Added category to the UPDATE statement
        await conn.execute("""
            UPDATE asbeza_items
            SET name=$1, base_price=$2, image_url=$3, category=$4
            WHERE id=$5
        """, 
        data.get("name"), 
        data.get("base_price"), 
        data.get("image_url"), 
        data.get("category"), # New field
        item_id)
        
    return web.json_response({"status":"ok","message":f"Item {item_id} updated"})

@admin_required
async def delete_item_admin(request: web.Request) -> web.Response:
    item_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        await conn.execute("DELETE FROM asbeza_items WHERE id=$1", item_id)
        await conn.execute("DELETE FROM asbeza_variants WHERE item_id=$1", item_id)
    return web.json_response({"status":"ok","message":f"Item {item_id} deleted"})


@admin_required
async def update_variant_admin(request: web.Request) -> web.Response:
    variant_id = int(request.match_info['id'])
    data = await request.json()

    async with request.app["db"]._open_connection() as conn:
        # Added cost_price to the UPDATE statement
        await conn.execute("""
            UPDATE asbeza_variants
            SET name = $1,
                price = $2,
                cost_price = $3,
                stock = COALESCE($4, stock),
                image_url = COALESCE($5, image_url)
            WHERE id = $6
        """,
        data.get("name"),
        data.get("price"),
        data.get("cost_price"),  # New field
        data.get("stock"),
        data.get("image_url"),
        variant_id)

    return web.json_response({
        "status": "ok",
        "message": f"Variant {variant_id} updated (Cost Price: {data.get('cost_price')})"
    })

@admin_required
async def delete_variant_admin(request: web.Request) -> web.Response:
    variant_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        await conn.execute("DELETE FROM asbeza_variants WHERE id=$1", variant_id)
    return web.json_response({"status":"ok","message":f"Variant {variant_id} deleted"})

@admin_required
async def create_variant_admin(request: web.Request) -> web.Response:
    data = await request.json()
    item_id = data.get("item_id")
    name = data.get("name", "New Variant")
    price = data.get("price", 0)
    cost_price = data.get("cost_price", 0) # Default cost
    stock = data.get("stock", 0)
    image_url = data.get("image_url")

    if not item_id:
        return web.json_response({"status": "error", "message": "Missing item_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        # Added cost_price to the INSERT statement
        await conn.execute("""
            INSERT INTO asbeza_variants (item_id, name, price, cost_price, stock, image_url)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, item_id, name, price, cost_price, stock, image_url)

    return web.json_response({
        "status": "ok",
        "message": f"Variant '{name}' created with cost {cost_price}"
    })


@admin_required
async def list_users(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT telegram_id AS id, first_name, phone, campus, level, coins
            FROM users
            ORDER BY created_at DESC
        """)
    # Convert datetimes if needed
    users = [dict(r) for r in rows]
    return web.json_response({"status": "ok", "users": users})



async def get_user_details(request: web.Request) -> web.Response:
    user_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        user = await conn.fetchrow("""
            SELECT telegram_id AS id, first_name, phone, campus, level, coins
            FROM users WHERE telegram_id = $1
        """, user_id)
        if not user:
            return web.json_response({"status":"error","message":"User not found"}, status=404)

        # Orders summary
        orders = await conn.fetch("""
            SELECT id, total_price, created_at
            FROM asbeza_orders
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT 10
        """, user_id)

        total_orders = len(orders)
        lifetime_value = sum(o['total_price'] for o in orders)

        # Favorites (aggregate items ordered most)
        favorites = await conn.fetch("""
            SELECT i.name, SUM(oi.quantity) AS qty
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            JOIN asbeza_orders o ON oi.order_id = o.id
            WHERE o.user_id = $1
            GROUP BY i.name
            ORDER BY qty DESC
            LIMIT 5
        """, user_id)

    def to_dict(record):
        d = dict(record)
        for k, v in d.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                d[k] = v.isoformat()
        return d

    return web.json_response({
        "status": "ok",
        "user": dict(user),
        "summary": {
            "total_orders": total_orders,
            "lifetime_value": lifetime_value
        },
        "favorites": [dict(f) for f in favorites],
        "recent_orders": [to_dict(o) for o in orders]
    })
async def get_user_orders(request: web.Request) -> web.Response:
    user_id_str = request.query.get("user_id")
    if not user_id_str:
        return web.json_response({"status": "error", "message": "user_id required"}, status=400)
    
    user_id = int(user_id_str)
    db = request.app["db"]
    
    async with db._open_connection() as conn:
        # Optimized: Removed GROUP BY, added Subqueries for counts/images
        rows = await conn.fetch("""
            SELECT 
                o.id, 
                o.status, 
                o.total_price, 
                o.upfront_paid, 
                o.delivery_fee,
                o.created_at,
                (SELECT COUNT(*) FROM asbeza_order_items WHERE order_id = o.id) as item_count,
                (SELECT payment_proof_url FROM asbeza_order_payments WHERE order_id = o.id LIMIT 1) as image_url,
                d.name AS delivery_name,
                d.campus AS delivery_campus,
                d.phone AS delivery_phone
            FROM asbeza_orders o
            LEFT JOIN delivery_guys d ON o.delivery_guy_id = d.id
            WHERE o.user_id = $1 
            ORDER BY o.created_at DESC 
            LIMIT 15
        """, user_id)
        
        orders = []
        for r in rows:
            # High-end structured response
            order_data = dict(r)
            order_data["created_at"] = r["created_at"].isoformat()
            order_data["image_url"] = r["image_url"] or ""
            
            # Sub-object for delivery guy
            order_data["delivery"] = {
                "name": r["delivery_name"],
                "campus": r["delivery_campus"],
                "phone": r["delivery_phone"]
            } if r["delivery_name"] else None
            
            # Remove the flat delivery fields to keep the payload small
            del order_data["delivery_name"]
            del order_data["delivery_campus"]
            del order_data["delivery_phone"]
            
            orders.append(order_data)
            
    return web.json_response({"status": "ok", "orders": orders})

async def get_rider_order_details(request: web.Request) -> web.Response:
    try:
        order_id = int(request.match_info.get("order_id"))
        dg_id = int(request.match_info.get("delivery_guy_id"))
    except (ValueError, TypeError):
        return web.json_response({"status": "error", "message": "Invalid IDs"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        order_row = await conn.fetchrow("""
            SELECT 
                o.id, o.status, o.total_price, o.upfront_paid, o.delivery_fee, o.created_at,
                u.first_name AS customer_name, u.phone AS customer_phone,
                u.campus AS delivery_location
            FROM asbeza_orders o
            JOIN users u ON o.user_id = u.telegram_id
            WHERE o.id = $1 AND o.delivery_guy_id = $2
            LIMIT 1
        """, order_id, dg_id)

        item_rows = await conn.fetch("""
            SELECT 
                i.name AS item_name, 
                oi.quantity, 
                oi.price, 
                v.name AS variant_name, 
                v.image_url AS variant_image
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            WHERE oi.order_id = $1
        """, order_id)

    if not order_row:
        return web.json_response({"status": "error", "message": "Order not found"}, status=404)

    order_data = dict(order_row)
    order_data["created_at"] = order_row["created_at"].isoformat()
    order_data["items"] = [dict(i) for i in item_rows]

    return web.json_response({"status": "ok", "order": order_data})

# GET /api/delivery/food_stats?delivery_guy_id=123
async def get_food_stats(request: web.Request) -> web.Response:
    dg_id = request.query.get("delivery_guy_id")
    if not dg_id:
        return web.json_response({"status": "error", "message": "Missing delivery_guy_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) AS total_orders,
                SUM(delivery_fee) AS total_delivery_fees,
                SUM(delivery_fee) AS total_earnings
            FROM orders
            WHERE delivery_guy_id = $1 AND status = 'delivered'
        """, int(dg_id))

    return web.json_response({"status": "ok", "stats": dict(stats) if stats else {}})


# GET /api/delivery/asbeza_stats?delivery_guy_id=123
async def get_asbeza_stats(request: web.Request) -> web.Response:
    dg_id = request.query.get("delivery_guy_id")
    if not dg_id:
        return web.json_response({"status": "error", "message": "Missing delivery_guy_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) AS total_orders,
                SUM(delivery_fee) AS total_delivery_fees,
                SUM(total_price) AS total_order_value,
                SUM(delivery_fee + total_price) AS total_earnings
            FROM asbeza_orders
            WHERE delivery_guy_id = $1 AND status = 'delivered'
        """, int(dg_id))

    return web.json_response({"status": "ok", "stats": dict(stats) if stats else {}})

#Delivery Guys



# --- Delivery Guy Endpoints ---
async def get_my_orders(request: web.Request) -> web.Response:
    dg_id = request.query.get("delivery_guy_id")
    if not dg_id:
        return web.json_response({"status": "error", "message": "Missing ID"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        # Fetch orders assigned to this guy, newest first
        rows = await conn.fetch("""
            SELECT id, total_price, upfront_paid, status, created_at, delivered_at, delivery_fee
            FROM asbeza_orders 
            WHERE delivery_guy_id = $1
            ORDER BY created_at DESC
        """, int(dg_id))
        
        orders = [to_dict(r) for r in rows]
    return web.json_response({"status": "ok", "orders": orders})

async def get_my_stats(request: web.Request) -> web.Response:
    dg_id = request.query.get("delivery_guy_id")
    async with request.app["db"]._open_connection() as conn:
        stats = await conn.fetchrow("""
            SELECT total_deliveries, accepted_requests, skipped_requests, coins, xp, level 
            FROM delivery_guys WHERE id = $1
        """, int(dg_id))
        
    return web.json_response({"status": "ok", "stats": dict(stats) if stats else {}})

# CRITICAL: Endpoint to mark order as delivered
async def update_order_status_delivery(request: web.Request) -> web.Response:
    data = await request.json()
    order_id = data.get("order_id")
    new_status = data.get("status") # 'delivered'
    dg_id = data.get("delivery_guy_id")

    async with request.app["db"]._open_connection() as conn:
        async with conn.transaction():
            # 1. Update Order
            await conn.execute("""
                UPDATE asbeza_orders 
                SET status = $1, delivered_at = CURRENT_TIMESTAMP 
                WHERE id = $2 AND delivery_guy_id = $3
            """, new_status, order_id, int(dg_id))

            # 2. Reward the Delivery Guy (Simple Gamification)
            if new_status == 'delivered':
                await conn.execute("""
                    UPDATE delivery_guys 
                    SET total_deliveries = total_deliveries + 1,
                        coins = coins + 10,
                        xp = xp + 50
                    WHERE id = $1
                """, int(dg_id))

    return web.json_response({"status": "ok", "message": "Order completed!"})


# GET /api/delivery/settings?delivery_guy_id=123
async def get_settings(request: web.Request) -> web.Response:
    dg_id = request.query.get("delivery_guy_id")
    if not dg_id:
        return web.json_response({"status": "error", "message": "Missing ID"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        dg = await conn.fetchrow("""
            SELECT id, name, campus, phone, active, blocked, gender
            FROM delivery_guys WHERE id = $1
        """, int(dg_id))

    if not dg:
        return web.json_response({"status": "error", "message": "Delivery guy not found"}, status=404)

    return web.json_response({"status": "ok", "settings": dict(dg)})

# GET /admin/delivery-guys
async def list_delivery_guys(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT id, name, campus, phone, active, blocked, gender
            FROM delivery_guys
            WHERE active = TRUE AND blocked = FALSE
            ORDER BY name ASC
        """)
        guys = [dict(r) for r in rows]

    return web.json_response({"status": "ok", "guys": guys})

async def assign_courier(request: web.Request) -> web.Response:
    order_id = _int(request.match_info.get("order_id"))
    data = await request.json()
    dg_id = _int(data.get("delivery_guy_id"))

    if not order_id or not dg_id:
        return web.json_response({"status": "error", "message": "Missing order_id or delivery_guy_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        async with conn.transaction():
            dg = await conn.fetchrow("SELECT id, blocked FROM delivery_guys WHERE id=$1", dg_id)
            if not dg:
                return web.json_response({"status": "error", "message": "Delivery guy not found"}, status=404)
            if dg["blocked"]:
                return web.json_response({"status": "error", "message": "Delivery guy is blocked"}, status=403)

            await conn.execute("""
                UPDATE asbeza_orders
                SET delivery_guy_id=$1
                WHERE id=$2
            """, dg_id, order_id)

            await conn.execute("""
                UPDATE delivery_guys
                SET total_requests = total_requests + 1,
                    accepted_requests = accepted_requests + 1,
                    last_online_at = NOW()
                WHERE id=$1
            """, dg_id)

    return web.json_response({"status": "ok", "message": "Courier assigned successfully!"})


async def get_delivery_guy_id(request: web.Request) -> web.Response:
    telegram_id = request.query.get("telegram_id")
    if not telegram_id:
        return web.json_response({"status": "error", "message": "Missing telegram_id"}, status=400)

    async with request.app["db"]._open_connection() as conn:
        dg = await conn.fetchrow("""
            SELECT id FROM delivery_guys WHERE telegram_id = $1
        """, int(telegram_id))
    
    if not dg:
        return web.json_response({"status": "error", "message": "Delivery guy not found"}, status=404)

    return web.json_response({"status": "ok", "delivery_guy_id": dg["id"]})
