import datetime
from aiohttp import web
from typing import List, Dict
import math
import os
import uuid
import aiofiles

from config import settings

ALLOWED_IMAGE_MIMES = {"image/jpeg", "image/png", "image/webp", "image/jpg"}


async def get_asbeza_items(request: web.Request) -> web.Response:
    """
    GET /api/asbeza/items
    Returns active items (id, name, description, base_price, image_url)
    """
    db = request.app["db"]
    async with db._open_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, description, base_price, image_url
            FROM asbeza_items
            WHERE active = TRUE
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

    user_id = payload.get("user_id")
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
            
            payment_proof_url = payload.get("payment_proof_url")
            if payment_proof_url:
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
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)
    app.router.add_post("/api/asbeza/upload_screenshot", upload_screenshot)  # upload_screenshot defined below
    app.router.add_post("/api/admin/login", admin_login)
    app.router.add_get("/api/admin/orders", list_orders),
    app.router.add_get("/api/admin/stats", get_dashboard_stats)




async def upload_screenshot(request: web.Request) -> web.Response:
    """
    POST /api/asbeza/upload_screenshot
    Accepts multipart/form-data with field 'file'
    Returns: { "status":"ok", "url": "<public-url>" }
    """
    upload_dir = request.app.get("upload_dir", "./uploads")
    max_bytes = request.app.get("upload_max_bytes", 6 * 1024 * 1024)

    # ensure upload directory exists
    try:
        os.makedirs(upload_dir, exist_ok=True)
    except Exception:
        request.app.logger.exception("Failed to ensure upload directory exists")

    reader = await request.multipart()
    part = await reader.next()
    if part is None or part.name != "file":
        return web.json_response({"status": "error", "message": "file field is required"}, status=400)

    # Basic content-type check (client-provided)
    content_type = part.headers.get("Content-Type", "").lower()
    if content_type.split(";")[0] not in ALLOWED_IMAGE_MIMES:
        return web.json_response({"status": "error", "message": "unsupported file type"}, status=400)

    # generate safe filename
    ext = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp"
    }.get(content_type.split(";")[0], "")

    filename = f"{uuid.uuid4().hex}{ext}"
    dest_path = os.path.join(upload_dir, filename)

    # stream write with size limit
    size = 0
    try:
        async with aiofiles.open(dest_path, "wb") as f:
            while True:
                chunk = await part.read_chunk()  # default chunk size
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    # remove partial file
                    await f.close()
                    try:
                        os.remove(dest_path)
                    except Exception:
                        pass
                    return web.json_response({"status": "error", "message": "file too large"}, status=413)
                await f.write(chunk)
    except Exception:
        # cleanup on error
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception:
            pass
        request.app.logger.exception("upload failed")
        return web.json_response({"status": "error", "message": "upload failed"}, status=500)

    # Build public URL. If you serve uploads statically under upload_url_prefix:
    prefix = request.app.get("upload_url_prefix", "/uploads")
    public_url = f"{prefix.rstrip('/')}/{filename}"

    return web.json_response({"status": "ok", "url": public_url})




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

# --- ORDER MANAGEMENT ---
@admin_required
async def list_orders(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        # We join with payment proof so we can see the image URL immediately
        rows = await conn.fetch("""
            SELECT o.*, p.payment_proof_url, p.method as payment_method
            FROM asbeza_orders o
            LEFT JOIN asbeza_order_payments p ON o.id = p.order_id
            ORDER BY o.created_at DESC
        """)
        # Convert asyncpg records to dicts and handle datetime
        orders = [dict(r) for r in rows]
        # Helper to stringify date for JSON
        for o in orders: o['created_at'] = o['created_at'].isoformat()
    return web.json_response({"status": "ok", "orders": orders})

@admin_required
async def get_order_details(request: web.Request) -> web.Response:
    order_id = int(request.match_info['id'])
    async with request.app["db"]._open_connection() as conn:
        # Get the specific items and their variant names
        items = await conn.fetch("""
            SELECT oi.*, v.name as variant_name, i.name as item_name, i.image_url
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            WHERE oi.order_id = $1
        """, order_id)
        
        return web.json_response({
            "status": "ok", 
            "items": [dict(r) for r in items]
        })

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

@admin_required
async def get_dashboard_stats(request: web.Request) -> web.Response:
    async with request.app["db"]._open_connection() as conn:
        # 1. Revenue trend (last 7 days)
        trend = await conn.fetch("""
            SELECT DATE(created_at) as date, SUM(total_price) as total
            FROM asbeza_orders
            WHERE created_at > CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(created_at) ORDER BY DATE(created_at)
        """)

        # Convert date objects to strings
        trend_data = []
        for r in trend:
            d = dict(r)
            if isinstance(d["date"], (datetime.date, datetime.datetime)):
                d["date"] = d["date"].isoformat()
            trend_data.append(d)

        # 2. Top Selling Items
        top_items = await conn.fetch("""
            SELECT i.name, COUNT(oi.id) as sales
            FROM asbeza_order_items oi
            JOIN asbeza_variants v ON oi.variant_id = v.id
            JOIN asbeza_items i ON v.item_id = i.id
            GROUP BY i.name ORDER BY sales DESC LIMIT 5
        """)

        return web.json_response({
            "status": "ok",
            "trend": trend_data,
            "top_selling": [dict(r) for r in top_items]
        })
