from aiohttp import web

async def get_asbeza_items(request):
    db = request.app["db"]
    async with db._open_connection() as conn:
        rows = await conn.fetch("""
            SELECT id, name, description, base_price, image_url
            FROM asbeza_items
            WHERE active = TRUE
        """)
    items = [dict(r) for r in rows]
    return web.json_response({"items": items})

from typing import List, Dict
import math

async def asbeza_checkout(request: web.Request) -> web.Response:
    """
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
      "upfront_paid": 192
    }
    Returns:
    { "status": "ok", "order_id": 42, "upfront": 192 }
    """
    db = request.app["db"]

    # Delivery fee rule (server-side)
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

    if items is None or not isinstance(items, list) or len(items) == 0:
        return web.json_response({"status": "error", "message": "items are required"}, status=400)

    # Validate item structure and collect variant ids
    variant_ids = []
    validated_items_input = []
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

    # Query DB for variant prices and existence
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
            client_delivery = payload.get("delivery_fee")
            client_total = payload.get("total_price")
            client_upfront = payload.get("upfront_paid")

            # If client provided any of these, ensure they match computed values
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

            # Insert order
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

    except Exception:
        request.app.logger.exception("Checkout DB error")
        return web.json_response({"status": "error", "message": "internal server error"}, status=500)

    return web.json_response({"status": "ok", "order_id": order_id, "upfront": upfront_paid})

def setup_asbeza_routes(app: web.Application):
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)
    app.router.add_post("/api/asbeza/upload_screenshot", upload_screenshot)  # new



import os
import uuid
import aiofiles

ALLOWED_IMAGE_MIMES = {"image/jpeg", "image/png", "image/webp", "image/jpg"}

async def upload_screenshot(request: web.Request) -> web.Response:
    """
    POST /api/asbeza/upload_screenshot
    Accepts multipart/form-data with field 'file'
    Returns: { "status":"ok", "url": "<public-url>" }
    """
    upload_dir = request.app.get("upload_dir", "./uploads")
    max_bytes = request.app.get("upload_max_bytes", 6 * 1024 * 1024)

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

    # Optionally: further server-side validation (image dimensions, scanning) here.

    # Build public URL. If you serve uploads statically under upload_url_prefix:
    prefix = request.app.get("upload_url_prefix", "/uploads")
    public_url = f"{prefix.rstrip('/')}/{filename}"

    return web.json_response({"status": "ok", "url": public_url})
