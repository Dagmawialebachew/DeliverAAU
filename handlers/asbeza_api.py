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

async def asbeza_checkout(request: web.Request) -> web.Response:
    """
    Expects JSON:
    {
      "user_id": 12345,
      "items": [
        {"variant_id": 1, "quantity": 2, "price": 120},
        ...
      ]
    }
    Returns:
    { "status": "ok", "order_id": 42, "upfront": 96 }
    """
    db = request.app["db"]

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "invalid json"}, status=400)

    user_id = payload.get("user_id")
    items: List[Dict] = payload.get("items", [])

    if not user_id or not isinstance(items, list) or not items:
        return web.json_response({"status": "error", "message": "user_id and items are required"}, status=400)

    # Validate items
    validated_items = []
    for idx, it in enumerate(items):
        try:
            variant_id = int(it.get("variant_id"))
            quantity = int(it.get("quantity", 1))
            price = float(it.get("price"))
            if quantity <= 0 or price < 0:
                raise ValueError()
        except Exception:
            return web.json_response({"status": "error", "message": f"invalid item at index {idx}"}, status=400)

        validated_items.append({"variant_id": variant_id, "quantity": quantity, "price": price})

    # Compute totals
    total = sum(it["price"] * it["quantity"] for it in validated_items)
    upfront = int(total * 0.4)  # integer birr upfront

    # Insert order + items in DB
    try:
        async with db._open_connection() as conn:
            order_id = await conn.fetchval(
                """
                INSERT INTO asbeza_orders (user_id, total_price, upfront_paid, status)
                VALUES ($1, $2, $3, 'pending')
                RETURNING id
                """,
                user_id, total, upfront
            )

            # Insert order items
            for it in validated_items:
                await conn.execute(
                    """
                    INSERT INTO asbeza_order_items (order_id, variant_id, quantity, price)
                    VALUES ($1, $2, $3, $4)
                    """,
                    order_id, it["variant_id"], it["quantity"], it["price"]
                )

    except Exception as e:
        # log server-side error
        request.app.logger.exception("Checkout DB error")
        return web.json_response({"status": "error", "message": "internal server error"}, status=500)

    return web.json_response({"status": "ok", "order_id": order_id, "upfront": upfront})


def setup_asbeza_routes(app: web.Application):
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)
