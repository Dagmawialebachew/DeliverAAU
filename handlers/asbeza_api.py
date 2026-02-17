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


async def asbeza_checkout(request):
    db = request.app["db"]
    data = await request.json()

    user_id = data["user_id"]
    items = data["items"]  # [{ "variant_id": 1, "quantity": 2, "price": 120 }, ...]

    total = sum(item["price"] * item.get("quantity", 1) for item in items)
    upfront = int(total * 0.4)

    async with db._open_connection() as conn:
        # Insert order
        order_id = await conn.fetchval("""
            INSERT INTO asbeza_orders (user_id, total_price, upfront_paid, status)
            VALUES ($1, $2, $3, 'pending')
            RETURNING id
        """, user_id, total, upfront)

        # Insert order items
        for item in items:
            await conn.execute("""
                INSERT INTO asbeza_order_items (order_id, variant_id, quantity, price)
                VALUES ($1, $2, $3, $4)
            """, order_id, item["variant_id"], item.get("quantity", 1), item["price"])

    return web.json_response({
        "status": "ok",
        "order_id": order_id,
        "upfront": upfront
    })


def setup_asbeza_routes(app: web.Application):
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)
