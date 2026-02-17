from aiohttp import web
import json

async def get_asbeza_items(request):
    db = request.app["db"]

    rows = await db.fetch("""
        SELECT id, name, price
        FROM asbeza_items
        WHERE active = TRUE
    """)
    print('here are rows from db:', rows)

    return web.json_response(rows)


async def asbeza_checkout(request):
    db = request.app["db"]
    data = await request.json()

    user_id = data["user_id"]
    items = data["items"]

    total = sum(item["price"] for item in items)
    upfront = int(total * 0.4)

    await db.execute("""
        INSERT INTO asbeza_orders
        (user_id, items_json, total_price, upfront_amount)
        VALUES ($1, $2, $3, $4)
    """, user_id, json.dumps(items), total, upfront)

    return web.json_response({
        "status": "ok",
        "upfront": upfront
    })


# ADD THIS FUNCTION
def setup_asbeza_routes(app):
    app.router.add_get("/api/asbeza/items", get_asbeza_items)
    app.router.add_post("/api/asbeza/checkout", asbeza_checkout)