# # test.py
# import asyncio
# import aiosqlite

# DB_PATH = "deliver_aau.db"  # adjust your path

# async def main():
#     print("Delivery Guys Status Check\n")
#     print(f"{'ID':<4} {'Name':<20} {'Campus':<10} {'Active':<6} {'Last Lat':<10} {'Last Lon':<10} {'Total Deliveries':<15}")
#     print("-"*80)

#     async with aiosqlite.connect(DB_PATH) as db:
#         db.row_factory = aiosqlite.Row
#         async with db.execute("SELECT * FROM delivery_guys") as cur:
#             rows = await cur.fetchall()

#         for dg in rows:
#             print(
#                 f"{dg['id']:<4} {dg['name']:<20} {dg['campus']:<10} {dg['active']:<6} "
#                 f"{str(dg['last_lat'] or ''):<10} {str(dg['last_lon'] or ''):<10} {dg['total_deliveries']:<15}"
#             )

# asyncio.run(main())

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# test.py
import aiosqlite
import asyncio
import json
from datetime import datetime

DB_PATH = "deliver_aau.db"  # adjust to your actual db path

async def show_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders ORDER BY created_at ASC") as cur:
            rows = await cur.fetchall()
            if not rows:
                print("📭 No orders found.")
                return

            for row in rows:
                order = dict(row)
                print("===================================")
                print(f"🆔 Order ID: {order['id']}")
                print(f"👤 User ID: {order['user_id']}")
                print(f"🚴 Delivery Guy ID: {order['delivery_guy_id']}")
                print(f"🏪 Vendor ID: {order['vendor_id']}")
                print(f"📍 Pickup: {order['pickup']}")
                print(f"🎯 Dropoff: {order['dropoff']}")
                try:
                    items = json.loads(order['items_json']) if order['items_json'] else []
                except Exception:
                    items = []
                print(f"🛒 Items: {items}")
                print(f"💵 Food Subtotal: {order['food_subtotal']} Birr")
                print(f"🚚 Delivery Fee: {order['delivery_fee']} Birr")
                print(f"📊 Status: {order['status']}")
                print(f"💳 Payment Method: {order['payment_method']}")
                print(f"💳 Payment Status: {order['payment_status']}")
                print(f"🧾 Receipt ID: {order['receipt_id']}")
                print(f"📦 Breakdown JSON: {order['breakdown_json']}")
                print(f"📡 Live Shared: {order['live_shared']}")
                print(f"⏰ Live Expires: {order['live_expires']}")
                print(f"📍 Last Lat/Lon: {order['last_lat']}, {order['last_lon']}")
                print(f"🕒 Created At: {order['created_at']}")
                print(f"🕒 Accepted At: {order['accepted_at']}")
                print(f"🕒 Delivered At: {order['delivered_at']}")
                print(f"🕒 Updated At: {order['updated_at']}")
                print("===================================\n")

if __name__ == "__main__":
    asyncio.run(show_orders())
