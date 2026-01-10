# # test.py (PostgreSQL Version)
# import asyncio
# import json
# from datetime import datetime
# # Assuming you have the Database class defined and imported from its location
# # Example: from database.db import Database

# # --- Dummy Database Class (Replace with your actual import and initialization) ---
# # NOTE: You MUST replace this with your actual Database class and connection logic.
# class DummyDatabase:
#     def __init__(self, dsn):
#         self.dsn = dsn # Connection string or DSN
#         # Assume your actual Database class has an _open_connection method
#         # that yields an asyncpg connection object.

#     async def _open_connection(self):
#         """Mock connection for demonstration."""
#         import asyncpg # Must be imported for context
#         try:
#             conn = await asyncpg.connect(self.dsn)
#             try:
#                 yield conn
#             finally:
#                 await conn.close()
#         except Exception as e:
#             print(f"Error connecting to database: {e}")
#             raise

#     # Helper method to convert asyncpg record to dict
#     def _row_to_dict(self, record):
#         return dict(record) if record else None

# # Initialize the Database (Use your actual DSN or connection string)
# # DB_DSN = "postgresql://user:password@host:port/dbname" 
# # db = Database(dsn=DB_DSN) 
# # --- End Dummy Setup ---


# async def show_orders_postgres(db_instance: DummyDatabase):
#     """Fetches and prints all orders from the PostgreSQL database."""
#     print("--- 📦 All Orders Status (PostgreSQL) ---")
    
#     # Use the Database class's internal connection context manager
#     async with db_instance._open_connection() as conn:
#         try:
#             rows = await conn.fetch("SELECT * FROM orders ORDER BY created_at ASC")
            
#             if not rows:
#                 print("📭 No orders found.")
#                 return

#             for row in rows:
#                 # Convert asyncpg record to dictionary
#                 order = db_instance._row_to_dict(row) 
                
#                 print("===================================")
#                 print(f"🆔 Order ID: {order['id']}")
#                 print(f"👤 User ID: {order['user_id']}")
#                 print(f"🚴 Delivery Guy ID: {order['delivery_guy_id']}")
#                 print(f"🏪 Vendor ID: {order['vendor_id']}")
#                 print(f"📍 Pickup: {order['pickup']}")
#                 print(f"🎯 Dropoff: {order['dropoff']}")
                
#                 # Items JSON parsing remains the same
#                 items = []
#                 try:
#                     # Note: asyncpg may return JSONB columns as dicts directly. 
#                     # We check and parse if it's still a string.
#                     items_data = order.get('items_json')
#                     if isinstance(items_data, str) and items_data:
#                         items = json.loads(items_data)
#                     elif isinstance(items_data, list):
#                         items = items_data
#                 except Exception:
#                     items = []
                
#                 print(f"🛒 Items: {items}")
#                 print(f"💵 Food Subtotal: {order['food_subtotal']} Birr")
#                 print(f"🚚 Delivery Fee: {order['delivery_fee']} Birr")
#                 print(f"📊 Status: {order['status']}")
#                 print(f"💳 Payment Method: {order['payment_method']}")
#                 print(f"💳 Payment Status: {order['payment_status']}")
#                 print(f"🧾 Receipt ID: {order['receipt_id']}")
#                 print(f"📦 Breakdown JSON: {order['breakdown_json']}")
#                 print(f"📡 Live Shared: {order['live_shared']}")
#                 print(f"⏰ Live Expires: {order['live_expires']}")
#                 print(f"📍 Last Lat/Lon: {order['last_lat']}, {order['last_lon']}")
#                 # Timestamps will likely be datetime objects with asyncpg
#                 print(f"🕒 Created At: {order['created_at']}")
#                 print(f"🕒 Accepted At: {order['accepted_at']}")
#                 print(f"🕒 Delivered At: {order['delivered_at']}")
#                 print(f"🕒 Updated At: {order['updated_at']}")
#                 print("===================================\n")
        
#         except Exception as e:
#             print(f"An error occurred during DB query: {e}")

# if __name__ == "__main__":
#     # You need to initialize your Database instance here with your actual DSN
#     # Example placeholder initialization:
#     # db_instance = Database(dsn="postgresql://...") 
    
#     # Since I cannot know your DSN, I'll use the Dummy class structure:
#     # NOTE: To run this, you MUST configure the real Database class and DSN.
#     print("WARNING: Replace DummyDatabase with your actual Database class and DSN.")
#     class FinalDatabase(DummyDatabase):
#         pass # Assume this is the real class after imports

#     DB_DSN_MOCK = "postgresql://test:test@localhost:5432/testdb" 
#     db_instance = FinalDatabase(dsn=DB_DSN_MOCK) 
    
#     # asyncio.run(show_orders_postgres(db_instance))
#     print("Script ready for execution (uncomment asyncio.run to run against your PostgreSQL).")



# import asyncio

# from database.db import Database

# async def test_schema():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # List all tables
#         tables = await conn.fetch("""
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema='public'
#         """)
#         print("Tables:", [t['table_name'] for t in tables])

#         # Show columns + types for delivery_guys
#         cols = await conn.fetch("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = 'delivery_guys'
#         """)
#         print("delivery_guys columns:", [(c['column_name'], c['data_type']) for c in cols])

#         # Show columns + types for orders
#         cols = await conn.fetch("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = 'orders'
#         """)
#         print("orders columns:", [(c['column_name'], c['data_type']) for c in cols])

# asyncio.run(test_schema())


#Test the assign delivery guy logic


# import asyncio
# from database.db import Database

# async def inspect_table_schema(conn, table_name: str):
#     print(f"\nInspecting table: {table_name}")

#     # List columns and data types
#     cols = await conn.fetch("""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_name = $1
#     """, table_name)

#     for col in cols:
#         print(f" - {col['column_name']:20} | {col['data_type']}")

# async def test_schema():
#     db = Database()
#     await db.init_pool()

#     async with db._open_connection() as conn:
#         # List all tables in public schema
#         tables = await conn.fetch("""
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema='public'
#         """)
#         table_names = [t['table_name'] for t in tables]
#         print("Tables in public schema:", table_names)

#         # Inspect specific tables
#         for table in ['delivery_guys', 'orders']:
#             if table in table_names:
#                 await inspect_table_schema(conn, table)
#             else:
#                 print(f"Table '{table}' not found in schema.")

# if __name__ == "__main__":
#     asyncio.run(test_schema())




# import asyncio
# import logging
# import json
# from database.db import Database
# from utils.helpers import assign_delivery_guy  # replace with the class where assign_delivery_guy lives

# logging.basicConfig(level=logging.INFO)

# ORDER_ID = 5  # pick a real order ID to test

# async def inspect_schema(conn):
#     # List all tables
#     tables = await conn.fetch("""
#         SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema='public'
#     """)
#     print(" Tables:", [t['table_name'] for t in tables])

#     # Show columns for delivery_guys
#     cols = await conn.fetch("""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_name = 'delivery_guys'
#     """)
#     print("delivery_guys columns:", [(c['column_name'], c['data_type']) for c in cols])

#     # Show columns for orders
#     cols = await conn.fetch("""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_name = 'orders'
#     """)
#     print("orders columns:", [(c['column_name'], c['data_type']) for c in cols])


# async def test_assignment(order_id: int):
#     db = Database()
#     await db.init_pool()

#     async with db._open_connection() as conn:
#         await inspect_schema(conn)

#     # Instantiate your service class
   
#     # Run the assignment
#     result = await assign_delivery_guy(db=db, order_id=order_id)
#     if result:
#         print("\n Assigned delivery guy:", result)
#     else:
#         print("\n No delivery guy could be assigned.")



# import asyncio
# import logging
# from database.db import Database

# logging.basicConfig(level=logging.INFO)

# async def inspect_delivery_guys():
#     db = Database()
#     await db.init_pool()

#     async with db._open_connection() as conn:
#         # First query: see all delivery guys
#         rows = await conn.fetch("""
#             SELECT id, name, active, blocked, total_requests, last_lat, last_lon, campus
#             FROM delivery_guys;
#         """)
#         print("\nDelivery Guys:")
#         for r in rows:
#             print(dict(r))

#         # Second query: active/in-progress counts
#         rows = await conn.fetch("""
#             WITH dg_active_counts AS (
#                 SELECT 
#                     delivery_guy_id AS dg_id, 
#                     COUNT(*) FILTER (WHERE status IN ('assigned','preparing','ready','in_progress')) AS active_count,
#                     COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress_count
#                 FROM orders
#                 GROUP BY delivery_guy_id
#             )
#             SELECT dg.id, dg.name, dg.active, dg.blocked,
#                    COALESCE(dac.active_count,0) AS active_orders,
#                    COALESCE(dac.in_progress_count,0) AS in_progress_orders
#             FROM delivery_guys dg
#             LEFT JOIN dg_active_counts dac ON dg.id = dac.dg_id;
#         """)
#         print("\nDelivery Guy Status Counts:")
#         for r in rows:
#             print(dict(r))

# if __name__ == "__main__":
#     asyncio.run(test_assignment(ORDER_ID))
#     asyncio.run(inspect_delivery_guys())





# import asyncio
# from database.db import Database

# ORDER_ID = 6

# async def find_student_by_order(order_id: int):
#     db = Database()
#     await db.init_pool()

#     async with db._open_connection() as conn:
#         # Step 1: fetch the order
#         order = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
#         if not order:
#             print(f"No order found with id={order_id}")
#             return

#         order_dict = dict(order)
#         print(f"Found order: {order_dict}")

#         # Step 2: get the user_id from the order
#         internal_user_id = order_dict["user_id"]
#         print(f"Internal user_id from order: {internal_user_id}")

#         # Step 3: fetch the student by internal id
#         student = await db.get_user_by_id(internal_user_id)
#         if not student:
#             print(f"No student found with id={internal_user_id}")
#             return

#         # Step 4: print student details
#         print("Student record:")
#         print(f" - ID: {student['id']}")
#         print(f" - Telegram ID: {student['telegram_id']}")
#         print(f" - Name: {student['first_name']}")
#         print(f" - Phone: {student['phone']}")
#         print(f" - Campus: {student['campus']}")

# if __name__ == "__main__":
#     asyncio.run(find_student_by_order(ORDER_ID))




# import asyncio
# import asyncpg
# from datetime import datetime

# async def show_current_orders(dsn: str):
#     """Prints current (non-delivered) orders from the database."""
#     try:
#         conn = await asyncpg.connect(dsn)

#         query = """
#         SELECT *
#         FROM orders
#         WHERE status != 'delivered'
#         ORDER BY created_at DESC
#         """

#         rows = await conn.fetch(query)

#         if not rows:
#             print("No current orders found.")
#             return

#         print("--- Current Orders ---")
#         for row in rows:
#             print("===================================")
#             print(f"Order ID: {row['id']}")
#             print(f"User ID: {row['user_id']}")
#             print(f"Delivery Guy ID: {row['delivery_guy_id']}")
#             print(f"Vendor ID: {row['vendor_id']}")
#             print(f"Pickup: {row['pickup']}")
#             print(f"Dropoff: {row['dropoff']}")
#             print(f"Status: {row['status']}")
#             print(f"Payment Status: {row['payment_status']}")
#             print(f"Created At: {row['created_at']}")
#             print(f"Updated At: {row['updated_at']}")
#             print("===================================\n")

#         await conn.close()

#     except Exception as e:
#         print(f"Error fetching current orders: {e}")

# if __name__ == "__main__":
#     DB_DSN = "postgresql://neondb_owner:npg_gTKxHQ7qdtC0@ep-soft-glitter-ad6vxp8t-pooler.c-2.us-east-1.aws.neon.tech/deliveryaau?sslmode=require&channel_binding=require"
#     asyncio.run(show_current_orders(DB_DSN))




from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
import asyncio



# @router.message(Command("id"))
# async def show_id(message: Message):
#     await message.answer(f"Your Telegram ID is: {message.from_user.id}")

# dp.include_router(router)

# async def main():
#     await bot.delete_webhook(drop_pending_updates=True)
#     await dp.start_polling(bot)

# if __name__ == "__main__":
#     asyncio.run(main())


# import asyncio
# from typing import List, Tuple
# from database.db import Database
# from database import db

# async def seed_delivery_guys(db: Database) -> None:
#     """
#     Insert a single delivery guy into the delivery_guys table.
#     Uses ON CONFLICT DO NOTHING so it won't raise if user_id already exists.
#     """

#     # (user_id, telegram_id, name, campus, phone, active, blocked,
#     #  total_deliveries, accepted_requests, total_requests,
#     #  coins, xp, level)
#     delivery_guys_data: List[Tuple] = [
#         (
#             1001,            # user_id
#             7112595006,      # telegram_id
#             "Kupachata",     # name
#             "4kilo",         # campus
#             "+251960306801", # phone (example; change as needed)
#             True,            # active
#             False,           # blocked
#             12,              # total_deliveries
#             14,              # accepted_requests
#             16,              # total_requests
#             10,              # coins
#             260,             # xp
#             3                # level
#         )
#     ]

#     async with db.open_connection() as conn:
#         insert_sql = """
#             INSERT INTO delivery_guys 
#             (user_id, telegram_id, name, campus, phone, active, blocked,
#              total_deliveries, accepted_requests, total_requests,
#              coins, xp, level)
#             VALUES ($1::BIGINT, $2::BIGINT, $3, $4, $5, $6, $7,
#                     $8, $9, $10, $11, $12, $13)
#             ON CONFLICT (user_id) DO NOTHING
#         """

#         for row in delivery_guys_data:
#             await conn.execute(insert_sql, *row)

#     print("✅ Delivery guy (with phone) inserted successfully (or already existed).")


# if __name__ == "__main__":
#     asyncio.run(seed_delivery_guys(db))




# import asyncio
from database.db import Database

# async def test_schema():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # List all tables in public schema
#         tables = await conn.fetch("""
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema='public'
#             ORDER BY table_name
#         """)
#         print("Tables:", [t['table_name'] for t in tables])

#         # Show columns + types for orders
#         orders_cols = await conn.fetch("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = 'orders'
#             ORDER BY ordinal_position
#         """)
#         print("orders columns:", [(c['column_name'], c['data_type']) for c in orders_cols])

#         # Show columns + types for vendors
#         vendors_cols = await conn.fetch("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = 'vendors'
#             ORDER BY ordinal_position
#         """)
#         print("vendors columns:", [(c['column_name'], c['data_type']) for c in vendors_cols])

#         # Optionally: peek at a few rows
#         sample_orders = await conn.fetch("SELECT * FROM orders LIMIT 5;")
#         print("Sample orders:", [dict(r) for r in sample_orders])

#         sample_vendors = await conn.fetch("SELECT * FROM vendors LIMIT 5;")
#         print("Sample vendors:", [dict(r) for r in sample_vendors])

# if __name__ == "__main__":
#     asyncio.run(test_schema())





# import asyncio

# async def get_order_by_id(conn, order_id: int):
#     row = await conn.fetchrow(
#         """
#         SELECT id, user_id, vendor_id, food_subtotal, delivery_fee, status,
#                items_json, dropoff, created_at, updated_at
#         FROM orders
#         WHERE id = $1
#         """,
#         order_id
#     )
#     return row

# async def update_order_delivery_fee(conn, order_id: int, new_fee: float):
#     result = await conn.execute(
#         """
#         UPDATE orders
#         SET delivery_fee = $2,
#             updated_at = CURRENT_TIMESTAMP
#         WHERE id = $1
#         """,
#         order_id, new_fee
#     )
#     return result

# async def update_order_status(conn, order_id: int, new_status: str):
#     result = await conn.execute(
#         """
#         UPDATE orders
#         SET status = $2,
#             updated_at = CURRENT_TIMESTAMP
#         WHERE id = $1
#         """,
#         order_id, new_status
#     )
#     return result

# async def test_schema():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # Fetch order #78
#         order = await get_order_by_id(conn, 78)
#         print("Before update:", dict(order) if order else "Order not found")

      

#         # Fetch again to confirm
#         order_after = await get_order_by_id(conn, 78)
#         print("After update:", dict(order_after) if order_after else "Order not found")

#         # Update delivery fee
#         res_fee = await update_order_delivery_fee(conn, 78, 20.0)
#         print("Delivery fee update result:", res_fee)

# if __name__ == "__main__":
#     asyncio.run(test_schema())

#--------------------- Referall and Bites Generation

# import asyncio
# import asyncpg
# import random
# import string

# from database.db import Database  # assuming you have this class
# from app_context import bot

# # --- Referral code generator ---
# def generate_referral_code(user_id: int) -> str:
#     # Example: UB + user_id padded + random 3 chars
#     suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
#     return f"UB{str(user_id).zfill(4)}{suffix}"

# # --- Starter bites mapping ---


# def starter_bites(order_count: int) -> int:
#     if 3 <= order_count <= 4:
#         return 1
#     elif 5 <= order_count <= 6:
#         return 2
#     elif 7 <= order_count <= 8:
#         return 3
#     elif 9 <= order_count <= 10:
#         return 4
#     elif order_count >= 11:
#         return 5
#     return 0

# async def backfill_referrals_and_bites():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # Fetch all users
#         users = await conn.fetch("SELECT id, telegram_id, referral_code FROM users")

#         for user in users:
#             uid = user["telegram_id"]
#             try: 
#                 chat = await bot.get_chat(uid)
#                 first = chat.first_name or ""
#                 last = chat.last_name or ""
#                 display_name = (first + " " + last).strip()
#                 print('here is a display name', display_name)
#             except Exception:
#                 display_name = f"User{uid}"
#             uid = user['id']
#             orders_count = await conn.fetchval(
#                 "SELECT COUNT(*) FROM orders WHERE user_id=$1 AND status='delivered'",
#                 uid
#             )
#             bites = starter_bites(orders_count)
#             print('here is the orders count', orders_count)
#             print('here is the bits result', bites)

#             if bites > 0:
#                 await conn.execute(
#                     """
#                     INSERT INTO leaderboards (user_id, display_name, bites, last_updated)
#                     VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
#                     ON CONFLICT (user_id)
#                     DO UPDATE SET display_name = EXCLUDED.display_name,
#                                 bites = EXCLUDED.bites,
#                                 last_updated = CURRENT_TIMESTAMP
#                     """,
#                     uid, display_name, bites
#                 )


#         print("✅ Backfill complete for referral codes and starter bites.")

# if __name__ == "__main__":
#     asyncio.run(backfill_referrals_and_bites())








# import asyncio
# import asyncpg
# import random, string
# from database.db import Database

# def starter_bites(order_count: int) -> int:
#     if 3 <= order_count <= 4:
#         return 1
#     elif 5 <= order_count <= 6:
#         return 2
#     elif 7 <= order_count <= 8:
#         return 3
#     elif 9 <= order_count <= 10:
#         return 4
#     elif order_count >= 11:
#         return 5
#     return 0


# async def rebuild_leaderboard(bot):
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # Truncate leaderboard
#         await conn.execute("TRUNCATE TABLE leaderboards RESTART IDENTITY;")
#         print("✅ Leaderboard table truncated")

#         # Fetch first 50 users
#         users = await conn.fetch(
#             "SELECT id, telegram_id FROM users ORDER BY id"
#         )

#         for user in users:
#             uid = user["id"]          # internal PK
#             tg_id = user["telegram_id"]

#             # --- Fetch display name from Telegram ---
#             display_name = f"User{tg_id}"
#             if tg_id:
#                 try:
#                     chat = await bot.get_chat(tg_id)
#                     first = chat.first_name or ""
#                     last = chat.last_name or ""
#                     display_name = (first + " " + last).strip() or display_name
#                 except Exception as e:
#                     print(f"⚠️ Could not fetch chat for {tg_id}: {e}")

#             # --- Count delivered orders ---
#             orders_count = await conn.fetchval(
#                 "SELECT COUNT(*) FROM orders WHERE user_id=$1 AND status='delivered'",
#                 uid
#             )
#             bites = starter_bites(orders_count)

#             if bites > 0:
#                 await conn.execute(
#                     """
#                     INSERT INTO leaderboards (user_id, display_name, bites, last_updated)
#                     VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
#                     ON CONFLICT (user_id) DO UPDATE
#                     SET display_name = EXCLUDED.display_name,
#                         bites = EXCLUDED.bites,
#                         last_updated = CURRENT_TIMESTAMP
#                     """,
#                     uid, display_name, bites
#                 )
#                 print(f"Inserted {uid} ({display_name}) with {bites} bites")

#         print("🎯 Leaderboard rebuild complete")


# if __name__ == "__main__":
#     from app_context import bot
#     asyncio.run(rebuild_leaderboard(bot))




import asyncio
from database.db import Database

async def check_users_and_leaderboard():
    db = Database()
    await db.init_pool()
    async with db._open_connection() as conn:
        # Show first 10 users with referral codes
        # users = await conn.fetch(
        #     """
        #     SELECT id, telegram_id, referral_code, created_at, updated_at
        #     FROM users
        #     ORDER BY id
        #     LIMIT 10
        #     """
        # )
        # print("\n=== Users Table Sample ===")
        # for u in users:
        #     print(dict(u))

        # Show first 10 leaderboard entries
        leaders = await conn.fetch(
            """
            SELECT user_id, display_name, bites, rank, last_updated
            FROM leaderboards
            ORDER BY bites DESC
            """
        )
        print("\n=== Leaderboard Sample ===")
        for l in leaders:
            print(dict(l))


if __name__ == "__main__":
    asyncio.run(check_users_and_leaderboard())




# import asyncio
# from database.db import Database

# async def print_orders_sample(limit: int = 10):
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         rows = await conn.fetch(
#             """
#             SELECT id, user_id, vendor_id, pickup, dropoff, food_subtotal,
#                    delivery_fee, status, created_at, delivered_at
#             FROM orders
#             ORDER BY id
#             LIMIT $1
#             """,
#             limit
#         )

#         print("\n=== Orders Table Sample ===")
#         for row in rows:
#             print(dict(row))

# if __name__ == "__main__":
#     asyncio.run(print_orders_sample())






# import asyncio
# from database.db import Database

async def upsert_leaderboard_bites(user_id: int, display_name: str, bites: int = 50):
    db = Database()
    await db.init_pool()
    async with db._open_connection() as conn:
        result = await conn.execute(
            """
            INSERT INTO leaderboards (user_id, display_name, bites, last_updated)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id)
            DO UPDATE SET bites = $3,
                          display_name = EXCLUDED.display_name,
                          last_updated = CURRENT_TIMESTAMP
            """,
            user_id, display_name, bites
        )
        print(f"Upsert result: {result}")

if __name__ == "__main__":
    # Replace with the user_id and display_name you want to test
    test_user_id = 1
    test_display_name = "Nati💀"
    asyncio.run(upsert_leaderboard_bites(test_user_id, test_display_name, 17))


# import asyncio
# from database.db import Database

# async def deduplicate_leaderboards():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         # Delete duplicates, keeping the row with the highest bites (or latest timestamp)
#         result = await conn.execute(
#             """
#             DELETE FROM leaderboards l
#             USING leaderboards l2
#             WHERE l.user_id = l2.user_id
#               AND l.ctid < l2.ctid;
#             """
#         )
#         print(f"Deduplication result: {result}")

# if __name__ == "__main__":
#     asyncio.run(deduplicate_leaderboards())






# import asyncio
# from database.db import Database  # adjust import path if needed

# async def delete_user_by_telegram_id(telegram_id: int):
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         result = await conn.execute(
#             "DELETE FROM users WHERE telegram_id=$1",
#             telegram_id
#         )
#         print(f"✅ Delete executed for telegram_id={telegram_id} | result={result}")

# if __name__ == "__main__":
#     # Replace with the telegram_id you want to delete
#     tg_id_to_delete = 7701933259
#     asyncio.run(delete_user_by_telegram_id(tg_id_to_delete))




# import asyncio
# from database.db import Database  # adjust path if needed

# async def show_users():
#     db = Database()
#     await db.init_pool()
#     async with db._open_connection() as conn:
#         rows = await conn.fetch("SELECT id, telegram_id, referral_code, referred_by FROM users")
#         print("=== Users Table ===")
#         for row in rows:
#             print(
#                 f"id={row['id']} | telegram_id={row['telegram_id']} | "
#                 f"referral_code={row.get('referral_code')} | referred_by={row['referred_by']}"
#             )

# if __name__ == "__main__":
#     asyncio.run(show_users())
