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





import asyncio
from database.db import Database

ORDER_ID = 6

async def find_student_by_order(order_id: int):
    db = Database()
    await db.init_pool()

    async with db._open_connection() as conn:
        # Step 1: fetch the order
        order = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
        if not order:
            print(f"No order found with id={order_id}")
            return

        order_dict = dict(order)
        print(f"Found order: {order_dict}")

        # Step 2: get the user_id from the order
        internal_user_id = order_dict["user_id"]
        print(f"Internal user_id from order: {internal_user_id}")

        # Step 3: fetch the student by internal id
        student = await db.get_user_by_id(internal_user_id)
        if not student:
            print(f"No student found with id={internal_user_id}")
            return

        # Step 4: print student details
        print("Student record:")
        print(f" - ID: {student['id']}")
        print(f" - Telegram ID: {student['telegram_id']}")
        print(f" - Name: {student['first_name']}")
        print(f" - Phone: {student['phone']}")
        print(f" - Campus: {student['campus']}")

if __name__ == "__main__":
    asyncio.run(find_student_by_order(ORDER_ID))
