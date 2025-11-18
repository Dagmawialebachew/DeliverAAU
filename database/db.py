# database/db.py
import json
import aiosqlite
from typing import Optional, Dict, Any, List
import datetime

# --- 1. UNIFIED SCHEMA SQL ---
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE,
    role TEXT,
    first_name TEXT,
    phone TEXT,
    campus TEXT,
    coins INTEGER DEFAULT 0,
    xp INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS delivery_guys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE, -- Made UNIQUE for direct lookup
    name TEXT,
    campus TEXT,
    phone TEXT, 
    active INTEGER DEFAULT 0, -- Default to offline
    blocked INTEGER DEFAULT 0, -- New field
    total_deliveries INTEGER DEFAULT 0,
    accepted_requests INTEGER DEFAULT 0, -- NEW: Used by calc_acceptance_rate
    total_requests INTEGER DEFAULT 0,    -- NEW: The missing column (assigned requests)
    last_lat REAL NULL,
    last_lon REAL NULL,
    skipped_requests INTEGER DEFAULT 0, -- New field
    last_skip_at TIMESTAMP NULL, -- New field
    last_online_at TIMESTAMP NULL, -- New field
    last_offline_at TIMESTAMP NULL, -- New field
    coins INTEGER DEFAULT 0, -- Gamification field
    xp INTEGER DEFAULT 0, -- Gamification field
    level INTEGER DEFAULT 1 -- Gamification field
);

CREATE TABLE IF NOT EXISTS vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE,          -- Vendor's Telegram account (must-have for bot access)
    name TEXT NOT NULL,                  -- Café / vendor name
    menu_json TEXT,                      -- Structured menu (JSON: items, categories, prices)
    status TEXT DEFAULT 'active',        -- active / inactive
    rating_avg REAL DEFAULT 0.0,         -- Cached average rating
    rating_count INTEGER DEFAULT 0,      -- Number of ratings received
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    delivery_guy_id INTEGER NULL, -- delivery_guy_id is NULL initially (pending)
    vendor_id INTEGER,
    pickup TEXT,
    dropoff TEXT,
    items_json TEXT,
    food_subtotal REAL DEFAULT 0.0,
    delivery_fee REAL DEFAULT 0.0,
    status TEXT, -- pending / assigned / preparing / ready / in_progress / delivered / cancelled
    payment_method TEXT,
    payment_status TEXT,
    receipt_id INTEGER,
    breakdown_json TEXT, -- New field (includes coords, metadata)
    live_shared INTEGER DEFAULT 0, 
    live_expires TIMESTAMP NULL,      
    last_lat REAL NULL,              
    last_lon REAL NULL,              
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    accepted_at TIMESTAMP NULL, -- New field
    delivered_at TIMESTAMP NULL, -- New field
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dg_id INTEGER,
    date TEXT, -- YYYY-MM-DD
    deliveries INTEGER DEFAULT 0,
    earnings REAL DEFAULT 0.0,
    skipped INTEGER DEFAULT 0,
    assigned INTEGER DEFAULT 0, -- Added to compute acceptance_rate easily
    acceptance_rate REAL DEFAULT 0.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- NEW: track last update
    UNIQUE(dg_id, date) -- Ensure only one entry per DG per day
);


CREATE TABLE IF NOT EXISTS ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    stars INTEGER,
    comment TEXT
);

CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT,
    uploader_user_id INTEGER,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified_by INTEGER,
    verify_status TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    type TEXT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    status TEXT
);

CREATE TABLE IF NOT EXISTS location_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NULL, -- Made nullable
    delivery_guy_id INTEGER NULL, -- Made nullable
    lat REAL,
    lon REAL,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS admin_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT
);

CREATE TABLE IF NOT EXISTS jobs_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT,
    key TEXT,
    status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user_end ON subscriptions(user_id, end_ts);
CREATE INDEX IF NOT EXISTS idx_location_order_ts ON location_logs(order_id, ts);
CREATE INDEX IF NOT EXISTS idx_daily_stats_dg_date ON daily_stats(dg_id, date);
CREATE INDEX IF NOT EXISTS idx_vendors_telegram_id ON vendors(telegram_id);
CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name);
"""

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self) -> None:
        """Initialize SQLite schema if not present."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA_SQL)
            await db.commit()

    # Optional tiny helper used above: expose connection context if you want to reuse connection
    def _open_connection(self):
        return aiosqlite.connect(self.db_path)

    # -------------------- Users --------------------
    async def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def create_user(
        self,
        telegram_id: int,
        role: str,
        first_name: str,
        phone: str,
        campus: str,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO users
                (telegram_id, role, first_name, phone, campus, status)
                VALUES (?, ?, ?, ?, ?, 'active')
                """,
                (telegram_id, role, first_name, phone, campus),
            )
            await db.commit()
            async with db.execute(
                "SELECT id FROM users WHERE telegram_id = ?", (telegram_id,)
            ) as cur:
                row = await cur.fetchone()
                return row[0]
            

    async def get_orders_for_vendor(self, vendor_id: int, *, date: Optional[str] = None, status_filter: Optional[List[str]] = None, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List orders for a vendor, optionally filtered by date (YYYY-MM-DD),
        status list, and paginated (limit/offset).
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            where = ["vendor_id = ?"]
            params = [vendor_id]

            if date:
                where.append("DATE(created_at) = ?")
                params.append(date)

            if status_filter:
                placeholders = ",".join(["?"] * len(status_filter))
                where.append(f"status IN ({placeholders})")
                params.extend(status_filter)

            sql = f"SELECT * FROM orders WHERE {' AND '.join(where)} ORDER BY created_at DESC"
            if limit is not None:
                sql += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])

            async with db.execute(sql, params) as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]


    async def count_orders_for_vendor(self, vendor_id: int, *, date: Optional[str] = None, status_filter: Optional[List[str]] = None) -> int:
        """Count orders for pagination and summaries."""
        async with aiosqlite.connect(self.db_path) as db:
            where = ["vendor_id = ?"]
            params = [vendor_id]

            if date:
                where.append("DATE(created_at) = ?")
                params.append(date)

            if status_filter:
                placeholders = ",".join(["?"] * len(status_filter))
                where.append(f"status IN ({placeholders})")
                params.extend(status_filter)

            sql = f"SELECT COUNT(*) FROM orders WHERE {' AND '.join(where)}"
            async with db.execute(sql, params) as cur:
                row = await cur.fetchone()
                return int(row[0])

            

# Inside Database class

    async def summarize_vendor_day(self, vendor_id: int, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns daily summary: counts, food revenue, delivery fees, ratings snapshot, reliability.
        date format: YYYY-MM-DD (defaults to today if None).
        """
        date = date or datetime.date.today().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # Orders summary (delivered vs cancelled vs prepared)
            async with db.execute(
                """
                SELECT
                SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                SUM(food_subtotal) AS food_revenue,
                SUM(delivery_fee) AS delivery_fees
                FROM orders
                WHERE vendor_id = ? AND DATE(created_at) = ?
                """,
                (vendor_id, date)
            ) as cur:
                s = await cur.fetchone()
                delivered = int(s["delivered_count"] or 0)
                cancelled = int(s["cancelled_count"] or 0)
                food_rev = float(s["food_revenue"] or 0.0)
                delivery_fees = float(s["delivery_fees"] or 0.0)

            # Ratings snapshot (from cached vendor fields)
            async with db.execute("SELECT rating_avg, rating_count FROM vendors WHERE id = ?", (vendor_id,)) as cur:
                v = await cur.fetchone()
                rating_avg = float(v["rating_avg"] if v else 0.0)
                rating_count = int(v["rating_count"] if v else 0)

            # Reliability: prepared+delivered vs cancelled (approximation from statuses that day)
            async with db.execute(
                """
                SELECT
                SUM(CASE WHEN status IN ('preparing','ready','in_progress','delivered') THEN 1 ELSE 0 END) AS progressed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                FROM orders
                WHERE vendor_id = ? AND DATE(created_at) = ?
                """,
                (vendor_id, date)
            ) as cur:
                r = await cur.fetchone()
                progressed = int(r["progressed"] or 0)
                cancelled2 = int(r["cancelled"] or 0)
                denom = progressed + cancelled2
                reliability = 0 if denom == 0 else round(100.0 * progressed / denom)

        return {
            "date": date,
            "delivered": delivered,
            "cancelled": cancelled,
            "food_revenue": food_rev,
            "delivery_fees": delivery_fees,
            "total_payout": food_rev + delivery_fees,
            "rating_avg": rating_avg,
            "rating_count": rating_count,
            "reliability_pct": reliability,
        }


    async def summarize_vendor_week(self, vendor_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Weekly summary across a date range (defaults to current ISO week).
        Returns totals and per-day breakdown.
        """
        if not start_date or not end_date:
            today = datetime.date.today()
            start = today - datetime.timedelta(days=today.weekday())  # Monday
            end = start + datetime.timedelta(days=6)                  # Sunday
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # Totals
            async with db.execute(
                """
                SELECT
                SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                SUM(food_subtotal) AS food_revenue,
                SUM(delivery_fee) AS delivery_fees
                FROM orders
                WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?
                """,
                (vendor_id, start_date, end_date)
            ) as cur:
                t = await cur.fetchone()
                delivered = int(t["delivered_count"] or 0)
                cancelled = int(t["cancelled_count"] or 0)
                food_rev = float(t["food_revenue"] or 0.0)
                delivery_fees = float(t["delivery_fees"] or 0.0)

            # Per-day breakdown
            async with db.execute(
                """
                SELECT DATE(created_at) AS d,
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    SUM(food_subtotal) AS food_revenue,
                    SUM(delivery_fee) AS delivery_fees
                FROM orders
                WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?
                GROUP BY DATE(created_at)
                ORDER BY d ASC
                """,
                (vendor_id, start_date, end_date)
            ) as cur:
                rows = await cur.fetchall()
                days = [{
                    "date": r["d"],
                    "delivered": int(r["delivered_count"] or 0),
                    "cancelled": int(r["cancelled_count"] or 0),
                    "food_revenue": float(r["food_revenue"] or 0.0),
                    "delivery_fees": float(r["delivery_fees"] or 0.0),
                    "total_payout": float(r["food_revenue"] or 0.0) + float(r["delivery_fees"] or 0.0),
                } for r in rows]

        return {
            "start_date": start_date,
            "end_date": end_date,
            "delivered": delivered,
            "cancelled": cancelled,
            "food_revenue": food_rev,
            "delivery_fees": delivery_fees,
            "total_payout": food_rev + delivery_fees,
            "days": days
        }


# Inside Database class

    async def get_vendor_daily_orders_page(self, vendor_id: int, date: Optional[str], page: int, page_size: int = 10) -> Dict[str, Any]:
        """
        Returns {orders, total, page, pages} for the given day.
        """
        date = date or datetime.date.today().strftime("%Y-%m-%d")
        total = await self.count_orders_for_vendor(vendor_id, date=date)
        pages = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, pages))
        offset = (page - 1) * page_size
        orders = await self.get_orders_for_vendor(vendor_id, date=date, limit=page_size, offset=offset)
        return {"orders": orders, "total": total, "page": page, "pages": pages}


    async def get_vendor_weekly_orders_page(self, vendor_id: int, start_date: Optional[str], end_date: Optional[str], page: int, page_size: int = 10) -> Dict[str, Any]:
        """
        Paginates across the week range.
        """
        # Count total across range
        async with aiosqlite.connect(self.db_path) as db:
            where = ["vendor_id = ?", "DATE(created_at) BETWEEN ? AND ?"]
            params = [vendor_id, start_date, end_date]
            async with db.execute(f"SELECT COUNT(*) FROM orders WHERE {' AND '.join(where)}", params) as cur:
                row = await cur.fetchone()
                total = int(row[0])

        pages = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, pages))
        offset = (page - 1) * page_size
        orders = await self.get_orders_for_vendor(vendor_id, status_filter=None, limit=page_size, offset=offset)
        return {"orders": orders, "total": total, "page": page, "pages": pages, "start_date": start_date, "end_date": end_date}
    
    # Inside Database class

    async def calc_vendor_reliability_for_day(self, vendor_id: int, date: Optional[str] = None) -> float:
        """
        Returns reliability percentage for a given day:
        progressed (accepted/in_progress/delivered) vs cancelled.
        """
        date = date or datetime.date.today().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                SUM(CASE WHEN status IN ('preparing','ready','in_progress','delivered') THEN 1 ELSE 0 END) AS progressed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                FROM orders
                WHERE vendor_id = ? AND DATE(created_at) = ?
                """,
                (vendor_id, date)
            ) as cur:
                r = await cur.fetchone()
                progressed = int((r[0] or 0))
                cancelled = int((r[1] or 0))
        denom = progressed + cancelled
        return 0.0 if denom == 0 else round(100.0 * progressed / denom, 2)



    async def get_user_by_id(self, internal_user_id: int) -> Optional[Dict[str, Any]]:
        """
        Return the users row by internal DB id.
        (Note: The original function name 'get_user_by_id' was misleadingly using telegram_id. 
        This is now corrected to use the internal DB ID, which is the primary key.)
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM users WHERE id = ? LIMIT 1",
                (internal_user_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
            
    async def get_student_chat_id(self, order: Dict[str, Any]) -> Optional[int]:
        """
        Resolve the student's Telegram chat_id from an order record.
        - order["user_id"] is the internal DB id of the user.
        - This method fetches the user row and returns user["telegram_id"].
        """
        user_id = order.get("user_id")
        if not user_id:
            return None

        user = await self.get_user_by_id(user_id)
        return user["telegram_id"] if user else None


    # -------------------- Delivery Guys --------------------
    async def get_delivery_guy(self, delivery_guy_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM delivery_guys WHERE id = ?", (delivery_guy_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
    
   


    async def create_delivery_guy(self, user_id: int, name: str, campus: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO delivery_guys
                (user_id, name, campus, active, total_deliveries)
                VALUES (?, ?, ?, 1, 0)
                """,
                (user_id, name, campus),
            )
            await db.commit()
            async with db.execute("SELECT id FROM delivery_guys WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0]
                
    # --- Delivery guy lookups ---
    async def get_delivery_guy_by_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Return the delivery_guy row for a given Telegram user_id.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM delivery_guys WHERE user_id = ? LIMIT 1",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def update_delivery_guy_coords(self, dg_id: int, lat: float, lon: float):
        """
        Update the last known coordinates of a delivery guy.
        """
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET last_lat = ?, last_lon = ? WHERE id = ?",
                (lat, lon, dg_id)
            )
            await conn.commit()
    
    # --- New Status/Timestamp Methods ---
    async def set_delivery_guy_online(self, dg_id: int) -> None:
        """Sets active=1 and updates last_online_at."""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = 1, last_online_at = ? WHERE id = ?",
                (now, dg_id)
            )
            await conn.commit()

    async def set_delivery_guy_offline(self, dg_id: int) -> None:
        """Sets active=0 and updates last_offline_at."""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = 0, last_offline_at = ? WHERE id = ?",
                (now, dg_id)
            )
            await conn.commit()

    async def block_delivery_guy(self, dg_id: int) -> None:
        """Sets active=0 and blocked=1."""
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = 0, blocked = 1 WHERE id = ?",
                (dg_id,)
            )
            await conn.commit()

    # -------------------- Orders --------------------
    async def create_order(
        self,
        user_id: int,
        vendor_id: int,
        pickup: str,
        dropoff: str,
        items_json: str,
        food_subtotal: float,
        delivery_fee: float,
        status: str,
        payment_method: str,
        payment_status: str,
        receipt_id: Optional[int],
        breakdown_json: str,
        delivery_guy_id: Optional[int] = None, # Made optional
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO orders
                (user_id, delivery_guy_id, vendor_id, pickup, dropoff, items_json,
                food_subtotal, delivery_fee, status, payment_method, payment_status,
                receipt_id, breakdown_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id, delivery_guy_id, vendor_id, pickup, dropoff, items_json,
                    food_subtotal, delivery_fee, status, payment_method, payment_status,
                    receipt_id, breakdown_json
                ),
            )
            await db.commit()
            async with db.execute("SELECT last_insert_rowid()") as cur:
                row = await cur.fetchone()
                return row[0]
            
    async def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def update_order_status(self, order_id: int, status: str, dg_id: Optional[int] = None) -> None:
        """Updates the order status and handles time-based fields."""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        sql_parts = ["status = ?", "updated_at = ?"]
        params = [status, now]

        if dg_id:
             sql_parts.append("delivery_guy_id = ?")
             params.append(dg_id)
        
        if status in ('accepted', 'preparing', 'ready'):
            sql_parts.append("accepted_at = ?")
            params.append(now)
        elif status == 'delivered':
            sql_parts.append("delivered_at = ?")
            params.append(now)

        params.append(order_id)
        
        sql = f"UPDATE orders SET {', '.join(sql_parts)} WHERE id = ?"

        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(sql, tuple(params))
            await conn.commit()
            
    # -------------------- Daily Stats & Gamification --------------------

    async def record_daily_stat_assignment(self, dg_id: int, date_str: str) -> None:
        """Increments assigned count for a DG's daily stat record."""
        async with aiosqlite.connect(self.db_path) as db:
            # Upsert (Insert or Update) logic
            await db.execute(
                """
                INSERT INTO daily_stats (dg_id, date, assigned)
                VALUES (?, ?, 1)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                assigned = assigned + 1
                """,
                (dg_id, date_str),
            )
            await db.commit()

    async def record_daily_stat_delivery(self, dg_id: int, date_str: str, earnings: float, total_xp: int = 10, total_coins: float = 0.0) -> None:
        """Updates daily_stats and dg gamification stats upon a successful delivery."""
        
        # 1. Update daily_stats table
        async with aiosqlite.connect(self.db_path) as db:
            # Update daily_stats (deliveries, earnings)
            await db.execute(
                """
                INSERT INTO daily_stats (dg_id, date, deliveries, earnings)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                deliveries = deliveries + 1,
                earnings = earnings + ?
                """,
                (dg_id, date_str, earnings, earnings),
            )
            
            # 2. Update delivery_guys gamification stats and total deliveries
            await db.execute(
                """
                UPDATE delivery_guys SET
                total_deliveries = total_deliveries + 1,
                xp = xp + ?,
                coins = coins + ?
                WHERE id = ?
                """,
                (total_xp, total_coins, dg_id)
            )

            await db.commit()
    
    async def increment_skip(self, dg_id: int) -> None:
        """Increments skipped_requests and updates last_skip_at for a DG."""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        
        async with aiosqlite.connect(self.db_path) as db:
            # 1. Update delivery_guys table
            await db.execute(
                """
                UPDATE delivery_guys SET
                skipped_requests = skipped_requests + 1,
                last_skip_at = ?
                WHERE id = ?
                """,
                (now, dg_id)
            )
            
            # 2. Update daily_stats table
            # NOTE: We assume the order was assigned first, so a record already exists, 
            # but we use UPSERT to be safe.
            await db.execute(
                """
                INSERT INTO daily_stats (dg_id, date, skipped)
                VALUES (?, ?, 1)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                skipped = skipped + 1
                """,
                (dg_id, today_str),
            )
            
            await db.commit()

    async def reset_daily_skip_count(self, dg_id: int) -> None:
        """Resets the DG's `skipped_requests` counter."""
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE delivery_guys SET skipped_requests = 0 WHERE id = ?",
                (dg_id,)
            )
            await conn.commit()

    async def get_daily_stats(self, dg_id: int, date_str: str) -> Optional[Dict[str, Any]]:
        """Retrieves daily stats for a DG on a specific date."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM daily_stats WHERE dg_id = ? AND date = ?",
                (dg_id, date_str)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
            
    async def get_stats_for_period(self, dg_id: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Retrieves stats for a delivery guy over a period."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM daily_stats WHERE dg_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC",
                (dg_id, start_date, end_date)
            ) as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

    # --- Vendor, Location, & Other Methods (Omitted for brevity, kept mostly as is) ---
    # ... (Keep all other existing methods like get_latest_active_order_for_dg, update_order_live, list_vendors, etc., below this line)

    # database/db.py (inside Database class)

    async def list_vendors(self) -> List[Dict[str, Any]]:
        """Return all vendors ordered by name."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM vendors ORDER BY name ASC") as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

    async def get_vendor(self, vendor_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a vendor by internal id."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM vendors WHERE id = ?", (vendor_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def get_vendor_by_telegram(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a vendor by their Telegram account id."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM vendors WHERE telegram_id = ?", (telegram_id,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def create_vendor(self, telegram_id: int, name: str, menu: Optional[List[Dict[str, Any]]] = None) -> int:
        """Insert a new vendor and return its id."""
        async with aiosqlite.connect(self.db_path) as db:
            menu_json = json.dumps(menu or [])
            cur = await db.execute(
                "INSERT INTO vendors (telegram_id, name, menu_json) VALUES (?, ?, ?)",
                (telegram_id, name, menu_json),
            )
            await db.commit()
            return cur.lastrowid

    async def update_vendor_menu(self, vendor_id: int, menu: List[Dict[str, Any]]) -> None:
        """Update a vendor's menu JSON."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE vendors SET menu_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (json.dumps(menu), vendor_id),
            )
            await db.commit()

    async def set_vendor_status(self, vendor_id: int, status: str) -> None:
        """Activate/deactivate a vendor."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE vendors SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, vendor_id),
            )
            await db.commit()

            
    async def get_order_for_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Returns the latest non-delivered order for a user.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM orders WHERE user_id = ? AND status NOT IN ('delivered', 'cancelled') ORDER BY created_at DESC LIMIT 1",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None
            
    async def get_latest_active_order_for_dg(self, delivery_guy_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM orders WHERE delivery_guy_id = ? AND status NOT IN ('delivered', 'cancelled') ORDER BY created_at DESC LIMIT 1",
                (delivery_guy_id,)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def create_location_log(
    self,
    order_id: Optional[int],
    delivery_guy_id: Optional[int],
    lat: float,
    lon: float
) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO location_logs (order_id, delivery_guy_id, lat, lon)
                VALUES (?, ?, ?, ?)
                """,
                (order_id, delivery_guy_id, lat, lon),
            )
            await db.commit()
            async with db.execute("SELECT last_insert_rowid()") as cur:
                row = await cur.fetchone()
                return row[0]

    async def update_order_live(self, order_id: int, last_lat: float, last_lon: float):
        """
        Update the live location of the delivery guy for a specific order.
        """
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE orders SET last_lat = ?, last_lon = ?, status = 'in_progress' WHERE id = ?",
                (last_lat, last_lon, order_id)
            )
            await conn.commit()
            
    async def set_order_timestamp(self, order_id: int, field: str):
        assert field in ("accepted_at", "delivered_at")
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(f"UPDATE orders SET {field} = ? WHERE id = ?", (now, order_id))
            await conn.commit()

            
    # The original log_delivery_location seems redundant with create_location_log and update_order_live
    # Sticking to the final plan, this function is removed.

    # -------------------- Seed Functions --------------------
    # (The seed functions are kept outside the class for simple execution, as in the original code)


from config import settings
import random
settings

async def seed_vendors(db_path: str) -> None:
    vendors = [
        {
            "telegram_id": settings.VENDOR_IDS["Tg house"],
            "name": "Tg house (Test Vendor)",
            "menu": [
                {"id": 1, "name": "🍝 Pasta Bolognese", "price": 90},
                {"id": 2, "name": "🥚 Enkulal Firfir", "price": 100},
                {"id": 3, "name": "🥘 Shiro Tegabino", "price": 120},
                {"id": 4, "name": "🥗 Mixed Salad", "price": 80},
                {"id": 5, "name": "☕ Macchiato", "price": 50},
                {"id": 6, "name": "🍵 Tea", "price": 20},
                {"id": 7, "name": "🥪 Club Sandwich", "price": 110},
                {"id": 8, "name": "🍕 Mini Pizza", "price": 150},
            ],
        },
        {
            "telegram_id": settings.VENDOR_IDS["Abudabi"],
            "name": "Abudabi",
            "menu": [
                {"id": 1, "name": "🥙 Chicken Shawarma", "price": 110},
                {"id": 2, "name": "🥙 Falafel Wrap", "price": 85},
                {"id": 3, "name": "🍟 French Fries", "price": 40},
                {"id": 4, "name": "🥤 Orange Juice", "price": 50},
                {"id": 5, "name": "🥤 Mango Juice", "price": 55},
                {"id": 6, "name": "🥤 Coke", "price": 30},
                {"id": 7, "name": "🍲 Lentil Soup", "price": 70},
            ],
        },
        {
            "telegram_id": random.randint(100000000, 999999999),
            "name": "Selam Fast Food",
            "menu": [
                {"id": 1, "name": "🍔 Cheeseburger", "price": 95},
                {"id": 2, "name": "🌭 Hotdog", "price": 70},
                {"id": 3, "name": "🍗 Chicken Nuggets (6pc)", "price": 80},
                {"id": 4, "name": "🍕 Slice of Pizza", "price": 60},
                {"id": 5, "name": "🍦 Ice Cream", "price": 35},
                {"id": 6, "name": "🥤 Sprite", "price": 30},
                {"id": 7, "name": "🥤 Fanta", "price": 30},
            ],
        },
    ]

    async with aiosqlite.connect(db_path) as db:
        for v in vendors:
            menu_json = json.dumps(v["menu"])
            await db.execute(
                """
                INSERT OR IGNORE INTO vendors (telegram_id, name, menu_json)
                VALUES (?, ?, ?)
                """,
                (v["telegram_id"], v["name"], menu_json),
            )
        await db.commit()

    print("✅ Vendors table seeded successfully with expanded menus")


async def seed_delivery_guys(db_path: str):
    delivery_guys_data = [
        (settings.DG_IDS["Dagmawi"], "Dagmawi", "6kilo", 1, 12, 14, 16, 10, 260, 3),
        (settings.DG_IDS["Muktar"], "Muktar", "5kilo", 1, 8, 10, 12, 80, 150, 2),
        (settings.DG_IDS["Yonatan"], "Yonatan", "6kilo", 0, 5, 6, 8, 50, 100, 1),
        # Randoms
        (random.randint(100000000, 999999999), "Selam", "6kilo", 0, 15, 18, 20, 150, 300, 4),
        (random.randint(100000000, 999999999), "Kebede", "6kilo", 1, 20, 22, 24, 200, 400, 5),
    ]

    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("DELETE FROM delivery_guys")
        await conn.commit()
        for row in delivery_guys_data:
            await conn.execute(
                """
                INSERT INTO delivery_guys 
                (user_id, name, campus, active, total_deliveries, accepted_requests, total_requests, xp, coins, level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        await conn.commit()
    print("✅ Delivery guys table reset and seeded with 5 entries")