# database/db.py (Postgres/asyncpg migration)
import json
import os
import random
import asyncpg
from config import settings
from typing import Optional, Dict, Any, List, Tuple
import datetime
from asyncpg.connection import Connection
from asyncpg.pool import Pool

# --- 1. UNIFIED SCHEMA SQL (Postgres Dialect) ---
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE,
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
    id SERIAL PRIMARY KEY,
    user_id BIGINT UNIQUE, -- Foreign key to users.id
    telegram_id BIGINT UNIQUE, -- Telegram chat_id for messaging
    name TEXT,
    campus TEXT,
    phone TEXT, 
    active BOOLEAN DEFAULT FALSE, -- Changed INTEGER DEFAULT 0 to BOOLEAN DEFAULT FALSE
    blocked BOOLEAN DEFAULT FALSE, -- Changed INTEGER DEFAULT 0 to BOOLEAN DEFAULT FALSE
    total_deliveries INTEGER DEFAULT 0,
    accepted_requests INTEGER DEFAULT 0,
    total_requests INTEGER DEFAULT 0,
    last_lat DOUBLE PRECISION NULL, -- Changed REAL to DOUBLE PRECISION
    last_lon DOUBLE PRECISION NULL, -- Changed REAL to DOUBLE PRECISION
    skipped_requests INTEGER DEFAULT 0,
    last_skip_at TIMESTAMP NULL,
    last_online_at TIMESTAMP NULL,
    last_offline_at TIMESTAMP NULL,
    coins INTEGER DEFAULT 0,
    xp INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS vendors (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE, 
    name TEXT NOT NULL, 
    menu_json TEXT, 
    status TEXT DEFAULT 'active',
    rating_avg DOUBLE PRECISION DEFAULT 0.0, -- Changed REAL to DOUBLE PRECISION
    rating_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    delivery_guy_id INTEGER NULL, 
    vendor_id INTEGER,
    pickup TEXT,
    dropoff TEXT,
    items_json TEXT,
    food_subtotal DOUBLE PRECISION DEFAULT 0.0, -- Changed REAL to DOUBLE PRECISION
    delivery_fee DOUBLE PRECISION DEFAULT 0.0, -- Changed REAL to DOUBLE PRECISION
    status TEXT, -- pending / assigned / preparing / ready / in_progress / delivered / cancelled
    payment_method TEXT,
    payment_status TEXT,
    receipt_id INTEGER,
    breakdown_json TEXT,
    live_shared BOOLEAN DEFAULT FALSE, -- Changed INTEGER DEFAULT 0 to BOOLEAN DEFAULT FALSE
    live_expires TIMESTAMP NULL, 
    last_lat DOUBLE PRECISION NULL, -- Changed REAL to DOUBLE PRECISION
    last_lon DOUBLE PRECISION NULL, -- Changed REAL to DOUBLE PRECISION
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    accepted_at TIMESTAMP NULL,
    delivered_at TIMESTAMP NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL PRIMARY KEY,
    dg_id INTEGER,
    date TEXT, -- YYYY-MM-DD
    deliveries INTEGER DEFAULT 0,
    earnings DOUBLE PRECISION DEFAULT 0.0, -- Changed REAL to DOUBLE PRECISION
    skipped INTEGER DEFAULT 0,
    assigned INTEGER DEFAULT 0, 
    acceptance_rate DOUBLE PRECISION DEFAULT 0.0, -- Changed REAL to DOUBLE PRECISION
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dg_id, date)
);


CREATE TABLE IF NOT EXISTS ratings (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    delivery_guy_id INTEGER,
    vendor_id INTEGER,
    stars INTEGER NOT NULL,
    comment TEXT,
    type TEXT NOT NULL CHECK (type IN ('delivery','vendor')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_order_type UNIQUE (order_id, type)
);



CREATE TABLE IF NOT EXISTS media (
    id SERIAL PRIMARY KEY,
    file_path TEXT,
    uploader_user_id INTEGER,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified_by INTEGER,
    verify_status TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    type TEXT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    status TEXT
);

CREATE TABLE IF NOT EXISTS location_logs (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NULL, 
    delivery_guy_id INTEGER NULL, 
    lat DOUBLE PRECISION, -- Changed REAL to DOUBLE PRECISION
    lon DOUBLE PRECISION, -- Changed REAL to DOUBLE PRECISION
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS admin_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT
);

CREATE TABLE IF NOT EXISTS jobs_log (
    id SERIAL PRIMARY KEY,
    job_name TEXT,
    key TEXT,
    status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes (Postgres doesn't need IF NOT EXISTS for CREATE INDEX in a transaction block
-- but this is fine for initial setup)
CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user_end ON subscriptions(user_id, end_ts);
CREATE INDEX IF NOT EXISTS idx_location_order_ts ON location_logs(order_id, ts);
CREATE INDEX IF NOT EXISTS idx_daily_stats_dg_date ON daily_stats(dg_id, date);
CREATE INDEX IF NOT EXISTS idx_vendors_telegram_id ON vendors(telegram_id);
CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name);
ALTER TABLE orders ALTER COLUMN user_id TYPE BIGINT;
ALTER TABLE orders ALTER COLUMN delivery_guy_id TYPE BIGINT;
ALTER TABLE orders ALTER COLUMN vendor_id TYPE BIGINT;

-- Make sure dg_id is BIGINT
ALTER TABLE daily_stats ALTER COLUMN dg_id TYPE BIGINT;


ALTER TABLE orders
ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP NULL,
ADD COLUMN IF NOT EXISTS vendor_confirmed_at TIMESTAMP NULL,
ADD COLUMN IF NOT EXISTS cancel_reason TEXT NULL;

-- Optional: index to speed up expiry checks
CREATE INDEX IF NOT EXISTS idx_orders_expires ON orders(expires_at);


"""

class Database:
    def __init__(self):
        self.database_url = os.environ.get("DB_PATH")  # use DATABASE_URL not DB_PATH
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is not set.")
        self._pool: Optional[Pool] = None

    async def init_pool(self):
        """Initialize the asyncpg pool once at startup."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.database_url)

    def _get_pool(self) -> Pool:
        """Return the pool synchronously (must be initialized first)."""
        if not self._pool:
            raise RuntimeError("Pool not initialized. Call init_pool() first.")
        return self._pool
    
    async def close_pool(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
    
    async def init_schema(self):
        """Run SCHEMA_SQL to create tables if they don't exist."""
        async with self._open_connection() as conn:
            await conn.execute(SCHEMA_SQL)

    def _open_connection(self):
        """Return an async context manager for acquiring a connection."""
        return self._get_pool().acquire()

    @staticmethod
    def _row_to_dict(record: asyncpg.Record) -> Dict[str, Any]:
        return dict(record)
    
    async def execute(self, query: str, *args):
        """Generic execute for INSERT/UPDATE/DELETE."""
        async with self._open_connection() as conn:
            return await conn.execute(query, *args)

    async def update_user_field(self, telegram_id: int, field: str, value: str):
        """Update a single field in users table."""
        async with self._open_connection() as conn:
            return await conn.execute(
                f"UPDATE users SET {field} = $1 WHERE telegram_id = $2",
                value, telegram_id
            )
  


    # -------------------- Users --------------------
    async def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE telegram_id = $1", telegram_id
            )
            return self._row_to_dict(row) if row else None

    async def create_user(
        self,
        telegram_id: int,
        role: str,
        first_name: str,
        phone: str,
        campus: str,
    ) -> int:
        async with self._open_connection() as conn:
            # Use RETURNING id to get the new primary key immediately
            result = await conn.fetchval(
                """
                INSERT INTO users
                (telegram_id, role, first_name, phone, campus, status)
                VALUES ($1, $2, $3, $4, $5, 'active')
                ON CONFLICT (telegram_id) DO NOTHING
                RETURNING id
                """,
                telegram_id, role, first_name, phone, campus,
            )
            # If nothing was inserted (due to ON CONFLICT), fetch existing ID
            if result is None:
                result = await conn.fetchval(
                    "SELECT id FROM users WHERE telegram_id = $1", telegram_id
                )
            
            # The result is already the integer ID or None if the user was somehow deleted
            return int(result) if result is not None else 0 # Return 0 or raise if insertion/lookup fails

    async def get_orders_for_vendor(self, vendor_id: int, *, date: Optional[str] = None, status_filter: Optional[List[str]] = None, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List orders for a vendor, optionally filtered by date (YYYY-MM-DD),
        status list, and paginated (limit/offset).
        """
        async with self._open_connection() as conn:
            where = ["vendor_id = $1"]
            params: List[Any] = [vendor_id]
            param_counter = 2

            if date:
                where.append(f"created_at::DATE = ${param_counter}") # Postgres specific date extraction
                params.append(date)
                param_counter += 1

            if status_filter:
                # asyncpg handles IN clauses with lists automatically for one parameter
                where.append(f"status = ANY(${(param_counter)})")
                params.append(status_filter)
                param_counter += 1

            sql = f"SELECT * FROM orders WHERE {' AND '.join(where)} ORDER BY created_at DESC"
            if limit is not None:
                sql += f" LIMIT ${param_counter} OFFSET ${param_counter + 1}"
                params.extend([limit, offset])

            rows = await conn.fetch(sql, *params)
            return [self._row_to_dict(r) for r in rows]

    async def reset_schema(self):
        async with self._open_connection() as conn:
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            
    async def get_internal_user_id(self, telegram_id: int) -> Optional[int]:
        row = await self._pool.fetchrow(
            "SELECT id FROM users WHERE telegram_id = $1",
            telegram_id
        )
        return row["id"] if row else None

    async def count_orders_for_vendor(self, vendor_id: int, *, date: Optional[str] = None, status_filter: Optional[List[str]] = None) -> int:
        """Count orders for pagination and summaries."""
        async with self._open_connection() as conn:
            where = ["vendor_id = $1"]
            params: List[Any] = [vendor_id]
            param_counter = 2

            if date:
                where.append(f"created_at::DATE = ${param_counter}")
                params.append(date)
                param_counter += 1

            if status_filter:
                where.append(f"status = ANY(${(param_counter)})")
                params.append(status_filter)
                param_counter += 1

            sql = f"SELECT COUNT(*) FROM orders WHERE {' AND '.join(where)}"
            count = await conn.fetchval(sql, *params)
            return int(count)

    async def summarize_vendor_day(self, vendor_id: int, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns daily summary: counts, food revenue, delivery fees, ratings snapshot, reliability.
        date format: YYYY-MM-DD (defaults to today if None).
        """
        date = date or datetime.date.today().strftime("%Y-%m-%d")
        async with self._open_connection() as conn:

            # Orders summary (delivered vs cancelled vs prepared)
            s = await conn.fetchrow(
                """
                SELECT
                SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                COALESCE(SUM(food_subtotal), 0.0) AS food_revenue,
                COALESCE(SUM(delivery_fee), 0.0) AS delivery_fees
                FROM orders
                WHERE vendor_id = $1 AND created_at::DATE = $2
                """,
                vendor_id, date
            )
            delivered = int(s["delivered_count"] if s else 0)
            cancelled = int(s["cancelled_count"] if s else 0)
            food_rev = float(s["food_revenue"] if s else 0.0)
            delivery_fees = float(s["delivery_fees"] if s else 0.0)

            # Ratings snapshot (from cached vendor fields)
            v = await conn.fetchrow("SELECT rating_avg, rating_count FROM vendors WHERE id = $1", vendor_id)
            rating_avg = float(v["rating_avg"] if v else 0.0)
            rating_count = int(v["rating_count"] if v else 0)

            # Reliability calculation
            r = await conn.fetchrow(
                """
                SELECT
                SUM(CASE WHEN status IN ('preparing','ready','in_progress','delivered') THEN 1 ELSE 0 END) AS progressed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                FROM orders
                WHERE vendor_id = $1 AND created_at::DATE = $2
                """,
                vendor_id, date
            )
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
            end = start + datetime.timedelta(days=6)  # Sunday
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")

        async with self._open_connection() as conn:

            # Totals
            t = await conn.fetchrow(
                """
                SELECT
                SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                COALESCE(SUM(food_subtotal), 0.0) AS food_revenue,
                COALESCE(SUM(delivery_fee), 0.0) AS delivery_fees
                FROM orders
                WHERE vendor_id = $1 AND created_at::DATE BETWEEN $2 AND $3
                """,
                vendor_id, start_date, end_date
            )
            delivered = int(t["delivered_count"] or 0)
            cancelled = int(t["cancelled_count"] or 0)
            food_rev = float(t["food_revenue"] or 0.0)
            delivery_fees = float(t["delivery_fee"] or 0.0)

            # Per-day breakdown
            rows = await conn.fetch(
                """
                SELECT created_at::DATE AS d,
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    COALESCE(SUM(food_subtotal), 0.0) AS food_revenue,
                    COALESCE(SUM(delivery_fee), 0.0) AS delivery_fees
                FROM orders
                WHERE vendor_id = $1 AND created_at::DATE BETWEEN $2 AND $3
                GROUP BY created_at::DATE
                ORDER BY d ASC
                """,
                vendor_id, start_date, end_date
            )
            days = [{
                "date": str(r["d"]), # Convert date object to string
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
        NOTE: This implementation does not filter by date in get_orders_for_vendor,
        but the count is for the range. Assuming you want all orders for the vendor.
        If you want to filter get_orders_for_vendor, you'd need to modify it to accept the range.
        For now, I'm fetching the count for the period and paginating all orders (as the original get_orders_for_vendor was called).
        """
        
        async with self._open_connection() as conn:
            where = ["vendor_id = $1", "created_at::DATE BETWEEN $2 AND $3"]
            params = [vendor_id, start_date, end_date]
            total = await conn.fetchval(f"SELECT COUNT(*) FROM orders WHERE {' AND '.join(where)}", *params)
            total = int(total) if total is not None else 0


        pages = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, pages))
        offset = (page - 1) * page_size
        
        # NOTE: The original code called get_orders_for_vendor without the date filter here,
        # which would fetch *all* vendor orders, paginated.
        # I'm retaining the original *behavior* but the count is for the range.
        # If you need orders *only* for the range, modify the call below:
        orders = await self.get_orders_for_vendor(vendor_id, limit=page_size, offset=offset)
        
        return {"orders": orders, "total": total, "page": page, "pages": pages, "start_date": start_date, "end_date": end_date}

    async def calc_vendor_reliability_for_day(self, vendor_id: int, date: Optional[str] = None) -> float:
        """
        Returns reliability percentage for a given day:
        progressed (accepted/in_progress/delivered) vs cancelled.
        """
        date = datetime.date.today()
        async with self._open_connection() as conn:
            r = await conn.fetchrow(
                """
                SELECT
                SUM(CASE WHEN status IN ('preparing','ready','in_progress','delivered') THEN 1 ELSE 0 END) AS progressed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                FROM orders
                WHERE vendor_id = $1 AND created_at::DATE = $2
                """,
                vendor_id, date
            )
            progressed = int(r["progressed"] or 0)
            cancelled = int(r["cancelled"] or 0)

        denom = progressed + cancelled
        return 0.0 if denom == 0 else round(100.0 * progressed / denom, 2)
    
    
    
    async def get_delivery_guy_by_user_id(self, user_id: int):
        """
        Fetch a delivery guy by foreign key user_id and return the row.
        Raises ValueError if not found.
        """
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT id, user_id, telegram_id, name FROM delivery_guys WHERE user_id=$1",
                user_id
            )
        if row is None:
            raise ValueError(f"Delivery guy with user_id={user_id} not found in DB")
        return dict(row)
    
    
    async def get_delivery_guy_by_id(self, dg_id: int):
        """
        Fetch a delivery guy by primary key id and return the row.
        Raises ValueError if not found.
        """
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT id, user_id, telegram_id, name FROM delivery_guys WHERE id=$1",
                dg_id
            )
        if row is None:
            raise ValueError(f"Delivery guy with id={dg_id} not found in DB")
        return dict(row)


    async def get_delivery_guy_telegram_id_by_id(self, dg_id: int) -> int:
        """
        Convenience helper: return just the telegram_id for a given delivery_guy_id.
        """
        guy = await self.get_delivery_guy_by_id(dg_id)
        return guy["telegram_id"]

    async def get_delivery_guy_telegram_id(self, user_id: int) -> int:
        """
        Convenience helper: return just the telegram_id for a given user_id.
        """
        guy = await self.get_delivery_guy_by_user_id(user_id)
        return guy["telegram_id"]

    async def get_user_by_id(self, internal_user_id: int) -> Optional[Dict[str, Any]]:
        """
        Return the users row by internal DB id.
        """
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE id = $1 LIMIT 1",
                internal_user_id
            )
            return self._row_to_dict(row) if row else None
            
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
        return int(user["telegram_id"]) if user and user.get("telegram_id") is not None else None


    # -------------------- Delivery Guys --------------------
    async def get_delivery_guy(self, delivery_guy_id: int) -> Optional[Dict[str, Any]]:
        async with self._open_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM delivery_guys WHERE id = $1", delivery_guy_id)
            return self._row_to_dict(row) if row else None
    

    async def create_delivery_guy(self, user_id: int, name: str, campus: str) -> int:
        async with self._open_connection() as conn:
            dg_id = await conn.fetchval(
                """
                INSERT INTO delivery_guys
                (user_id, name, campus, active, total_deliveries)
                VALUES ($1, $2, $3, TRUE, 0)
                RETURNING id
                """,
                user_id, name, campus,
            )
            return int(dg_id) if dg_id is not None else 0
            
    # --- Delivery guy lookups ---
    async def get_delivery_guy_by_user(self, telegram_id: int):
        try:
            async with self._open_connection() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM delivery_guys WHERE telegram_id=$1", telegram_id
                )
        except asyncpg.InvalidCachedStatementError:
            await self.recycle_pool()
            async with self._open_connection() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM delivery_guys WHERE telegram_id=$1", telegram_id
                )


    async def get_daily_stats_for_dg(self, dg_id: int, date: str) -> Dict[str, Any]:
        """
        Returns stats for a delivery guy on a given date.
        Includes deliveries, earnings, xp, coins.
        """
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT 
                    COALESCE(SUM(delivery_fee), 0) AS earnings,
                    COUNT(*) AS deliveries
                FROM orders
                WHERE delivery_guy_id = $1 AND DATE(updated_at) = $2
                """,
                dg_id, date
            )
        if not row:
            return {"earnings": 0, "deliveries": 0}
        
        # XP and coins can be derived here
        xp = row["deliveries"] * 10
        coins = row["earnings"] * 0.05
        return {
            "earnings": row["earnings"],
            "deliveries": row["deliveries"],
            "xp": xp,
            "coins": coins
        }
    async def get_weekly_earnings_for_dg(self, dg_id: int, week_start: str, week_end: str) -> List[Dict[str, Any]]:
        """
        Returns day-by-day breakdown for the week.
        Each entry: {date, deliveries, earnings, xp, coins}
        """
        async with self._open_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    DATE(updated_at) AS day,
                    COALESCE(SUM(delivery_fee), 0) AS earnings,
                    COUNT(*) AS deliveries
                FROM orders
                WHERE delivery_guy_id = $1 AND DATE(updated_at) BETWEEN $2 AND $3
                GROUP BY day
                ORDER BY day ASC
                """,
                dg_id, week_start, week_end
            )
        
        stats = []
        for r in rows:
            xp = r["deliveries"] * 10
            coins = r["earnings"] * 0.05
            stats.append({
                "date": r["day"],
                "deliveries": r["deliveries"],
                "earnings": r["earnings"],
                "xp": xp,
                "coins": coins
            })
        return stats
    async def get_weekly_totals_for_dg(self, dg_id: int, week_start: str, week_end: str) -> Dict[str, Any]:
        """
        Returns total deliveries, earnings, xp, coins for the week.
        """
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT 
                    COALESCE(SUM(delivery_fee), 0) AS earnings,
                    COUNT(*) AS deliveries
                FROM orders
                WHERE delivery_guy_id = $1 AND DATE(updated_at) BETWEEN $2 AND $3
                """,
                dg_id, week_start, week_end
            )
        if not row:
            return {"earnings": 0, "deliveries": 0, "xp": 0, "coins": 0}
        
        xp = row["deliveries"] * 10
        coins = row["earnings"] * 0.05
        return {
            "earnings": row["earnings"],
            "deliveries": row["deliveries"],
            "xp": xp,
            "coins": coins
        }

    async def update_delivery_guy_coords(self, dg_id: int, lat: float, lon: float):
        """
        Update the last known coordinates of a delivery guy.
        """
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE delivery_guys SET last_lat = $1, last_lon = $2 WHERE id = $3",
                lat, lon, dg_id
            )
    
    # --- New Status/Timestamp Methods ---
    async def set_delivery_guy_online(self, dg_id: int) -> None:
        """Sets active=1 and updates last_online_at."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = TRUE, last_online_at = CURRENT_TIMESTAMP WHERE id = $1",
                dg_id
            )

    async def set_delivery_guy_offline(self, dg_id: int) -> None:
        """Sets active=0 and updates last_offline_at."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = FALSE, last_offline_at = CURRENT_TIMESTAMP WHERE id = $1",
                dg_id
            )

    async def block_delivery_guy(self, dg_id: int) -> None:
        """Sets active=FALSE and blocked=TRUE."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE delivery_guys SET active = FALSE, blocked = TRUE WHERE id = $1",
                dg_id
            )

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
        delivery_guy_id: Optional[int] = None,
    ) -> int:
        from datetime import datetime, timedelta, timezone
        now = datetime.utcnow()  # naive UTC
        expires_at = now + timedelta(minutes=30)
        async with self._open_connection() as conn:
            order_id = await conn.fetchval(
                """
                INSERT INTO orders
                (user_id, delivery_guy_id, vendor_id, pickup, dropoff, items_json,
                food_subtotal, delivery_fee, status, payment_method, payment_status,
                receipt_id, breakdown_json, created_at, updated_at, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $15)
                RETURNING id
                """,
                user_id, delivery_guy_id, vendor_id, pickup, dropoff, items_json,
                food_subtotal, delivery_fee, status, payment_method, payment_status,
                receipt_id, breakdown_json, now, expires_at,
            )
            return int(order_id) if order_id is not None else 0

            
    async def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        async with self._open_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
            return self._row_to_dict(row) if row else None

    async def update_order_status(self, order_id: int, status: str, dg_id: Optional[int] = None) -> None:
        """Updates the order status and handles time-based fields."""
        
        sql_parts = ["status = $1", "updated_at = CURRENT_TIMESTAMP"]
        params = [status]
        param_counter = 2

        if dg_id:
            sql_parts.append(f"delivery_guy_id = ${param_counter}")
            params.append(dg_id)
            param_counter += 1
        
        if status in ('accepted', 'preparing', 'ready'):
            sql_parts.append("accepted_at = CURRENT_TIMESTAMP")
        elif status == 'delivered':
            sql_parts.append("delivered_at = CURRENT_TIMESTAMP")

        sql = f"UPDATE orders SET {', '.join(sql_parts)} WHERE id = ${param_counter}"
        params.append(order_id)
        
        async with self._open_connection() as conn:
            await conn.execute(sql, *params)
            
    # -------------------- Daily Stats & Gamification --------------------

    async def record_daily_stat_assignment(self, dg_id: int, date_str: str) -> None:
        """Increments assigned count for a DG's daily stat record."""
        async with self._open_connection() as conn:
            # Upsert (INSERT ... ON CONFLICT DO UPDATE) logic
            await conn.execute(
                """
                INSERT INTO daily_stats (dg_id, date, assigned)
                VALUES ($1, $2, 1)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                assigned = daily_stats.assigned + 1,
                updated_at = CURRENT_TIMESTAMP
                """,
                dg_id, date_str,
            )

    async def record_daily_stat_delivery(self, dg_id: int, date_str: str, earnings: float, total_xp: int = 10, total_coins: float = 0.0) -> None:
        """Updates daily_stats and dg gamification stats upon a successful delivery."""
        
        async with self._open_connection() as conn:
            # 1. Update daily_stats table
            await conn.execute(
                """
                INSERT INTO daily_stats (dg_id, date, deliveries, earnings)
                VALUES ($1, $2, 1, $3)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                deliveries = daily_stats.deliveries + 1,
                earnings = daily_stats.earnings + $3,
                updated_at = CURRENT_TIMESTAMP
                """,
                dg_id, date_str, earnings
            )
            
            # 2. Update delivery_guys gamification stats and total deliveries
            await conn.execute(
                """
                UPDATE delivery_guys SET
                total_deliveries = total_deliveries + 1,
                xp = xp + $1,
                coins = coins + $2
                WHERE id = $3
                """,
                total_xp, total_coins, dg_id
            )
    
    async def increment_skip(self, dg_id: int) -> None:
        """Increments skipped_requests and updates last_skip_at for a DG."""
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        
        async with self._open_connection() as conn:
            # 1. Update delivery_guys table
            await conn.execute(
                """
                UPDATE delivery_guys SET
                skipped_requests = skipped_requests + 1,
                last_skip_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                dg_id
            )
            
            # 2. Update daily_stats table (UPSERT)
            await conn.execute(
                """
                INSERT INTO daily_stats (dg_id, date, skipped)
                VALUES ($1, $2, 1)
                ON CONFLICT(dg_id, date) DO UPDATE SET
                skipped = daily_stats.skipped + 1,
                updated_at = CURRENT_TIMESTAMP
                """,
                dg_id, today_str
            )

    async def reset_daily_skip_count(self, dg_id: int) -> None:
        """Resets the DG's `skipped_requests` counter."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE delivery_guys SET skipped_requests = 0 WHERE id = $1",
                dg_id
            )

    async def get_daily_stats(self, dg_id: int, date_str: str) -> Optional[Dict[str, Any]]:
        """Retrieves daily stats for a DG on a specific date."""
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM daily_stats WHERE dg_id = $1 AND date = $2",
                dg_id, date_str
            )
            return self._row_to_dict(row) if row else None
            
    async def get_stats_for_period(self, dg_id: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Retrieves stats for a delivery guy over a period."""
        async with self._open_connection() as conn:
            rows = await conn.fetch(
                "SELECT * FROM daily_stats WHERE dg_id = $1 AND date BETWEEN $2 AND $3 ORDER BY date DESC",
                dg_id, start_date, end_date
            )
            return [self._row_to_dict(r) for r in rows]

    # --- Vendor, Location, & Other Methods (Adapted) ---

    async def list_vendors(self) -> List[Dict[str, Any]]:
        """Return all vendors ordered by name."""
        async with self._open_connection() as conn:
            rows = await conn.fetch("SELECT * FROM vendors ORDER BY name ASC")
            return [self._row_to_dict(r) for r in rows]

   
    


# -------------------- Vendors --------------------

    async def get_vendor(self, vendor_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a vendor by internal id."""
        async with self._open_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM vendors WHERE id = $1", vendor_id)
            return self._row_to_dict(row) if row else None

    async def get_vendor_by_telegram(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a vendor by their Telegram account id."""
        async with self._open_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM vendors WHERE telegram_id = $1", telegram_id)
            return self._row_to_dict(row) if row else None

    async def create_vendor(self, telegram_id: int, name: str, menu: Optional[List[Dict[str, Any]]] = None) -> int:
        """Insert a new vendor and return its id."""
        menu_json = json.dumps(menu or [])
        async with self._open_connection() as conn:
            vendor_id = await conn.fetchval(
                "INSERT INTO vendors (telegram_id, name, menu_json) VALUES ($1, $2, $3) RETURNING id",
                telegram_id, name, menu_json
            )
            return int(vendor_id) if vendor_id else 0

    async def update_vendor_menu(self, vendor_id: int, menu: List[Dict[str, Any]]) -> None:
        """Update a vendor's menu JSON."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE vendors SET menu_json = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                json.dumps(menu), vendor_id
            )

    async def set_vendor_status(self, vendor_id: int, status: str) -> None:
        """Activate/deactivate a vendor."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE vendors SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                status, vendor_id
            )

    # -------------------- Orders --------------------

    async def get_order_for_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Returns the latest non-delivered order for a user."""
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE user_id = $1 AND status NOT IN ('delivered','cancelled') ORDER BY created_at DESC LIMIT 1",
                user_id
            )
            return self._row_to_dict(row) if row else None

    async def get_latest_active_order_for_dg(self, delivery_guy_id: int) -> Optional[Dict[str, Any]]:
        async with self._open_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE delivery_guy_id = $1 AND status NOT IN ('delivered','cancelled') ORDER BY created_at DESC LIMIT 1",
                delivery_guy_id
            )
            return self._row_to_dict(row) if row else None

    # -------------------- Location Logs --------------------

    async def create_location_log(self, order_id: Optional[int], delivery_guy_id: Optional[int], lat: float, lon: float) -> int:
        async with self._open_connection() as conn:
            log_id = await conn.fetchval(
                "INSERT INTO location_logs (order_id, delivery_guy_id, lat, lon) VALUES ($1, $2, $3, $4) RETURNING id",
                order_id, delivery_guy_id, lat, lon
            )
            return int(log_id) if log_id else 0

    async def update_order_live(self, order_id: int, last_lat: float, last_lon: float):
        """Update the live location of the delivery guy for a specific order."""
        async with self._open_connection() as conn:
            await conn.execute(
                "UPDATE orders SET last_lat = $1, last_lon = $2, status = 'in_progress' WHERE id = $3",
                last_lat, last_lon, order_id
            )

    async def set_order_timestamp(self, order_id: int, field: str):
        assert field in ("accepted_at", "delivered_at")
        async with self._open_connection() as conn:
            await conn.execute(f"UPDATE orders SET {field} = CURRENT_TIMESTAMP WHERE id = $1", order_id)

# -------------------- Seed Functions --------------------
async def seed_vendors(db: Database) -> None:
    vendors = [
        {
            "telegram_id": settings.VENDOR_IDS["Tg house"],
            "name": "Tg house (Test Vendor)",
            "menu": [
                {"id": 1, "name": "🍝 Pasta Bolognese", "price": 90, "category": "Mains"},
                {"id": 2, "name": "🥚 Enkulal Firfir", "price": 100, "category": "Fasting"},
                {"id": 3, "name": "🥘 Shiro Tegabino", "price": 120, "category": "Fasting"},
                {"id": 4, "name": "🥗 Mixed Salad", "price": 80, "category": "Specials"},
                {"id": 5, "name": "☕ Macchiato", "price": 50, "category": "Drinks"},
                {"id": 6, "name": "🍵 Tea", "price": 20, "category": "Drinks"},
                {"id": 7, "name": "🥪 Club Sandwich", "price": 110, "category": "Non Fasting"},
                {"id": 8, "name": "🍕 Mini Pizza", "price": 150, "category": "Specials"},
            ],
        },
        {
            "telegram_id": settings.VENDOR_IDS["Abudabi"],
            "name": "Abudabi",
            "menu": [
                {"id": 1, "name": "🥙 Chicken Shawarma", "price": 110, "category": "Non Fasting"},
                {"id": 2, "name": "🥙 Falafel Wrap", "price": 85, "category": "Fasting"},
                {"id": 3, "name": "🍟 French Fries", "price": 40, "category": "Specials"},
                {"id": 4, "name": "🥤 Orange Juice", "price": 50, "category": "Drinks"},
                {"id": 5, "name": "🥤 Mango Juice", "price": 55, "category": "Drinks"},
                {"id": 6, "name": "🥤 Coke", "price": 30, "category": "Drinks"},
                {"id": 7, "name": "🍲 Lentil Soup", "price": 70, "category": "Fasting"},
            ],
        },
        {
            "telegram_id": random.randint(100000000, 999999999),
            "name": "Selam Fast Food",
            "menu": [
                {"id": 1, "name": "🍔 Cheeseburger", "price": 95, "category": "Mains"},
                {"id": 2, "name": "🌭 Hotdog", "price": 70, "category": "Mains"},
                {"id": 3, "name": "🍗 Chicken Nuggets (6pc)", "price": 80, "category": "Specials"},
                {"id": 4, "name": "🍕 Slice of Pizza", "price": 60, "category": "Mains"},
                {"id": 5, "name": "🍦 Ice Cream", "price": 35, "category": "Specials"},
                {"id": 6, "name": "🥤 Sprite", "price": 30, "category": "Drinks"},
                {"id": 7, "name": "🥤 Fanta", "price": 30, "category": "Drinks"},
            ],
        },
    ]
    
    

    async with db._open_connection() as conn:
    # Delete all existing vendors
        await conn.execute("TRUNCATE TABLE vendors RESTART IDENTITY CASCADE")

        # Now insert fresh seed data
        for v in vendors:
            menu_json = json.dumps(v["menu"])
            await conn.execute(
                """
                INSERT INTO vendors (telegram_id, name, menu_json)
                VALUES ($1, $2, $3)
                """,
                v["telegram_id"], v["name"], menu_json
            )
    print("✅ Vendors table truncated and seeded successfully")



async def seed_delivery_guys(db: Database) -> None:
    """
    Reset and seed the delivery_guys table with sample entries.
    """

    # Each tuple: (user_id, telegram_id, name, campus, active,
    #              total_deliveries, accepted_requests, total_requests,
    #              coins, xp, level)
    delivery_guys_data: List[Tuple] = [
    (1001, settings.DG_IDS["Dagmawi"], "Dagmawi", "6kilo", True, 12, 14, 16, 10, 260, 3),
    (1002, settings.DG_IDS["Muktar"], "Muktar", "5kilo", True, 8, 10, 12, 80, 150, 2),
    (1003, settings.DG_IDS["Yonatan"], "Yonatan", "6kilo", True, 5, 6, 8, 50, 100, 1),
    # Random demo entries
    (random.randint(100000000, 999999999), random.randint(100000000, 999999999),
     "Selam", "6kilo", False, 15, 18, 20, 150, 300, 4),
    (random.randint(100000000, 999999999), random.randint(100000000, 999999999),
     "Kebede", "6kilo", False, 20, 22, 24, 200, 400, 5),
]



    async with db._open_connection() as conn:
        # Clear table first
        await conn.execute("TRUNCATE delivery_guys RESTART IDENTITY CASCADE")

        insert_sql = """
            INSERT INTO delivery_guys 
            (user_id, telegram_id, name, campus, active,
             total_deliveries, accepted_requests, total_requests,
             coins, xp, level)
            VALUES ($1::BIGINT, $2::BIGINT, $3, $4, $5,
                    $6, $7, $8, $9, $10, $11)
        """

        for row in delivery_guys_data:
            await conn.execute(insert_sql, *row)

    print("✅ delivery_guys table reset and seeded with entries")