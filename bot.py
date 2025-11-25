"""
Deliver AAU Bot - FastAPI Webhook entry point.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Update, BotCommand

from config import settings
from database.db import Database, seed_delivery_guys, seed_vendors
from utils.scheduler import BotScheduler

# Middlewares
from middlewares.logging_middleware import LoggingMiddleware
from middlewares.throttling_middleware import ThrottlingMiddleware
from middlewares.error_handling_middleware import ErrorHandlingMiddleware
from app_context import bot, dp, db
# Routers
from handlers.onboarding import router as onboarding_router
from handlers.student import router as student_router
from handlers.student_track_order import router as student_track_order_router
from handlers.delivery_guy import router as delivery_guy_router
from handlers.vendor import router as vendor_router
from handlers.rating import router as rating_router
from handlers.help import router as help_router
from handlers.settings import router as settings_router
from middlewares.gracefull_fallback_middleware import GracefulFallbackMiddleware

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Bot & Dispatcher ---

# Middlewares
# Throttling: register separately for messages and callbacks
dp.message.middleware(ThrottlingMiddleware(
    message_interval=1.5,   # silently drop spammy messages
    callback_interval=0.5   # polite popup for fast button clicks
))
dp.callback_query.middleware(ThrottlingMiddleware(
    message_interval=1.5,
    callback_interval=0.5
))

#Gracefull fall back middleware registeration


dp.message.middleware(GracefulFallbackMiddleware())
dp.callback_query.middleware(GracefulFallbackMiddleware())
# Routers

# Error handling: also register per-event
dp.message.middleware(ErrorHandlingMiddleware())
dp.callback_query.middleware(ErrorHandlingMiddleware())

dp.include_router(onboarding_router)
dp.include_router(student_router)
dp.include_router(student_track_order_router)
dp.include_router(delivery_guy_router)
dp.include_router(vendor_router)
dp.include_router(rating_router)
dp.include_router(help_router)
dp.include_router(settings_router)

# --- DB + Scheduler ---
scheduler = BotScheduler(db=db, bot=bot)

# --- Lifespan handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("🚀 Starting Deliver AAU Bot (Webhook mode)...")

    # Init DB + seed
    await db.init_pool()
    # await db.reset_schema()
    await db.init_schema()  
    # await seed_vendors(db)          # pass Database instance, not db_path
    # await seed_delivery_guys(db)    # same here
    logging.info("✅ Database initialized")

    # Start scheduler
    scheduler.start()
    logging.info("✅ Scheduler started")

    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Start Deliver AAU"),
        BotCommand(command="help", description="Help & contact"),
        BotCommand(command="dashboard", description="Delivery Guy Dashboard"),
    ])

    webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/bot/{settings.BOT_TOKEN}"
    await bot.set_webhook(webhook_url)



    yield   # <-- app runs here

    # Shutdown
    logging.info("🛑 Shutting down Deliver AAU Bot...")
    scheduler.shutdown()
    await bot.session.close()

# --- FastAPI app ---
app = FastAPI(lifespan=lifespan)

@app.post(f"/bot/{settings.BOT_TOKEN}")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def health_check():
    return {"status": "ok", "service": "Deliver AAU Bot"}

if __name__ == "__main__":
    import asyncio

    async def main():
    # 1. Create pool
        await db.init_pool()

        # 2. Reset schema (drop/recreate tables)
        # await db.reset_schema()
        await db.init_schema()
        logging.info("✅ Database initialized")
        
        
        # await db.close_pool()
        await db.init_pool()

        # 3. Seed data
        # await seed_vendors(db)
        # await seed_delivery_guys(db)

        # 4. Start scheduler
        scheduler.start()

        # 5. Bot commands
        await bot.set_my_commands([
            BotCommand(command="start", description="Start Deliver AAU"),
            BotCommand(command="help", description="Help & contact"),
            BotCommand(command="dashboard", description="Delivery Guy Dashboard"),
        ])
        await bot.delete_webhook(drop_pending_updates=True)


        # 6. Start polling
        await dp.start_polling(bot)
    asyncio.run(main())