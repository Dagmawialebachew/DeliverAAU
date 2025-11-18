"""
Deliver AAU Bot - FastAPI Webhook entry point.
"""

import asyncio
import logging
import os

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

# Routers
from handlers.onboarding import router as onboarding_router
from handlers.student import router as student_router
from handlers.student_track_order import router as student_track_order_router
from handlers.delivery_guy import router as delivery_guy_router
from handlers.vendor import router as vendor_router

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Bot & Dispatcher ---
bot = Bot(
    token=settings.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
)
dp = Dispatcher(storage=MemoryStorage())

# Middlewares
dp.update.middleware(LoggingMiddleware())
dp.update.middleware(ThrottlingMiddleware(interval=0.6))

# Routers
dp.include_router(onboarding_router)
dp.include_router(student_router)
dp.include_router(student_track_order_router)
dp.include_router(delivery_guy_router)
dp.include_router(vendor_router)

# --- DB + Scheduler ---
db = Database(settings.DB_PATH)
scheduler = BotScheduler(db=db, bot=bot)

# --- FastAPI app ---
app = FastAPI()


@app.on_event("startup")
async def on_startup():
    logging.info("🚀 Starting Deliver AAU Bot (Webhook mode)...")

    # Init DB + seed
    await db.init_db()
    await seed_vendors(db.db_path)
    await seed_delivery_guys(db.db_path)
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

    # Set webhook
    webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/bot/{settings.BOT_TOKEN}"
    await bot.set_webhook(webhook_url)
    logging.info(f"🎯 Webhook set to {webhook_url}")


@app.on_event("shutdown")
async def on_shutdown():
    logging.info("🛑 Shutting down Deliver AAU Bot...")
    scheduler.shutdown()
    await bot.session.close()


@app.post(f"/bot/{settings.BOT_TOKEN}")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update(**data)
    await dp.feed_update(bot, update)
    return {"ok": True}


@app.get("/")
async def health_check():
    return {"status": "ok", "service": "Deliver AAU Bot"}
