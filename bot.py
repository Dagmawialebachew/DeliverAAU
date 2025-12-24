"""
UniBites Delivery Bot - FastAPI Webhook entry point.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Update, BotCommand, BotCommandScopeDefault, BotCommandScopeChat

from config import settings
from database.db import Database, debug_list_delivery_guys, generate_delivery_guy_row, seed_delivery_guys, seed_vendors, seed_specific_dg, update_menu_item_price
from utils.scheduler import BotScheduler

# Middlewares
from middlewares.logging_middleware import LoggingMiddleware
from middlewares.throttling_middleware import ThrottlingMiddleware
from middlewares.error_handling_middleware import ErrorHandlingMiddleware
from app_context import bot, dp, db
# Routers
from handlers.onboarding import router as onboarding_router
from handlers.admin_order import router as admin_order_router
from handlers.student import router as student_router
from handlers.student_track_order import router as student_track_order_router
from handlers.delivery_guy import router as delivery_guy_router
from handlers.vendor import router as vendor_router
from handlers.help import router as help_router
from handlers.settings import router as settings_router
from handlers.admin import router as admin_router
from handlers.rating import router as rating_router
from middlewares.gracefull_fallback_middleware import GracefulFallbackMiddleware

# # --- Logging ---
# logging.basicConfig(level=logging.INFO)

# # --- Bot & Dispatcher ---

# # Middlewares
# # Throttling: register separately for messages and callbacks
# dp.message.middleware(ThrottlingMiddleware(
#     message_interval=1.5,   # silently drop spammy messages
#     callback_interval=0.5   # polite popup for fast button clicks
# ))
# dp.callback_query.middleware(ThrottlingMiddleware(
#     message_interval=1.5,
#     callback_interval=0.5
# ))

# #Gracefull fall back middleware registeration


# dp.message.middleware(GracefulFallbackMiddleware())
# dp.callback_query.middleware(GracefulFallbackMiddleware())
# # Routers

# # Error handling: also register per-event
# dp.message.middleware(ErrorHandlingMiddleware())
# dp.callback_query.middleware(ErrorHandlingMiddleware())

# dp.include_router(onboarding_router)
# dp.include_router(student_router)
# dp.include_router(student_track_order_router)
# dp.include_router(delivery_guy_router)
# dp.include_router(vendor_router)
# dp.include_router(rating_router)
# dp.include_router(help_router)
# dp.include_router(settings_router)
# dp.include_router(admin_router)
# dp.include_router(admin_order_router)
# # --- DB + Scheduler ---
# scheduler = BotScheduler(db=db, bot=bot)

# # --- Lifespan handler ---
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logging.info("🚀 Starting UniBites Delivery Bot (Webhook mode)...")

#     # Init DB + seed
#     await db.init_pool()
#     # await db.reset_schema()
#     await db.init_schema()  
#     # await seed_vendors(db)          # pass Database instance, not db_path
#     # await seed_delivery_guys(db)    # same here
#     logging.info("✅ Database initialized")

#     # Start scheduler
#     scheduler.start()
#     logging.info("✅ Scheduler started")

#     # Set bot commands
#     await set_commands(bot, settings.ADMIN_IDS)

#     webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/bot/{settings.BOT_TOKEN}"
#     await bot.set_webhook(webhook_url)



#     yield   # <-- app runs here

#     # Shutdown
#     logging.info("🛑 Shutting down UniBites Delivery Bot...")
#     scheduler.shutdown()
#     await bot.session.close()

# # --- FastAPI app ---
# app = FastAPI(lifespan=lifespan)

# @app.post(f"/bot/{settings.BOT_TOKEN}")
# async def telegram_webhook(request: Request):
#     data = await request.json()
#     update = Update(**data)
#     await dp.feed_update(bot, update)
#     return {"ok": True}



# async def set_commands(bot, admin_ids: list[int]):
#     # Commands for everyone
#     user_commands = [
#         BotCommand(command="start", description="🚀 Start UniBites Delivery"),
#         BotCommand(command="help", description="❓ Help & Contact"),
#     ]
#     await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

#     # Commands only for admins
#     admin_commands = user_commands + [
#         BotCommand(command="admin", description="🔐 Admin Command Center"),
#     ]
#     for admin_id in admin_ids:
#         await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))



# @app.get("/")
# async def health_check():
#     return {"status": "ok", "service": "UniBites Delivery Bot"}

# if __name__ == "__main__":
#     import asyncio

#     async def main():
#     # 1. Create pool
#         await db.init_pool()

#         # 2. Reset schema (drop/recreate tables)
#         # await db.reset_schema()
#         await db.init_schema()
#         logging.info("✅ Database initialized")
        
        
#         # await db.close_pool()
#         await db.init_pool()

#         # 3. Seed data
#         # await seed_vendors(db)
#         # await seed_delivery_guys(db)
#         # await seed_speicific_dg(db)

#         # 4. Start scheduler
#         scheduler.start()
#         await set_commands(bot, settings.ADMIN_IDS)   # <-- call here

#         # 5. Bot commands
      

#         await bot.delete_webhook(drop_pending_updates=True)
#         logging.info(f"Dispatcher id: {id(dp)}")
#         logging.info("Webhook deleted, starting polling with dp id: %s", id(dp))


#         # 6. Start polling
#         await dp.start_polling(bot)
#     asyncio.run(main())
    
import asyncio
import logging
import os
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from config import settings
from app_context import bot, dp, db
from utils.scheduler import BotScheduler

# Routers
from handlers.onboarding import router as onboarding_router
from handlers.admin_order import router as admin_order_router
from handlers.student import router as student_router
from handlers.student_track_order import router as student_track_order_router
from handlers.delivery_guy import router as delivery_guy_router
from handlers.vendor import router as vendor_router
from handlers.help import router as help_router
from handlers.settings import router as settings_router
from handlers.admin import router as admin_router
from handlers.rating import router as rating_router

# Middlewares
from middlewares.throttling_middleware import ThrottlingMiddleware
from middlewares.error_handling_middleware import ErrorHandlingMiddleware
from middlewares.gracefull_fallback_middleware import GracefulFallbackMiddleware

logging.basicConfig(level=logging.INFO)

# --- Dispatcher setup ---
dp.message.middleware(ThrottlingMiddleware(message_interval=1.5, callback_interval=0.5))
dp.callback_query.middleware(ThrottlingMiddleware(message_interval=1.5, callback_interval=0.5))
dp.message.middleware(GracefulFallbackMiddleware())
dp.callback_query.middleware(GracefulFallbackMiddleware())
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
dp.include_router(admin_router)
dp.include_router(admin_order_router)

scheduler = BotScheduler(db=db, bot=bot)

# --- Bot commands ---
async def set_commands(bot, admin_ids: list[int]):
    user_commands = [
        BotCommand(command="start", description="🚀 Start UniBites Delivery"),
        BotCommand(command="help", description="❓ Help & Contact"),
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    admin_commands = user_commands + [
        BotCommand(command="admin", description="🔐 Admin Command Center"),
    ]
    for admin_id in admin_ids:
        await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))

# --- Startup / Shutdown ---
async def on_startup(bot: Bot):
    logging.info("🚀 Starting UniBites Delivery Bot...")
    await db.init_pool()
    await db.init_schema()
    scheduler.start()
    await set_commands(bot, settings.ADMIN_IDS)
    webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/webhook"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)
    logging.info(f"Webhook set to: {webhook_url}")

async def on_shutdown(bot: Bot):
    logging.info("🛑 Shutting down UniBites Delivery Bot...")
    scheduler.shutdown()
    await db.close_pool()
    await bot.session.close()

# --- Health check ---
async def health_check(request):
    return web.Response(text="OK")

# --- Webhook app factory ---
async def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health_check)

    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path="/webhook")

    setup_application(app, dp, bot=bot)

    app.on_startup.append(lambda app: on_startup(bot))
    app.on_cleanup.append(lambda app: on_shutdown(bot))
    return app

# --- Polling mode ---
async def start_polling():
    await db.init_pool()
    await db.init_schema()
    scheduler.start()
    await set_commands(bot, settings.ADMIN_IDS)
      # 3. Seed data
    # await seed_delivery_guys(db)
    # await seed_specific_dg(db)
    # await generate_delivery_guy_row(db, 1701238322)
    # await debug_list_delivery_guys(db)
    
        # 2. Reset schema (drop/recreate tables)
    # await db.reset_schema.
#     await seed_vendors(db)
#     await update_menu_item_price(
#     db=db,
#     vendor_telegram_id=8487056502,  # Tena Mgb Bet
#     item_id=1,
#     new_price=230,
# )
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

# --- Entrypoint ---
if __name__ == "__main__":
    if "--polling" in sys.argv:
        
       
        asyncio.run(start_polling())
    else:
        port = int(os.getenv("PORT", "8080"))
        logging.info(f"Starting webhook server on http://0.0.0.0:{port}")
        web.run_app(create_app(), host="0.0.0.0", port=port)
