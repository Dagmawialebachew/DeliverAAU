# """
# Deliver AAU Bot - Local polling entry point.
# """

# import asyncio
# import logging
# from aiogram import Bot, Dispatcher
# from aiogram.fsm.storage.memory import MemoryStorage
# from aiogram.enums import ParseMode
# from aiogram.client.default import DefaultBotProperties
# from aiogram.types import BotCommand

# from config import settings
# from database.db import Database, seed_delivery_guys, seed_vendors
# from utils.scheduler import BotScheduler

# # Middlewares
# from middlewares.logging_middleware import LoggingMiddleware
# from middlewares.throttling_middleware import ThrottlingMiddleware

# # Routers
# from handlers.onboarding import router as onboarding_router
# from handlers.student import router as student_router
# from handlers.student_track_order import router as student_track_order_router
# from handlers.delivery_guy import router as delivery_guy_router
# from handlers.vendor import router as vendor_router

# async def main():
#     logging.basicConfig(level=logging.INFO)

#     bot = Bot(
#         token=settings.BOT_TOKEN,
#         default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
#     )
#     dp = Dispatcher(storage=MemoryStorage())

#     # Middlewares
#     dp.update.middleware(LoggingMiddleware())
#     dp.update.middleware(ThrottlingMiddleware(interval=0.6))

#     # Routers
#     dp.include_router(onboarding_router)
#     dp.include_router(student_router)
#     dp.include_router(student_track_order_router)
#     dp.include_router(delivery_guy_router)
#     dp.include_router(vendor_router)

#     # DB + Scheduler
#     db = Database(settings.DB_PATH)
#     await db.init_db()
#     await seed_vendors(db.db_path)
#     await seed_delivery_guys(db.db_path)

#     scheduler = BotScheduler(db=db, bot=bot)
#     scheduler.start()

#     # Bot commands
#     await bot.set_my_commands([
#         BotCommand(command="start", description="Start Deliver AAU"),
#         BotCommand(command="help", description="Help & contact"),
#         BotCommand(command="dashboard", description="Delivery Guy Dashboard"),
#     ])

#     logging.info("🚀 Deliver AAU Bot started (Polling mode)")
#     try:
#         await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
#     finally:
#         scheduler.shutdown()
#         await bot.session.close()

# if __name__ == "__main__":
#     asyncio.run(main())
