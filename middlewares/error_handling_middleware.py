# middlewares/error_handling_middleware.py
import logging
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import TelegramObject
from config import settings

logger = logging.getLogger(__name__)

class ErrorHandlingMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject, data: Dict[str, Any]) -> Any:
        try:
            return await handler(event, data)
        except Exception as e:
            logger.exception("Unhandled exception in handler")

            # Notify user gracefully
            bot = data.get("bot")
            if bot and hasattr(event, "message") and event.message:
                pass

            if settings.ADMIN_GROUP_ID and bot:
                await bot.send_message(settings.ADMIN_GROUP_ID, f"❌ Exception: {e}")

            return None
