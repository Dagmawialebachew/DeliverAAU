# middlewares/error_handling_middleware.py
import logging
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import TelegramObject
from config import settings
from utils.helpers import get_text  # reuse your localization helper

logger = logging.getLogger(__name__)

class ErrorHandlingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        try:
            return await handler(event, data)
        except Exception as e:
            logger.exception("Unhandled exception in handler")

            bot = data.get("bot")
            user = data.get("event_from_user")
            lang = "en"

            # Try to fetch user language from DB
            app_context = data.get("app_context")
            if user and app_context:
                try:
                    db_user = await app_context.db.get_user(user.id)
                    if db_user and db_user.get("language"):
                        lang = db_user["language"]
                except Exception:
                    pass

            # Graceful user-facing message
            # if bot and hasattr(event, "message") and event.message:
            #     if "timeout" in str(e).lower() or "took too long" in str(e).lower():
            #         await bot.send_message(
            #             event.message.chat.id,
            #             get_text("error_timeout", lang)
            #         )
            #     else:
            #         await bot.send_message(
            #             event.message.chat.id,
            #             get_text("error_general", lang)
            #         )

            # Notify admins
            if settings.ADMIN_ERROR_GROUP_ID and bot:
                await bot.send_message(
                    settings.ADMIN_ERROR_GROUP_ID,
                    f"❌ Exception:\n{e}",
                    parse_mode=None
                )

            return None
