# middlewares/error_handling_middleware.py
import logging
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import TelegramObject
from config import settings

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
            user = data.get("event_from_user")  # aiogram puts user here
            is_vendor = False

            # Example: check vendor flag from FSM state or DB
            state = data.get("state")
            if state:
                state_data = await state.get_data()
                is_vendor = state_data.get("is_vendor", False)

            # Graceful user-facing message
            if bot and hasattr(event, "message") and event.message:
                if "timeout" in str(e).lower() or "took too long" in str(e).lower():
                    if is_vendor:
                        await bot.send_message(
                            event.message.chat.id,
                            "⚠️ ጥያቄዎ በጊዜ ተዘግቷል። እባክዎ ይቆዩ ወይም እንደገና ይሞክሩ።"
                        )
                    else:
                        await bot.send_message(
                            event.message.chat.id,
                            "⚠️ Your request took too long. Please wait or try again."
                        )

            if settings.ADMIN_ERROR_GROUP_ID and bot:
                await bot.send_message(
                    settings.ADMIN_ERROR_GROUP_ID,
                    f"❌ Exception:\n{e}",
                    parse_mode=None
                )

            return None

