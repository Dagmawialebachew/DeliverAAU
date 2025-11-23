# middlewares/graceful_fallback_middleware.py
import logging
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from typing import Callable, Dict, Any, Awaitable

from handlers.onboarding import main_menu  # or your main menu renderer

logger = logging.getLogger(__name__)

class GracefulFallbackMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any]
    ) -> Any:
        try:
            return await handler(event, data)
        except Exception as e:
            logger.exception("Unhandled exception in handler")

            # Deliver AAU‑themed fallback
            if isinstance(event, Message):
                await event.answer(
                    "⚠️ Something went wrong while processing your request.\n\n"
                    "✨ Don’t worry — your campus meal journey is safe.\n"
                    "Here’s your orders page again 👇"
                )
                try:
                    await main_menu(event, user_id=event.from_user.id, page=0)
                except Exception:
                    pass

            elif isinstance(event, CallbackQuery):
                await event.message.answer(
                    "⚠️ Something went wrong.\n\n"
                    "✨ Hang tight — let’s get you back to your orders page 👇"
                )
                try:
                    await main_menu(event, user_id=event.from_user.id, page=0)
                except Exception:
                    pass
                await event.answer("Error handled gracefully.", show_alert=False)

            return None
