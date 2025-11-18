"""
Language middleware to inject user's preferred language.
"""

from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Update


class LanguageMiddleware(BaseMiddleware):
    """Middleware for injecting user language preference."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        """
        Inject user language into handler data.

        Args:
            handler: Next handler
            event: Telegram event
            data: Handler data

        Returns:
            Handler result
        """
        db = data.get("db")
        lang = "en"  # Default language

        # Extract user ID
        user_id = None
        if isinstance(event, Update):
            if event.message:
                user_id = event.message.from_user.id
            elif event.callback_query:
                user_id = event.callback_query.from_user.id

        # Fetch user language from database
        if db and user_id:
            user = await db.get_user(user_id)
            if user:
                lang = user.get("language", "en")

        # Inject language into data
        data["lang"] = lang

        return await handler(event, data)
