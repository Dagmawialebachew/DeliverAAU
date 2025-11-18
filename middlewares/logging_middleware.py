"""
Logging middleware for all updates.
"""

import logging
from typing import Callable, Dict, Any, Awaitable
from datetime import datetime
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Update

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseMiddleware):
    """Middleware for logging all incoming updates."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        """
        Process update and log details.

        Args:
            handler: Next handler
            event: Telegram event
            data: Handler data

        Returns:
            Handler result
        """
        if isinstance(event, Update):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Extract user info
            user = None
            if event.message:
                user = event.message.from_user
                update_type = "message"
                content = event.message.text or "[media]"
            elif event.callback_query:
                user = event.callback_query.from_user
                update_type = "callback"
                content = event.callback_query.data
            else:
                update_type = "unknown"
                content = ""

            if user:
                logger.info(
                    f"[{timestamp}] {update_type.upper()} | "
                    f"User: {user.id} (@{user.username or 'no_username'}) | "
                    f"Content: {content[:50]}"
                )

        return await handler(event, data)
