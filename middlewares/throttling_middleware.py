# middlewares/throttling_middleware.py
import asyncio
import time
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from typing import Callable, Dict, Any, Awaitable


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, message_interval: float = 1.5, callback_interval: float = 0.5) -> None:
        super().__init__()
        self.message_interval = message_interval
        self.callback_interval = callback_interval
        self._last_seen_msg: dict[int, float] = {}
        self._last_seen_cb: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user_id = getattr(getattr(event, "from_user", None), "id", None)
        now = time.time()

        if isinstance(event, Message):
            last = self._last_seen_msg.get(user_id, 0.0)
            if (now - last) < self.message_interval:
                try:
                    msg = await event.answer("🍲 Too many messages — Deliver AAU got it, no need to flood.")
                    await asyncio.sleep(1)
                    await msg.delete()
                except Exception:
                    pass
                return None
            self._last_seen_msg[user_id] = now

        elif isinstance(event, CallbackQuery):
            last = self._last_seen_cb.get(user_id, 0.0)
            if (now - last) < self.callback_interval:
                try:
                    await event.answer("⏳ Deliver AAU is updating — please don’t tap so fast.", show_alert=False)
                except Exception:
                    pass
                return None
            self._last_seen_cb[user_id] = now

        return await handler(event, data)
