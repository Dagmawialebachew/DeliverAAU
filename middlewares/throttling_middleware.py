# middlewares/throttling_middleware.py
import time
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable


class ThrottlingMiddleware(BaseMiddleware):
    """
    Simple per-user throttling to reduce spam.
    """

    def __init__(self, interval: float = 0.8) -> None:
        super().__init__()
        self.interval = interval
        self._last_seen: dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        user_id = getattr(getattr(event, "from_user", None), "id", None)
        now = time.time()
        allow = True

        if user_id is not None:
            last = self._last_seen.get(user_id, 0.0)
            allow = (now - last) >= self.interval
            if allow:
                self._last_seen[user_id] = now

        if not allow:
            # Soft-drop the event for UX smoothness; no error message.
            return None

        return await handler(event, data)
