from typing import Dict, Any
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError 
from aiogram import Bot
# Dictionary to hold active order offers for live countdown.
# This is a shared state accessed by the scheduler and handlers.
# Key: order_id (int)
# Value: {
#   "chat_id": int,
#   "message_id": int,
#   "assigned_at": datetime,
#   "expiry_seconds": int,
#   "order_id": int
# }
PENDING_OFFERS: Dict[int, Dict[str, Any]] = {}

# Define the global offer expiry time (3 minutes)
EXPIRY_SECONDS = 180

