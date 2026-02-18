# keyboards/inline.py
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def 
confirm_order_kb() -> InlineKeyboardMarkup:
    """
    Inline confirmation for student before creating the order.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm", callback_data="order_confirm")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="order_cancel")]
    ])


def subscription_confirm_kb(sub_type: str) -> InlineKeyboardMarkup:
    """
    Inline confirm for subscription purchase instruction.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Activate {sub_type}", callback_data=f"subs_activate:{sub_type}")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="subs_cancel")]
    ])
