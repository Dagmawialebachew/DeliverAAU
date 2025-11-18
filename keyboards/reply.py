# keyboards/reply.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def main_menu() -> ReplyKeyboardMarkup:
    """
    Student-first main menu: clear, compact, and actionable.
    """
    buttons = [
        [KeyboardButton(text="📦 Request Delivery")],
        [KeyboardButton(text="🚶 Track Order")],
        [KeyboardButton(text="💰 My Coins"), KeyboardButton(text="🏆 Leaderboard")],
        [KeyboardButton(text="🪙 Subscriptions"), KeyboardButton(text="🛠 Settings")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def subscriptions_menu() -> ReplyKeyboardMarkup:
    """
    Subscriptions menu (Monthly & Daily).
    """
    buttons = [
        [KeyboardButton(text="🗓 Monthly plan")],
        [KeyboardButton(text="📅 Daily pass")],
        [KeyboardButton(text="⬅️ Back")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def settings_menu() -> ReplyKeyboardMarkup:
    """
    Settings submenu for quick student updates.
    """
    buttons = [
        [KeyboardButton(text="Change campus")],
        [KeyboardButton(text="Change phone")],
        [KeyboardButton(text="Contact support")],
        [KeyboardButton(text="⬅️ Back")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
