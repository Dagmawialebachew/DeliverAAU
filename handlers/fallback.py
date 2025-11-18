"""
Fallback handler for invalid inputs.
"""

from aiogram import Router
from aiogram.types import Message

from utils.localization import get_text
from keyboards.reply import get_main_menu_keyboard

router = Router()


@router.message()
async def fallback_handler(message: Message, lang: str = "en") -> None:
    """
    Handle invalid or unrecognized inputs.

    Args:
        message: Incoming message
        lang: User language
    """
    await message.answer(
        get_text(lang, "invalid_input"),
        reply_markup=get_main_menu_keyboard(lang)
    )
