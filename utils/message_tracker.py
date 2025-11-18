# utils/message_tracker.py

from aiogram.types import Message

async def tracked_send(message: Message, text: str, state, key: str, **kwargs):
    """
    Sends a message and stores its message_id under the given key in FSMContext.
    Example:
        await tracked_send(message, "✅ Location saved.", state, "location_saved")
    """
    sent = await message.answer(text, **kwargs)
    await _store_message_id(state, key, sent.message_id)
    return sent


async def _store_message_id(state, key: str, message_id: int):
    """Stores message_id with standardized key naming."""
    data = await state.get_data()
    tracked = data.get("_tracked_msg_ids", {})
    tracked[key] = message_id
    await state.update_data(_tracked_msg_ids=tracked)


async def cleanup_tracked_messages(bot, chat_id: int, state):
    """
    Deletes all tracked messages stored in FSM context (like '✅ saved', 'Next step', etc.)
    """
    data = await state.get_data()
    tracked = data.get("_tracked_msg_ids", {})

    for key, mid in tracked.items():
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass

    # Clear tracking state
    await state.update_data(_tracked_msg_ids={})
