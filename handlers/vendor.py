# -*- coding: utf-8 -*-
import json
import math
import datetime
from typing import Optional, List, Dict, Any

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)
from aiogram import Bot
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from database.db import Database
from config import settings

# Notifications (English for students/DGs/admin), Amharic for vendors via texts in handlers)
from utils.db_helpers import (
    notify_student_prepared,
    notify_student_cancelled,
    notify_dg_pickup_ready,
    notify_dg_cancelled,
    notify_admin_log,
    calc_vendor_day_summary,
    calc_vendor_week_summary,
)

router = Router()

# You can inject db and bot from your app bootstrap
db = Database(settings.DB_PATH)
ADMIN_GROUP_ID = settings.ADMIN_GROUP_ID

# -------------------------------------------------
# 📌 Central Dashboard (ReplyKeyboard, Amharic UX)
# -------------------------------------------------
def vendor_dashboard_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 ሜኑ"), KeyboardButton(text="📦 ትዕዛዞች")],
            [KeyboardButton(text="📊 አፈጻጸም"), KeyboardButton(text="👨‍💼 አስተዳዳሪን አግኝ")],
        ],
        resize_keyboard=True
    )

def performance_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 የዛሬ ትዕዛዞች"), KeyboardButton(text="📅 የሳምንቱ ትዕዛዞች")],
            [KeyboardButton(text="⬅️ ወደ ዳሽቦርድ")],
        ],
        resize_keyboard=True
    )

# -------------------------------------------------
# Helpers (pagination UI)
# -------------------------------------------------
def paginate_orders_kb(page: int, pages: int, scope: str, extra_payload: str = "") -> InlineKeyboardMarkup:
    # scope: "daily" or "weekly"
    prev_cb = f"perf:{scope}:page:{page-1}:{extra_payload}" if page > 1 else "perf:noop"
    next_cb = f"perf:{scope}:page:{page+1}:{extra_payload}" if page < pages else "perf:noop"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ ቀድሞ", callback_data=prev_cb),
                InlineKeyboardButton(text=f"📄 ገጽ {page}/{pages}", callback_data="perf:noop"),
                InlineKeyboardButton(text="➡️ የሚቀጥለው", callback_data=next_cb),
            ]
        ]
    )

# -------------------------------------------------
# Entry point: /vendor and Back to dashboard
# -------------------------------------------------
@router.message(Command("vendor"))
@router.message(F.text == "⬅️ ወደ ዳሽቦርድ")
async def show_vendor_dashboard(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ እባክዎ ከአስተዳዳሪ ጋር ይነጋገሩ።", reply_markup=ReplyKeyboardRemove())
        return

    text = (
        f"🏪 <b>{vendor['name']}</b>\n"
        f"⭐ አማካይ ደረጃ: {float(vendor.get('rating_avg', 0.0)):.1f} "
        f"({int(vendor.get('rating_count', 0))} አስተያየት)\n\n"
        "ወደ ዳሽቦርድ እንኳን በደህና መጡ!"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=vendor_dashboard_keyboard())

# -------------------------------------------------
# 📋 Menu Management (simple inline actions)
# -------------------------------------------------
@router.message(F.text == "📋 ሜኑ")
async def vendor_menu(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም። እባክዎ አስተዳዳሪን አግኙ።")
        return

    menu = json.loads(vendor.get("menu_json") or "[]")
    if not menu:
        await message.answer(
            "📭ሜኑዎ ባዶ ነው።\n➕ አዲስ አክል ይጫኑ።",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="➕ አዲስ አክል", callback_data=f"menu:add:{vendor['id']}")]
                ]
            )
        )
        # also show reply keyboard for dashboard navigation
        await message.answer(
            "⬅️ ወደ ዳሽቦርድ መመለስ 👇",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ ወደ ዳሽቦርድ")]],
                resize_keyboard=True
            )
        )
        return

    lines = ["📋 የምግብ ዝርዝር"]
    for item in menu:
        lines.append(f"• {item.get('name','')} — {item.get('price',0)} ብር")

    # send inline keyboard with menu actions
    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ አዲስ ምግብ", callback_data=f"menu:add:{vendor['id']}"), InlineKeyboardButton(text="📝 አስተካክል", callback_data=f"menu:edit:{vendor['id']}")],
                [InlineKeyboardButton(text="🗑 አስወግድ", callback_data=f"menu:remove:{vendor['id']}")],
            ]
        )
    )

    # then send reply keyboard for dashboard navigation
    await message.answer(
        "⬅️ ወደ ዳሽቦርድ መመለስ 👇",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ ወደ ዳሽቦርድ")]],
            resize_keyboard=True
        )
    )


class VendorMenuStates(StatesGroup):
    add_name = State()
    add_price = State()
    edit_select = State()
    edit_price = State()
    remove_select = State()

# -----------------------------
# ➕ Add new item
# -----------------------------
@router.callback_query(F.data.startswith("menu:add:"))
async def menu_add(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    vendor_id = int(cb.data.split(":")[-1])
    await state.update_data(vendor_id=vendor_id)
    await cb.message.answer("🍴 የምግቡ ስም ያስገቡ።")
    await state.set_state(VendorMenuStates.add_name)
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"ℹ️ Vendor #{vendor_id} started adding a new menu item.")

@router.message(VendorMenuStates.add_name)
async def menu_add_name(message: Message, state: FSMContext):
    await state.update_data(item_name=message.text.strip())
    await message.answer("💵 ዋጋውን ያስገቡ (በብር).")
    await state.set_state(VendorMenuStates.add_price)


@router.message(VendorMenuStates.add_price)
async def menu_add_price(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    vendor_id = data["vendor_id"]
    item_name = data["item_name"]
    try:
        price = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ እባክዎ ትክክለኛ ዋጋ ያስገቡ (ቁጥር).")
        return

    vendor = await db.get_vendor(vendor_id)
    menu = json.loads(vendor.get("menu_json") or "[]")
    new_id = max([i["id"] for i in menu], default=0) + 1
    menu.append({"id": new_id, "name": item_name, "price": price})
    await db.update_vendor_menu(vendor_id, menu)

    # confirmation
    await message.answer(f"✅ አዲስ ምግብ '{item_name}' ({price} ብር) ተጨምሯል።")

    # immediately show updated menu again
    await vendor_menu(message)

    await state.clear()
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"✅ Vendor #{vendor_id} added new item '{item_name}' ({price} birr).")



# -----------------------------
# 📝 Edit existing item
# -----------------------------
@router.callback_query(F.data.startswith("menu:edit:"))
async def menu_edit(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    vendor_id = int(cb.data.split(":")[-1])
    vendor = await db.get_vendor(vendor_id)
    menu = json.loads(vendor.get("menu_json") or "[]")
    if not menu:
        await cb.message.answer("📭 ሜኑ ባዶ ነው።")
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"{i['name']} ({i['price']} ብር)", callback_data=f"edit_item:{vendor_id}:{i['id']}")] for i in menu]
    )
    await cb.message.answer("📝 የሚለውን እቃ ይምረጡ።", reply_markup=kb)
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"📝 Vendor #{vendor_id} started editing menu.")

@router.callback_query(F.data.startswith("edit_item:"))
async def edit_item(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    _, vendor_id, item_id = cb.data.split(":")
    await state.update_data(vendor_id=int(vendor_id), item_id=int(item_id))
    await cb.message.answer("💵 አዲስ ዋጋ ያስገቡ (በብር).")
    await state.set_state(VendorMenuStates.edit_price)

@router.message(VendorMenuStates.edit_price)
async def edit_price(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    vendor_id = data["vendor_id"]
    item_id = data["item_id"]
    try:
        new_price = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ እባክዎ ትክክለኛ ዋጋ ያስገቡ.")
        return

    vendor = await db.get_vendor(vendor_id)
    menu = json.loads(vendor.get("menu_json") or "[]")
    for item in menu:
        if item["id"] == item_id:
            item["price"] = new_price
    await db.update_vendor_menu(vendor_id, menu)

    await message.answer(f"✅ ዋጋ ተስተካክሏል።")
    await vendor_menu(message)

    await state.clear()
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"📝 Vendor #{vendor_id} updated item #{item_id} price to {new_price} birr.")

# -----------------------------
# 🗑 Remove item
# -----------------------------
@router.callback_query(F.data.startswith("menu:remove:"))
async def menu_remove(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    vendor_id = int(cb.data.split(":")[-1])
    vendor = await db.get_vendor(vendor_id)
    menu = json.loads(vendor.get("menu_json") or "[]")
    if not menu:
        await cb.message.answer("📭 ምናሌ ባዶ ነው።")
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"🗑 {i['name']} ({i['price']} ብር)", callback_data=f"remove_item:{vendor_id}:{i['id']}")] for i in menu]
    )
    await cb.message.answer("🗑 የሚለውን እቃ ይምረጡ።", reply_markup=kb)
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"🗑 Vendor #{vendor_id} started removing menu item.")

@router.callback_query(F.data.startswith("remove_item:"))
async def remove_item(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    _, vendor_id, item_id = cb.data.split(":")
    vendor_id = int(vendor_id)
    item_id = int(item_id)

    vendor = await db.get_vendor(vendor_id)
    menu = json.loads(vendor.get("menu_json") or "[]")
    menu = [i for i in menu if i["id"] != item_id]
    await db.update_vendor_menu(vendor_id, menu)

    await cb.message.answer("✅ እቃው ተሰርዘዋል።")
    await vendor_menu(cb.message)

    await state.clear()
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"🗑 Vendor #{vendor_id} removed item #{item_id}.")

#----------------------------------------------
# 📦 Active Orders (Prepared / Out of Stock)
# -------------------------------------------------


@router.message(F.text == "📦 ትዕዛዞች")
async def vendor_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም። እባክዎ አስተዳዳሪን አግኙ።")
        return

    # Count orders in each category
    new_count = await db.count_orders_for_vendor(vendor["id"], status_filter=["pending","assigned"])
    preparing_count = await db.count_orders_for_vendor(vendor["id"], status_filter=["preparing"])
    ready_count = await db.count_orders_for_vendor(vendor["id"], status_filter=["ready"])  # same status, but shown separately

    # Simple Amharic summary
    summary_text = (
        "📦 የትዕዛዝ መግለጫ\n\n"
        f"🆕 አዲስ ትዕዛዞች: {new_count}\n"
        f"⚙️ በመዘጋጀት ላይ: {preparing_count}\n"
        f"✅ ዝግጁ ለመውሰድ: {ready_count}\n\n"
        "👇 ከታች ያለውን ቁልፍ ይጠቀሙ።"
    )

    await message.answer(summary_text, reply_markup=vendor_orders_keyboard())



@router.callback_query(F.data.startswith("vendor:accept:"))
async def vendor_accept_order(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    await db.update_order_status(order_id, "preparing")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("✅ ትዕዛዙ ተቀበለ።")

    # Notify student
    student_chat_id = await db.get_student_chat_id(order)
    # if student_chat_id:
    #     await cb.bot.send_message(student_chat_id, f"✅ ትዕዛዝዎ #{order_id} ተቀበለ።")

    # Admin log
    vendor = await db.get_vendor(order["vendor_id"])
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"✅ Vendor {vendor['name']} accepted Order #{order_id}.")


@router.callback_query(F.data.startswith("vendor:reject:"))
async def vendor_reject_order(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    await db.update_order_status(order_id, "cancelled")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("❌ ትዕዛዙ አልተቀበለም።")

    # Notify student
    student_chat_id = await db.get_student_chat_id(order)
    if student_chat_id:
        await cb.bot.send_message(student_chat_id, f"❌ ትዕዛዝዎ #{order_id} አልተቀበለም።")

    # Admin log
    vendor = await db.get_vendor(order["vendor_id"])
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"⚠️ Vendor {vendor['name']} rejected Order #{order_id}.")



def vendor_orders_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🆕 አዲስ ትዕዛዞች"), KeyboardButton(text="⚙️ በመዘጋጀት ላይ ያሉ")],
            [KeyboardButton(text="✅ ዝግጁ ለመውሰድ"), KeyboardButton(text="⬅️ ወደ ዳሽቦርድ")],
        ],
        resize_keyboard=True
    )


# -----------------------------
# Shared pagination keyboard
# scope: new | preparing | ready
# -----------------------------
def paginate_kb(page: int, pages: int, scope: str) -> InlineKeyboardMarkup:
    prev_cb = f"orders:{scope}:page:{page-1}" if page > 1 else "orders:noop"
    next_cb = f"orders:{scope}:page:{page+1}" if page < pages else "orders:noop"
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ ቀድሞ", callback_data=prev_cb),
            InlineKeyboardButton(text=f"📄 ገጽ {page}/{pages}", callback_data="orders:noop"),
            InlineKeyboardButton(text="➡️ የሚቀጥለው", callback_data=next_cb),
        ]]
    )

# -----------------------------
# Helpers: render one order line (Amharic)
# -----------------------------
def render_order_line(o: dict, include_dg: bool = False) -> str:
    items = ", ".join(i.get("name", "") for i in json.loads(o.get("items_json") or "[]"))
    parts = [
        f"📦 ትዕዛዝ #{o['id']}",
        f"🛒 እቃዎች: {items}",
        f"💵 ዋጋ: {int(o.get('food_subtotal', 0))} ብር",
    ]
    if include_dg and o.get("delivery_guy_id"):
        parts.append("🚴 የሚያውሰዱት: " + (o.get("dg_name") or "—"))
    return "\n".join(parts)

# -----------------------------
# 🆕 New Orders (pending/assigned) + pagination
# -----------------------------
async def safe_send(bot: Bot, chat_id: int, text: str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        # swallow; consider logging to DB or Sentry
        pass

# -----------------------------
# 🆕 New Orders (pending/assigned) + pagination
# -----------------------------
@router.message(F.text == "🆕 አዲስ ትዕዛዞች")
async def vendor_new_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም። እባክዎ አስተዳዳሪን አግኙ።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["pending", "assigned"])
    if total == 0:
        await message.answer("📭 አዲስ ትዕዛዝ የለም።", reply_markup=vendor_orders_keyboard())
        return

    pages = max(1, math.ceil(total / page_size))
    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["pending", "assigned"], limit=page_size, offset=0)

    for o in orders:
        text = render_order_line(o)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ ተቀበል", callback_data=f"vendor:accept:{o['id']}")],
                [InlineKeyboardButton(text="❌ አልተቀበለም", callback_data=f"vendor:reject:{o['id']}")]
            ]
        )
        await message.answer(text, reply_markup=kb)

    await message.answer("📄 ገጽ 1", reply_markup=paginate_kb(1, pages, "new"))


@router.callback_query(F.data.startswith("orders:new:page:"))
async def vendor_new_orders_page(cb: CallbackQuery):
    await cb.answer()
    page = int(cb.data.split(":")[-1])
    vendor = await db.get_vendor_by_telegram(cb.from_user.id)
    if not vendor:
        await cb.message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["pending", "assigned"])
    pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size

    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["pending", "assigned"], limit=page_size, offset=offset)
    await cb.message.edit_reply_markup(reply_markup=None)

    if not orders:
        await cb.message.answer("📭 አዲስ ትዕዛዝ የለም።")
    else:
        for o in orders:
            text = render_order_line(o)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ ተቀበል", callback_data=f"vendor:accept:{o['id']}")],
                    [InlineKeyboardButton(text="❌ አልተቀበለም", callback_data=f"vendor:reject:{o['id']}")]
                ]
            )
            await cb.message.answer(text, reply_markup=kb)

    await cb.message.answer(f"📄 ገጽ {page}/{pages}", reply_markup=paginate_kb(page, pages, "new"))


# -----------------------------
# ✅ Accept / Reject actions
# -----------------------------
@router.callback_query(F.data.startswith("vendor:accept:"))
async def vendor_accept_order(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    # Status -> preparing, record accepted_at
    await db.update_order_status(order_id, "preparing")
    # helper must exist: sets accepted_at = CURRENT_TIMESTAMP (implement in db layer)
    try:
        await db.set_order_timestamp(order_id, "accepted_at")
    except Exception:
        pass

    vendor = await db.get_vendor(order["vendor_id"])
    vendor_name = vendor["name"] if vendor else "Vendor"

    # Vendor sees Amharic
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("⚙️ ትዕዛዙ በመዘጋጀት ላይ ነው።")

    # Notify student (English short message; you can localize)
    student_chat_id = await db.get_student_chat_id(order)
    if student_chat_id:
        await safe_send(bot, student_chat_id, f"✅ Your order #{order_id} is being prepared by {vendor_name}.")

    # Admin log
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"✅ Vendor {vendor_name} accepted Order #{order_id} (status: preparing).")


@router.callback_query(F.data.startswith("vendor:reject:"))
async def vendor_reject_order(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    await db.update_order_status(order_id, "cancelled")
    vendor = await db.get_vendor(order["vendor_id"])
    vendor_name = vendor["name"] if vendor else "Vendor"

    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("❌ ትዕዛዙ ተሰርዘዋል።")

    # Student notify
    student_chat_id = await db.get_student_chat_id(order)
    if student_chat_id:
        await safe_send(bot, student_chat_id, f"❌ Your order #{order_id} was cancelled by {vendor_name}.")

    # Notify assigned DG (if any) in English
    if order.get("delivery_guy_id"):
        dg = await db.get_delivery_guy(order["delivery_guy_id"])
        if dg:
            await safe_send(bot, dg["user_id"], f"⚠️ Order #{order_id} was cancelled by {vendor_name}.")

    await notify_admin_log(bot, ADMIN_GROUP_ID, f"⚠️ Vendor {vendor_name} cancelled Order #{order_id}.")


# -----------------------------
# ⚙️ Preparing Orders (preparing) + pagination
# -----------------------------
@router.message(F.text == "⚙️ በመዘጋጀት ላይ ያሉ")
async def vendor_preparing_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["preparing"])
    if total == 0:
        await message.answer("📭 በመዘጋጀት ላይ ያለ ትዕዛዝ የለም።", reply_markup=vendor_orders_keyboard())
        return

    pages = max(1, math.ceil(total / page_size))
    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["preparing"], limit=page_size, offset=0)
    for o in orders:
        text = render_order_line(o)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ ዝግጁ ለመውሰድ", callback_data=f"order:ready:{o['id']}")],
                [InlineKeyboardButton(text="❌ ተሰርዘዋል", callback_data=f"order:cancel:{o['id']}")]
            ]
        )
        await message.answer(text, reply_markup=kb)

    await message.answer("📄 ገጽ 1", reply_markup=paginate_kb(1, pages, "preparing"))


@router.callback_query(F.data.startswith("orders:preparing:page:"))
async def vendor_preparing_orders_page(cb: CallbackQuery):
    await cb.answer()
    page = int(cb.data.split(":")[-1])
    vendor = await db.get_vendor_by_telegram(cb.from_user.id)
    if not vendor:
        await cb.message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["preparing"])
    pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size

    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["preparing"], limit=page_size, offset=offset)
    await cb.message.edit_reply_markup(reply_markup=None)

    if not orders:
        await cb.message.answer("📭 በመዘጋጀት ላይ ያለ ትዕዛዝ የለም።")
    else:
        for o in orders:
            text = render_order_line(o)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ ዝግጁ ለመውሰድ", callback_data=f"order:ready:{o['id']}")],
                    [InlineKeyboardButton(text="❌ ተሰርዘዋል", callback_data=f"order:cancel:{o['id']}")]
                ]
            )
            await cb.message.answer(text, reply_markup=kb)

    await cb.message.answer(f"📄 ገጽ {page}/{pages}", reply_markup=paginate_kb(page, pages, "preparing"))


# -----------------------------
# ✅ Ready for Pickup (ready) + pagination
# -----------------------------
@router.message(F.text == "✅ ዝግጁ ለመውሰድ")
async def vendor_ready_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["ready"])
    if total == 0:
        await message.answer("📭 ዝግጁ የሆነ ትዕዛዝ የለም።", reply_markup=vendor_orders_keyboard())
        return

    pages = max(1, math.ceil(total / page_size))
    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["ready"], limit=page_size, offset=0)

    for o in orders:
        dg_name = ""
        if o.get("delivery_guy_id"):
            dg = await db.get_delivery_guy(o["delivery_guy_id"])
            if dg:
                dg_name = dg.get("name", "")
        line = render_order_line({**o, "dg_name": dg_name}, include_dg=True)
        await message.answer(line)

    await message.answer("📄 ገጽ 1", reply_markup=paginate_kb(1, pages, "ready"))


@router.callback_query(F.data.startswith("orders:ready:page:"))
async def vendor_ready_orders_page(cb: CallbackQuery):
    await cb.answer()
    page = int(cb.data.split(":")[-1])
    vendor = await db.get_vendor_by_telegram(cb.from_user.id)
    if not vendor:
        await cb.message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], status_filter=["ready"])
    pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size

    orders = await db.get_orders_for_vendor(vendor["id"], status_filter=["ready"], limit=page_size, offset=offset)
    await cb.message.edit_reply_markup(reply_markup=None)

    if not orders:
        await cb.message.answer("📭 ዝግጁ የሆነ ትዕዛዝ የለም።")
    else:
        for o in orders:
            dg_name = ""
            if o.get("delivery_guy_id"):
                dg = await db.get_delivery_guy(o["delivery_guy_id"])
                if dg:
                    dg_name = dg.get("name", "")
            line = render_order_line({**o, "dg_name": dg_name}, include_dg=True)
            await cb.message.answer(line)

    await cb.message.answer(f"📄 ገጽ {page}/{pages}", reply_markup=paginate_kb(page, pages, "ready"))


# -----------------------------
# 📦 Mark Ready (notify DG in English)
# -----------------------------
@router.callback_query(F.data.startswith("order:ready:"))
async def order_mark_ready(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    # Update status and optionally set a timestamp (if you track)
    await db.update_order_status(order_id, "ready")

    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("✅ ትዕዛዙ ዝግጁ ለመውሰድ ነው።")

    # Notify DG (English, eye-catching, include essential info)
    if order.get("delivery_guy_id"):
        dg = await db.get_delivery_guy(order["delivery_guy_id"])
        if dg:
            vendor = await db.get_vendor(order["vendor_id"])
            vendor_name = vendor["name"] if vendor else "Vendor"
            pickup = order.get("pickup") or "Vendor location"
            dropoff = order.get("dropoff") or "Student location"
            try:
                items = json.loads(order.get("items_json", "[]")) or []
            except Exception:
                items = []
            item_list = ", ".join([i.get("name", "") for i in items]) or "Items"
            total_food = order.get("food_subtotal", 0)
            delivery_fee = order.get("delivery_fee", 0)

            dg_msg = (
                f"🚨 NEW PICKUP ALERT 🚨\n\n"
                f"📦 Order #{order_id} is READY\n"
                f"📍 Pickup: {pickup}\n"
                f"🎯 Dropoff: {dropoff}\n"
                f"🛒 Items: {item_list}\n"
                f"💵 Total: {total_food} Birr + Delivery Fee: {delivery_fee} Birr\n\n"
                f"👉 GO NOW to collect this order."
            )
            await safe_send(bot, dg["user_id"], dg_msg)
    else:
        vendor = await db.get_vendor(order["vendor_id"])
        vendor_name = vendor["name"] if vendor else "Vendor"
        await notify_admin_log(bot, ADMIN_GROUP_ID, f"⚠️ Order #{order_id} from {vendor_name} is ready but no DG assigned.")

    # Notify student
    student_chat_id = await db.get_student_chat_id(order)
    if student_chat_id:
        await safe_send(bot, student_chat_id, f"📦 Your order #{order_id} is ready and will be picked up soon.")

    # Admin log
    vendor = await db.get_vendor(order["vendor_id"])
    vendor_name = vendor["name"] if vendor else "Vendor"
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"✅ Vendor {vendor_name} marked Order #{order_id} as READY.")


# -----------------------------
# ❌ Cancel (single handler)
# -----------------------------
@router.callback_query(F.data.startswith("order:cancel:"))
async def order_mark_cancelled(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    order_id = int(cb.data.split(":")[-1])
    order = await db.get_order(order_id)
    if not order:
        await cb.message.answer("⚠️ ትዕዛዝ አልተገኘም።")
        return

    await db.update_order_status(order_id, "cancelled")
    vendor = await db.get_vendor(order["vendor_id"])
    vendor_name = vendor["name"] if vendor else "Vendor"

    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("❌ ትዕዛዙ ተሰርዘዋል።")

    # Student notify
    student_chat_id = await db.get_student_chat_id(order)
    if student_chat_id:
        await safe_send(bot, student_chat_id, f"❌ Your order #{order_id} was cancelled by {vendor_name}.")

    # DG notify if assigned
    if order.get("delivery_guy_id"):
        dg = await db.get_delivery_guy(order["delivery_guy_id"])
        if dg:
            await safe_send(bot, dg["user_id"], f"⚠️ Order #{order_id} has been cancelled.")

    await notify_admin_log(bot, ADMIN_GROUP_ID, f"⚠️ Vendor {vendor_name} cancelled Order #{order_id} (out of stock).")
# -------------------------------------------------
# 📊 Performance (summary + Today/Weekly paginated)
# -------------------------------------------------
@router.message(F.text == "📊 አፈጻጸም")
async def vendor_performance(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    # Fresh daily summary
    s = await calc_vendor_day_summary(db.db_path, vendor["id"], date=datetime.date.today().strftime("%Y-%m-%d"))
    text = (
        "📊 የአፈጻጸም ሪፖርት\n"
        f"📦 ትዕዛዞች: {s['delivered'] + s['cancelled']} (✅ {s['delivered']} | ❌ {s['cancelled']})\n"
        f"💵 የዛሬ ገቢ: {int(s['total_payout'])} ብር\n"
        f"💵 የሳምንቱ ገቢ: — በላይ ያለውን ምናሌ ይጠቀሙ\n"
        f"⭐ አማካይ ደረጃ: {float(s['rating_avg']):.1f}\n"
        f"⚡ ታማኝነት: {int(s['reliability_pct'])}%"
    )
    await message.answer(text, reply_markup=performance_keyboard())

@router.message(F.text == "📅 የዛሬ ትዕዛዞች")
async def performance_today_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    today = datetime.date.today().strftime("%Y-%m-%d")
    total = await db.count_orders_for_vendor(vendor["id"], date=today)
    page_size = 10
    pages = max(1, math.ceil(total / page_size))

    # Fetch page 1
    orders = await db.get_orders_for_vendor(vendor["id"], date=today, limit=page_size, offset=0)
    if not orders:
        await message.answer("📭 ዛሬ ትዕዛዝ የለም።", reply_markup=performance_keyboard())
        return

    for o in orders:
        items = ", ".join(i.get("name","") for i in json.loads(o.get("items_json") or "[]"))
        await message.answer(
            f"📦 ትዕዛዝ #{o['id']} — {o['status']}\n"
            f"🛒 እቃዎች: {items}\n"
            f"💵 ክፍያ: {int(o.get('delivery_fee', 0))} ብር\n"
            f"📍 መድረሻ: {o.get('dropoff','')}"
        )

    kb = paginate_orders_kb(page=1, pages=pages, scope="daily", extra_payload=today)
    await message.answer("📄 ገጽ 1", reply_markup=kb)

@router.callback_query(F.data.startswith("perf:daily:page:"))
async def perf_daily_page(cb: CallbackQuery):
    await cb.answer()
    # perf:daily:page:{page}:{date}
    _, scope, _, page_str, date = cb.data.split(":", maxsplit=4)
    page = int(page_str)
    vendor = await db.get_vendor_by_telegram(cb.from_user.id)
    if not vendor:
        await cb.message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    page_size = 10
    total = await db.count_orders_for_vendor(vendor["id"], date=date)
    pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size

    orders = await db.get_orders_for_vendor(vendor["id"], date=date, limit=page_size, offset=offset)
    if not orders:
        await cb.message.answer("📭 ትዕዛዝ የለም።")
        return

    await cb.message.edit_reply_markup(reply_markup=None)

    for o in orders:
        items = ", ".join(i.get("name","") for i in json.loads(o.get("items_json") or "[]"))
        await cb.message.answer(
            f"📦 ትዕዛዝ #{o['id']} — {o['status']}\n"
            f"🛒 እቃዎች: {items}\n"
            f"💵 ክፍያ: {int(o.get('delivery_fee', 0))} ብር\n"
            f"📍 መድረሻ: {o.get('dropoff','')}"
        )

    kb = paginate_orders_kb(page=page, pages=pages, scope="daily", extra_payload=date)
    await cb.message.answer(f"📄 ገጽ {page}/{pages}", reply_markup=kb)

@router.message(F.text == "📅 የሳምንቱ ትዕዛዞች")
async def performance_week_orders(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    today = datetime.date.today()
    start = today - datetime.timedelta(days=today.weekday())
    end = start + datetime.timedelta(days=6)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # total across range
    async with db._open_connection() as conn:
        async with conn.execute(
            "SELECT COUNT(*) FROM orders WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?",
            (vendor["id"], start_date, end_date)
        ) as cur:
            row = await cur.fetchone()
            total = int(row[0])

    page_size = 10
    pages = max(1, math.ceil(total / page_size))
    orders = await db.get_orders_for_vendor(vendor["id"], limit=page_size, offset=0)  # compact page 1
    if not orders:
        await message.answer("📭 በዚህ ሳምንት ትዕዛዝ የለም።", reply_markup=performance_keyboard())
        return

    await message.answer(
        f"📅 የሳምንቱ ትዕዛዞች\n🗓 ከ{start_date} እስከ {end_date}\n"
        f"📦 ጠቅላላ ትዕዛዞች: {total}"
    )

    for o in orders:
        items = ", ".join(i.get("name","") for i in json.loads(o.get("items_json") or "[]"))
        await message.answer(
            f"📦 ትዕዛዝ #{o['id']} — {o['status']}\n"
            f"🛒 እቃዎች: {items}\n"
            f"💵 ክፍያ: {int(o.get('delivery_fee', 0))} ብር\n"
            f"📍 መድረሻ: {o.get('dropoff','')}"
        )

    payload = f"{start_date}:{end_date}"
    kb = paginate_orders_kb(page=1, pages=pages, scope="weekly", extra_payload=payload)
    await message.answer("📄 ገጽ 1", reply_markup=kb)

@router.callback_query(F.data.startswith("perf:weekly:page:"))
async def perf_weekly_page(cb: CallbackQuery):
    await cb.answer()
    # perf:weekly:page:{page}:{start_date}:{end_date}
    parts = cb.data.split(":")
    page = int(parts[3])
    start_date = parts[4]
    end_date = parts[5]
    vendor = await db.get_vendor_by_telegram(cb.from_user.id)
    if not vendor:
        await cb.message.answer("⚠️ ሱቅ አልተገኘም።")
        return

    async with db._open_connection() as conn:
        async with conn.execute(
            "SELECT COUNT(*) FROM orders WHERE vendor_id = ? AND DATE(created_at) BETWEEN ? AND ?",
            (vendor["id"], start_date, end_date)
        ) as cur:
            row = await cur.fetchone()
            total = int(row[0])

    page_size = 10
    pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size

    # NOTE: weekly pagination currently uses status_filter=None (all)
    orders = await db.get_orders_for_vendor(vendor["id"], limit=page_size, offset=offset)
    await cb.message.edit_reply_markup(reply_markup=None)

    if not orders:
        await cb.message.answer("📭 ትዕዛዝ የለም።")
        return

    for o in orders:
        items = ", ".join(i.get("name","") for i in json.loads(o.get("items_json") or "[]"))
        await cb.message.answer(
            f"📦 ትዕዛዝ #{o['id']} — {o['status']}\n"
            f"🛒 እቃዎች: {items}\n"
            f"💵 ክፍያ: {int(o.get('delivery_fee', 0))} ብር\n"
            f"📍 መድረሻ: {o.get('dropoff','')}"
        )

    payload = f"{start_date}:{end_date}"
    kb = paginate_orders_kb(page=page, pages=pages, scope="weekly", extra_payload=payload)
    await cb.message.answer(f"📄 ገጽ {page}/{pages}", reply_markup=kb)

# -------------------------------------------------
# 👨‍💼 Contact Admin
# -------------------------------------------------
@router.message(F.text == "👨‍💼 አስተዳዳሪን አግኝ")
async def contact_admin(message: Message, bot: Bot):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return
    await message.answer("📞 መልዕክትዎን ይላኩ። አስተዳዳሪ ይደርሳል።")
    # Forward context to admin group when they reply with details
    await notify_admin_log(bot, ADMIN_GROUP_ID, f"📞 Vendor {vendor['name']} requested admin support.")

# -------------------------------------------------
# Optional: Shortcut commands to performance summaries
# -------------------------------------------------
@router.message(Command("vendor_today"))
async def vendor_today_summary(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return
    s = await calc_vendor_day_summary(db.db_path, vendor["id"])
    await message.answer(
        "📊 የዕለቱ ሪፖርት\n"
        f"📦 ትዕዛዞች: {s['delivered'] + s['cancelled']} (✅ {s['delivered']} | ❌ {s['cancelled']})\n"
        f"💵 ገቢ: {int(s['total_payout'])} ብር\n"
        f"⭐ አማካይ ደረጃ: {float(s['rating_avg']):.1f}\n"
        f"⚡ ታማኝነት: {int(s['reliability_pct'])}%"
    )

@router.message(Command("vendor_week"))
async def vendor_week_summary(message: Message):
    vendor = await db.get_vendor_by_telegram(message.from_user.id)
    if not vendor:
        await message.answer("⚠️ ሱቅ አልተገኘም።")
        return
    ws = await calc_vendor_week_summary(db.db_path, vendor["id"])
    await message.answer(
        f"📅 የሳምንቱ ሪፖርት\n"
        f"🗓 ከ{ws['start_date']} እስከ {ws['end_date']}\n"
        f"📦 ትዕዛዞች: {ws['delivered'] + ws['cancelled']} (✅ {ws['delivered']} | ❌ {ws['cancelled']})\n"
        f"💵 ጠቅላላ ገቢ: {int(ws['total_payout'])} ብር"
    )
