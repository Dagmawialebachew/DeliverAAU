"""
Localization module for bilingual support (English + Amharic).
"""

from typing import Dict, Any

MESSAGES: Dict[str, Dict[str, str]] = {
    "en": {
        # Welcome & Onboarding
        "welcome": "🎉 Welcome to **Deliver AAU**!\n\nYour campus delivery companion 📦\n\nLet's get you started! 🚀",
        "select_language": "🌍 Please select your preferred language:",
        "language_set": "✅ Language set to English!",
        "phone_request": "📱 Please share your phone number to continue.\n\nWe'll use this to coordinate deliveries.",
        "phone_received": "✅ Got it! Phone: {phone}",
        "campus_select": "🏫 Select your campus:",
        "registration_success": "🎊 Registration Complete!\n\n👤 Name: {name}\n📱 Phone: {phone}\n🏫 Campus: {campus}\n\nYou earned **50 XP** and **10 Coins**! 🎁",

        # Main Menu
        "main_menu": "🏠 **Main Menu**\n\nWhat would you like to do today?",
        "request_delivery": "📦 Request Delivery",
        "track_order": "🚴‍♂️ Track Order",
        "my_coins": "💰 My Coins",
        "leaderboard": "🏆 Leaderboard",
        "settings": "🛠 Settings",

        # Delivery Flow
        "pickup_location": "📍 Enter pickup location:",
        "dropoff_location": "📍 Enter drop-off location:",
        "item_description": "📝 Describe the item(s):",
        "delivery_confirm": "✅ Confirm your delivery request:\n\n📍 From: {pickup}\n📍 To: {dropoff}\n📦 Item: {item}\n\nConfirm?",
        "delivery_created": "🎉 Delivery request created!\n\n🆔 Order ID: #{order_id}\n⏳ Status: Pending\n\nWe'll notify you when a courier accepts! 📲",
        "no_active_orders": "📭 You have no active orders.",
        "order_status": "📦 **Order #{order_id}**\n\n{status}\n\n📍 From: {pickup}\n📍 To: {dropoff}\n📦 Item: {item}\n\n🕐 Created: {created}",

        # Coins & Gamification
        "coins_balance": "💰 **Your Balance**\n\n🪙 Coins: {coins}\n⭐ XP: {xp}\n🏅 Level: {level}\n\nKeep delivering to earn more! 🚀",
        "leaderboard_title": "🏆 **Top Deliverers**\n\n",
        "leaderboard_entry": "{rank}. {name} - {xp} XP | {coins} 🪙\n",
        "leaderboard_empty": "No rankings yet. Be the first! 🥇",

        # Rating
        "rate_delivery": "⭐ Rate your delivery experience:",
        "rating_thanks": "🙏 Thank you for your feedback!\n\nYou earned **5 XP** and **2 Coins**! 🎁",

        # Settings
        "settings_menu": "🛠 **Settings**\n\nManage your preferences:",
        "change_language": "🌍 Change Language",
        "change_campus": "🏫 Change Campus",
        "view_profile": "👤 View Profile",
        "profile_info": "👤 **Your Profile**\n\n📛 Name: {name}\n📱 Phone: {phone}\n🏫 Campus: {campus}\n🪙 Coins: {coins}\n⭐ XP: {xp}\n🏅 Level: {level}",

        # Buttons
        "btn_confirm": "✅ Confirm",
        "btn_cancel": "❌ Cancel",
        "btn_back": "⬅️ Back",

        # Errors & Fallback
        "invalid_input": "❌ Invalid input. Please use the menu buttons.",
        "error_generic": "❌ Something went wrong. Please try again.",
        "throttle_warning": "⚠️ Slow down! Please wait a moment."
    },

    "am": {
        # Welcome & Onboarding
        "welcome": "🎉 እንኳን ወደ **Deliver AAU** በደህና መጡ!\n\nየካምፓስ ማድረሻ አጋርዎ 📦\n\nእንጀምር! 🚀",
        "select_language": "🌍 እባክዎን ቋንቋዎን ይምረጡ:",
        "language_set": "✅ ቋንቋ ወደ አማርኛ ተቀየረ!",
        "phone_request": "📱 እባክዎን ስልክ ቁጥርዎን ያጋሩ።\n\nይህንን ለማድረሻ ማስተባበር እንጠቀማለን።",
        "phone_received": "✅ ደርሰናል! ስልክ: {phone}",
        "campus_select": "🏫 ካምፓስዎን ይምረጡ:",
        "registration_success": "🎊 ምዝገባ ተጠናቋል!\n\n👤 ስም: {name}\n📱 ስልክ: {phone}\n🏫 ካምፓስ: {campus}\n\n**50 XP** እና **10 ሳንቲም** አገኙ! 🎁",

        # Main Menu
        "main_menu": "🏠 **ዋና ምናሌ**\n\nዛ��ዕ ምን ማድረግ ይፈልጋሉ?",
        "request_delivery": "📦 ማድረሻ ጠይቅ",
        "track_order": "🚴‍♂️ ትዕዛዝ ተከታተል",
        "my_coins": "💰 የእኔ ሳንቲሞች",
        "leaderboard": "🏆 የመሪዎች ሰሌዳ",
        "settings": "🛠 ቅንብሮች",

        # Delivery Flow
        "pickup_location": "📍 የመውሰጃ ቦታ ያስገቡ:",
        "dropoff_location": "📍 የማድረሻ ቦታ ያስገቡ:",
        "item_description": "📝 ዕቃውን ይግለጹ:",
        "delivery_confirm": "✅ የማድረሻ ጥያቄዎን ያረጋግጡ:\n\n📍 ከ: {pickup}\n📍 ወደ: {dropoff}\n📦 ዕቃ: {item}\n\nማረጋገጥ?",
        "delivery_created": "🎉 የማድረሻ ጥያቄ ተፈጥሯል!\n\n🆔 የትዕዛዝ መታወቂያ: #{order_id}\n⏳ ሁኔታ: በመጠባበቅ ላይ\n\nኩሪየር ሲቀበል እናሳውቅዎታለን! 📲",
        "no_active_orders": "📭 ንቁ ትዕዛዞች የሉዎትም።",
        "order_status": "📦 **ትዕዛዝ #{order_id}**\n\n{status}\n\n📍 ከ: {pickup}\n📍 ወደ: {dropoff}\n📦 ዕቃ: {item}\n\n🕐 ተፈጥሯል: {created}",

        # Coins & Gamification
        "coins_balance": "💰 **የእርስዎ ሂሳብ**\n\n🪙 ሳንቲሞች: {coins}\n⭐ XP: {xp}\n🏅 ደረጃ: {level}\n\nበማድረስ ይቀጥሉ! 🚀",
        "leaderboard_title": "🏆 **ምርጥ አድራሾች**\n\n",
        "leaderboard_entry": "{rank}. {name} - {xp} XP | {coins} 🪙\n",
        "leaderboard_empty": "ገና ደረጃ የለም። የመጀመሪያው ይሁኑ! 🥇",

        # Rating
        "rate_delivery": "⭐ የማድረሻ ተሞክሮዎን ይገምግሙ:",
        "rating_thanks": "🙏 ለግብረ መልስዎ እናመሰግናለን!\n\n**5 XP** እና **2 ሳንቲም** አገኙ! 🎁",

        # Settings
        "settings_menu": "🛠 **ቅንብሮች**\n\nምርጫዎችዎን ያስተዳድሩ:",
        "change_language": "🌍 ቋንቋ ቀይር",
        "change_campus": "🏫 ካምፓስ ቀይር",
        "view_profile": "👤 መገለጫ አሳይ",
        "profile_info": "👤 **የእርስዎ መገለጫ**\n\n📛 ስም: {name}\n📱 ስልክ: {phone}\n🏫 ካምፓስ: {campus}\n🪙 ሳንቲሞች: {coins}\n⭐ XP: {xp}\n🏅 ደረጃ: {level}",

        # Buttons
        "btn_confirm": "✅ አረጋግጥ",
        "btn_cancel": "❌ ሰርዝ",
        "btn_back": "⬅️ ተመለስ",

        # Errors & Fallback
        "invalid_input": "❌ ልክ ያልሆነ ግቤት። እባክዎን የምናሌ አዝራሮችን ይጠቀሙ።",
        "error_generic": "❌ የሆነ ችግር ተፈጥሯል። እባክዎን እንደገና ይሞክሩ።",
        "throttle_warning": "⚠️ ዝግታ ይውረዱ! እባክዎን ትንሽ ይጠብቁ።"
    }
}


def get_text(lang: str, key: str, **kwargs: Any) -> str:
    """
    Get localized text by language and key.

    Args:
        lang: Language code ('en' or 'am')
        key: Message key
        **kwargs: Format arguments

    Returns:
        Formatted localized string
    """
    lang = lang if lang in MESSAGES else "en"
    text = MESSAGES[lang].get(key, MESSAGES["en"].get(key, key))

    if kwargs:
        try:
            return text.format(**kwargs)
        except KeyError:
            return text
    return text
