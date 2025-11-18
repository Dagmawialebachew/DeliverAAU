# config.py
import os
from dataclasses import dataclass, field
import random
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def env_list(key: str) -> list[int]:
    val = os.getenv(key, "")
    return [int(x.strip()) for x in val.split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: list[int] = field(default_factory=lambda: env_list("ADMIN_IDS"))
    DB_PATH: str = os.getenv("DB_PATH", "./data/deliver_aau.db")
    MEDIA_ROOT: str = os.getenv("MEDIA_ROOT", "./media")

    # Strict fees / rules (no distance math)
    AGENT_BASE_PAY: int = int(os.getenv("AGENT_BASE_PAY", "25"))
    SYSTEM_FEE_PER_MEAL: int = int(os.getenv("SYSTEM_FEE_PER_MEAL", "20"))
    CAFE_COMMISSION: float = float(os.getenv("CAFE_COMMISSION", "0.10"))

    # Gamification (defaults; editable later via admin flows)
    STUDENT_POINTS_PER_ORDER: int = int(os.getenv("STUDENT_POINTS_PER_ORDER", "10"))
    DRIVER_POINTS_PER_ORDER: int = int(os.getenv("DRIVER_POINTS_PER_ORDER", "12"))
    STREAK_BONUS_PER_DAY: int = int(os.getenv("STREAK_BONUS_PER_DAY", "3"))

    # Tracking intervals (seconds) for walk/run deliveries
    LOCATION_PING_INTERVAL_ACTIVE: int = int(os.getenv("LOCATION_PING_INTERVAL_ACTIVE", "15"))
    LOCATION_PING_INTERVAL_IDLE: int = int(os.getenv("LOCATION_PING_INTERVAL_IDLE", "90"))
    ADMIN_GROUP_ID: int = int(os.getenv("ADMIN_GROUP_ID", "0"))


    # Subscriptions
    SUBSCRIPTIONS_ENABLED: bool = os.getenv("SUBSCRIPTIONS_ENABLED", "true").lower() == "true"
    SUBSCRIPTION_MONTHLY_PRICE: int = int(os.getenv("SUBSCRIPTION_MONTHLY_PRICE", "299"))
    SUBSCRIPTION_DAILY_PRICE: int = int(os.getenv("SUBSCRIPTION_DAILY_PRICE", "25"))

    # Campuses (fixed)
    CAMPUSES: list[str] = field(default_factory=lambda: ["AK4", "AK5", "AK6"])
    
    VENDOR_IDS = {
    "Tg house": int(os.getenv("VENDOR_ID_TG_HOUSE", random.randint(100000000, 999999999))),
    "Abudabi": int(os.getenv("VENDOR_ID_ABUDABI", random.randint(100000000, 999999999))),
    # others will be random
}

    # Delivery guys
    DG_IDS = {
        "Dagmawi": int(os.getenv("DG_ID_DAGMAWI", random.randint(100000000, 999999999))),
        "Muktar": int(os.getenv("DG_ID_MUKTAR", random.randint(100000000, 999999999))),
        "Yonatan": int(os.getenv("DG_ID_YONATAN", random.randint(100000000, 999999999))),
        # others will be random
    }



settings = Settings()

# Ensure folders exist
Path(settings.MEDIA_ROOT).mkdir(parents=True, exist_ok=True)
Path("./data").mkdir(parents=True, exist_ok=True)
