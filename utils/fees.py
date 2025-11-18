# utils/fees.py
from config import settings


def round_half_birr(amount: float) -> float:
    """
    Round to nearest 0.50 birr (banker's rounding).
    """
    # Convert to halves
    halves = round(amount * 2)  # banker's rounding via Python round
    return halves / 2.0


def calculate_delivery_fee() -> float:
    """
    Strict delivery fee without distance: SYSTEM_FEE_PER_MEAL only.
    """
    return round_half_birr(float(settings.SYSTEM_FEE_PER_MEAL))


def calculate_vendor_commission(food_subtotal: float) -> float:
    """
    Vendor commission = food_subtotal * CAFE_COMMISSION (rounded to half birr).
    """
    return round_half_birr(food_subtotal * float(settings.CAFE_COMMISSION))


def agent_payout() -> float:
    """
    Agent payout per order = AGENT_BASE_PAY (rounded to half birr).
    """
    return round_half_birr(float(settings.AGENT_BASE_PAY))
