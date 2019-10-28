from decimal import Decimal
from decimal import ROUND_HALF_UP

MAX_DECIMAL_PLACES = 20
_PLACES_VALUE = Decimal(10) ** (-1 * MAX_DECIMAL_PLACES)


def convert_decimal(numeric):
    full_decimal = Decimal(numeric)
    _, _, exponent = full_decimal.as_tuple()
    # Round to MAX_DECIMAL_PLACES, if result has more places than that.
    if exponent < -MAX_DECIMAL_PLACES:
        return full_decimal.quantize(_PLACES_VALUE, rounding=ROUND_HALF_UP)
    else:
        return full_decimal
