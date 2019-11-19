# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from decimal import Decimal
from decimal import getcontext
from decimal import localcontext
from decimal import ROUND_HALF_UP

MAX_DECIMAL_PLACES = 20
_PLACES_VALUE = Decimal(10) ** (-1 * MAX_DECIMAL_PLACES)


def convert_decimal(numeric):
    full_decimal = Decimal(numeric)
    _, digits, exponent = full_decimal.as_tuple()
    # Round to MAX_DECIMAL_PLACES, if result has more places than that.
    if exponent < -MAX_DECIMAL_PLACES:
        # quantize can raise `decimal.InvalidOperation` if result is greater
        # than context precision, which is 28 by default. to get around this,
        # temporarily set a new precision up to the max number of sig figs  of
        # `full_decimal`, which is also the max for the result of `quantize`.
        # this ensures that the result of `quantize` will be within the precision
        # limit, and not raise the error.
        with localcontext() as ctx:
            ctx.prec = max(len(digits), getcontext().prec)
            return full_decimal.quantize(_PLACES_VALUE, rounding=ROUND_HALF_UP)
    else:
        return full_decimal
