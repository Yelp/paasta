from typing import Callable

from pyramid.request import Request
from pyramid.response import Response

Handler = Callable[[Request], Response]
