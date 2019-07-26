import time
from typing import Callable


def tail_newrelic(query: str, lookback_seconds: float, callback: Callable, sfx_api_token: str) -> None:
    # The callback will check the new relic API for alerts
    while True:
        print("Polling New relic")
        callback({}, {}, time.time())
        time.sleep(5)
