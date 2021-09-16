from contextlib import contextmanager
import json
import time


@contextmanager
def fake_zipkin(name):
    start = time.time()
    yield
    end = time.time()
    print(
        json.dumps(
            {"span": name, "start": start, "end": end}
        )
    )
