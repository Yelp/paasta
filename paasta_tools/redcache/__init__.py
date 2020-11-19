import hashlib
import pickle
import time
from functools import wraps

from redis.client import Redis

cache_server: Redis = None


def digest(hasher, data):
    if isinstance(data, (str, int, float)):
        hasher.update(str(data).encode("utf-8"))
    elif isinstance(data, dict):
        for key, val in data.items():
            hasher.update(str(key).encode("utf-8"))
            digest(hasher, val)
    elif "__iter__" in dir(data):
        for item in data:
            digest(hasher, item)
    else:
        raise f"can't digest {type(data)}: {data!r}"


def d(resolution=60, selector=lambda *args, **kwds: (args, kwds)):
    def dd(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            if cache_server is None:
                return f(*args, **kwds)

            hasher = hashlib.md5()
            digest(hasher, selector(*args, **kwds))
            data_hash = hasher.hexdigest()
            ts = int(time.time())
            ts_res = ts - ts % resolution
            key = f"{f.__module__}.{f.__name__}:{ts_res}:{data_hash}"

            # check if key exists
            exists = cache_server.get(key)
            if exists is None:
                # try grabbing a lock for the key
                if cache_server.set(key, pickle.dumps(dict(ts=ts)), nx=True):
                    val = f(*args, **kwds)
                    # set the value for others
                    cache_server.set(key, pickle.dumps(dict(ts=ts, val=val)))
                    return val
                else:
                    # re-fetch the key locked by somebody else
                    exists = cache_server.get(key)

            # re-fetch in a loop until `val` shows up, but only wait
            # for 20% of resolution time
            while time.time() < ts + resolution / 5.0:
                exists = pickle.loads(exists)
                if "val" in exists:
                    return exists["val"]
                time.sleep(0.5)
                exists = cache_server.get(key)

            # key we're looking for is already stale
            return f(*args, **kwds)

        return wrapper

    return dd


def setup():
    global cache_server
    cache_server = Redis(host="dev54-uswest1adevc.dev.yelpcorp.com", port=6379)
