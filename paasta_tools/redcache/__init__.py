import pickle
import time
from functools import wraps

from redis.client import Redis

cache_server: Redis = None


def hashable(data):
    if isinstance(data, dict):
        return frozenset((key, hashable(val)) for key, val in data.items())
    elif "__iter__" in dir(data):
        return tuple(hashable(item) for item in data)
    elif "__hash__" in dir(data):
        return data
    else:
        raise f"{type(data)}({data!r}) is not hashable"


def d(res=60, selector=lambda *args, **kwds: (args, kwds)):
    def dd(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            if cache_server is None:
                return f(*args, **kwds)

            data_hash = hash(hashable(selector(args, kwds)))
            ts = int(time.time())
            ts_res = ts - ts % res
            key = f"{f.__module__}.{f.__name__}:{ts_res}:{data_hash}"

            # check if key exists
            exists = cache_server.get(key)
            if exists is None:
                # try grabbing a lock for the key
                if cache_server.set(key, pickle.dumps(dict(ts=ts)), nx=True):
                    print("updating cache")
                    val = f(*args, **kwds)
                    # set the value for others
                    cache_server.set(key, pickle.dumps(dict(ts=ts, val=val)))
                    return val
                else:
                    # re-fetch the key locked by somebody else
                    exists = cache_server.get(key)

            # re-fetch in a loop until `val` shows up
            while time.time() < ts_res + res:
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
    cache_server = Redis(host="localhost", port=6379)
