import gzip

import arrow
import jsonpickle
import simplejson as json
from sortedcontainers import SortedDict


def _python_encode(obj):
    return json.loads(jsonpickle.encode(obj))


def _python_decode(obj):
    return jsonpickle.decode(json.dumps(obj))


class ArrowSerializer(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data['timestamp'] = obj.timestamp
        return data

    def restore(self, data):
        return arrow.get(data['timestamp'])


class SortedDictSerializer(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data['items'] = [(_python_encode(k), _python_encode(v)) for k, v in obj.items()]
        return data

    def restore(self, data):
        return SortedDict((_python_decode(k), _python_decode(v)) for k, v in data['items'])


def _register_handlers():
    # These operations are idempotent, it's safe to do more than once
    jsonpickle.handlers.register(arrow.Arrow, ArrowSerializer)
    jsonpickle.handlers.register(SortedDict, SortedDictSerializer)


def read_object_from_compressed_json(filename, raw_timestamps=False):
    """ Read a Python object from a gzipped JSON file """
    _register_handlers()
    with gzip.open(filename) as f:
        if raw_timestamps:
            old_arrow = arrow.get
            arrow.get = int
        data = jsonpickle.decode(f.read().decode())
        if raw_timestamps:
            arrow.get = old_arrow
        return data


def write_object_to_compressed_json(obj, filename):
    """ Write the Python object to a compressed (gzipped) JSON file

    :param obj: a Python object to serialize
    :param filename: the file to write to
    """
    _register_handlers()
    with gzip.open(filename, 'w') as f:
        f.write(jsonpickle.encode(obj).encode())
