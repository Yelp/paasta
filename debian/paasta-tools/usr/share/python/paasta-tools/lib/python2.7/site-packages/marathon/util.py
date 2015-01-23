import collections
import datetime
import types

try:
    import json
except ImportError:
    import simplejson as json
import re


class MarathonJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for Marathon object serialization."""

    def default(self, obj):
        if hasattr(obj, 'json_repr'):
            return self.default(obj.json_repr())

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        if isinstance(obj, collections.Iterable) and not isinstance(obj, types.StringTypes):
            if hasattr(obj, 'iteritems'):
                return {k: self.default(v) for k,v in obj.iteritems()}
            else:
                return [self.default(e) for e in obj]

        return obj


class MarathonMinimalJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder for Marathon object serialization."""

    def default(self, obj):
        if hasattr(obj, 'json_repr'):
            return self.default(obj.json_repr(minimal=True))

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        if isinstance(obj, collections.Iterable) and not isinstance(obj, types.StringTypes):
            if hasattr(obj, 'iteritems'):
                return {k: self.default(v) for k,v in obj.iteritems() if (v or v == False)}
            else:
                return [self.default(e) for e in obj if (e or e == False)]

        return obj


def to_camel_case(snake_str):
    words = snake_str.split('_')
    return words[0] + ''.join(w.title() for w in words[1:])


def to_snake_case(camel_str):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()