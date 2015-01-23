import json

from marathon.util import to_camel_case, to_snake_case, MarathonJsonEncoder, MarathonMinimalJsonEncoder


class MarathonObject(object):
    """Base Marathon object."""

    def __repr__(self):
        return "{clazz}::{obj}".format(clazz=self.__class__.__name__, obj=self.to_json())

    def json_repr(self, minimal=False):
        """Construct a JSON-friendly representation of the object.

        :param bool minimal: Construct a minimal representation of the object (ignore nulls and empty collections)

        :rtype: dict
        """
        if minimal:
            return {to_camel_case(k):v for k,v in vars(self).iteritems() if (v or v == False)}
        else:
            return {to_camel_case(k):v for k,v in vars(self).iteritems()}

    @classmethod
    def from_json(cls, attributes):
        """Construct an object from a parsed response.

        :param dict attributes: object attributes from parsed response
        """
        return cls(**{to_snake_case(k): v for k,v in attributes.iteritems()})

    def to_json(self, minimal=False):
        """Encode an object as a JSON string.

        :param bool minimal: Construct a minimal representation of the object (ignore nulls and empty collections)

        :rtype: str
        """
        if minimal:
            return json.dumps(self.json_repr(minimal=True), cls=MarathonMinimalJsonEncoder, sort_keys=True)
        else:
            return json.dumps(self.json_repr(), cls=MarathonJsonEncoder, sort_keys=True)



class MarathonResource(MarathonObject):
    """Base Marathon resource."""

    def __repr__(self):
        if 'id' in vars(self).keys():
            return "{clazz}::{id}".format(clazz=self.__class__.__name__, id=self.id)
        else:
            return "{clazz}::{obj}".format(clazz=self.__class__.__name__, obj=self.to_json())



