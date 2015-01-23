from ..exceptions import InvalidChoiceError
from .base import MarathonObject


class MarathonConstraint(MarathonObject):
    """Marathon placement constraint.

    See https://mesosphere.github.io/marathon/docs/constraints.html

    :param str field: constraint operator target
    :param str operator: must be one of [UNIQUE, CLUSTER, GROUP_BY, LIKE, UNLIKE]
    :param value: [optional] if `operator` is CLUSTER, constrain tasks to servers where `field` == `value`.
    If `operator` is GROUP_BY, place at most `value` tasks per group. If `operator`
    is `LIKE` or `UNLIKE`, filter servers using regexp.
    :type value: str, int, or None
    """

    OPERATORS = ['UNIQUE', 'CLUSTER', 'GROUP_BY', 'LIKE', 'UNLIKE']
    """Valid operators"""

    def __init__(self, field, operator, value=None):
        if not operator in self.OPERATORS:
            raise InvalidChoiceError('operator', operator, self.OPERATORS)
        self.field = field
        self.operator = operator
        self.value = value

    def __repr__(self):
        if self.value:
            template = "MarathonConstraint::{field}:{operator}:{value}"
        else:
            template = "MarathonConstraint::{field}:{operator}"
        return template.format(**self.__dict__)

    def json_repr(self, minimal=False):
        """Construct a JSON-friendly representation of the object.

        :param bool minimal: [ignored]

        :rtype: list
        """
        if self.value:
            return [self.field, self.operator, self.value]
        else:
            return [self.field, self.operator]

    @classmethod
    def from_json(cls, obj):
        """Construct a MarathonConstraint from a parsed response.

        :param dict attributes: object attributes from parsed response

        :rtype: :class:`MarathonConstraint`
        """
        if len(obj) == 2:
            (field, operator) = obj
            return cls(field, operator)
        if len(obj) > 2:
            (field, operator, value) = obj
            return cls(field, operator, value)
