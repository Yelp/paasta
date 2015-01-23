import logging

from .client import MarathonClient
from .models import MarathonResource, MarathonApp, MarathonTask, MarathonConstraint
from .exceptions import MarathonError, MarathonHttpError, NotFoundError, InvalidChoiceError

log = logging.getLogger(__name__)
logging.basicConfig()