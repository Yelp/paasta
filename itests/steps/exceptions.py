import behave
from hamcrest import assert_that
from hamcrest import equal_to


@behave.then('a (?P<error_type>.*Error|.*Exception) is raised')
def check_exception(context, error_type):
    assert_that(type(context.exception).__name__, equal_to(error_type))
