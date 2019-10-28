import behave
from hamcrest import assert_that


@behave.then('the log should contain "(?P<log_line>.*)"')
def check_log(context, log_line):
    assert_that(context.log_capture.find_event(log_line))
