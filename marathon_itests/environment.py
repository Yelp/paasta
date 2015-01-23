import time

def before_all(context):
    # TODO: Maybe be smarter and unblock when the marathon port is open?
    print "Sleeping 10 seconds to allow marathon to start up."
    time.sleep(10)
