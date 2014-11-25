def check():
    """
    Return the port number if service is running in smartstack, else False
    """
    if smartstack_check():
        return True, "Is in smartstack"
    else:
        return False, "Not in smartstack"


def smartstack_check():
    # TODO: write logic
    return True
