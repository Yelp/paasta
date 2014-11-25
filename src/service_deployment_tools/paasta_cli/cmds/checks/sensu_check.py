def check():
    """
    Return true if service is using sensu monitoring.yaml
    """
    if sensu_check():
        return True, "Is using sensu monitoring.yaml"
    else:
        return False, "Not utilizing sensu monitoring"


def sensu_check():
    # TODO: write logic
    return True
