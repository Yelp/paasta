def check():
    """
    Return true if dockerfile is present
    """
    if docker_check():
        return True, "Dockerfile present"
    else:
        return False, "No dockerfile present"


def docker_check():
    # TODO: write logic
    return False
