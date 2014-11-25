def check():
    """
    Return true if deploy.yaml is present
    """
    if yaml_check():
        return True, "Found deploy.yaml"
    else:
        return False, "Missing deploy.yaml file"


def yaml_check():
    return False
