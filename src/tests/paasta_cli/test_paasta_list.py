from service_deployment_tools.tests.test_marathon_tools \
    import test_get_marathon_services_for_cluster as get_services_test


def test_get_service():
    # TODO: Replace with unique test when get_services returns more than
    #  marathon services
    get_services_test()