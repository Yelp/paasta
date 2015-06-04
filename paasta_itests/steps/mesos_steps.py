from behave import given

@given('an unresponsive mesos instance')
def unresponsive_mesos(context):
    """ This is solely here for explicitness. 
     We want to highlight that no setup is done as part of the test, 
     meaning that the mesos cli cannot connect to an instance """
    pass

