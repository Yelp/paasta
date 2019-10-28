import moto
import pytest

from clusterman.aws.client import ec2


@pytest.fixture(autouse=True)
def setup_ec2():
    mock_ec2_obj = moto.mock_ec2()
    mock_ec2_obj.start()
    yield
    mock_ec2_obj.stop()


@pytest.fixture(autouse=True)
def setup_autoscaling():
    mock_autoscaling_obj = moto.mock_autoscaling()
    mock_autoscaling_obj.start()
    yield
    mock_autoscaling_obj.stop()


@pytest.fixture
def mock_subnet():
    vpc_response = ec2.create_vpc(CidrBlock='10.0.0.0/24')
    return ec2.create_subnet(
        CidrBlock='10.0.0.0/24',
        VpcId=vpc_response['Vpc']['VpcId'],
        AvailabilityZone='us-west-2a'
    )
