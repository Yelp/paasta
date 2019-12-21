from k8s_itests.utils import cmd
from k8s_itests.utils import init_all

terminate_on_exit = []


def setup_module(module):
    # Kill paasta api server on exit
    terminate_on_exit.append(init_all())


def teardown_module(module):
    for p in terminate_on_exit:
        p.kill()


class TestSomething:
    def test_autoscalig(self):
        print("------------------------------------------")
        print("executing kubectl")
        cmd("kubectl get hpa -n paasta", False)
        print("------------------------------------------")
