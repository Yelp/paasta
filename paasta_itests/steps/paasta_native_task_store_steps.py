import json

from behave import given
from behave import then
from behave import when

from paasta_tools.frameworks.task_store import MesosTaskParameters
from paasta_tools.frameworks.task_store import ZKTaskStore


@given("a ZKTaskStore")
def given_a_zktaskstore(context):
    context.task_store = ZKTaskStore(
        service_name="service",
        instance_name="instance",
        system_paasta_config=context.system_paasta_config,
        framework_id="testing_zk_task_store",
    )

    # clean up any old data
    for path in context.task_store.zk_client.get_children("/"):
        context.task_store.zk_client.delete(path)


@then("get_all_tasks should return {return_json}")
def then_get_all_tasks_should_return(context, return_json):
    all_tasks = context.task_store.get_all_tasks()
    expected_tasks = {
        k: MesosTaskParameters(**v) for k, v in json.loads(return_json).items()
    }
    assert all_tasks == expected_tasks


@when('we overwrite_task with task_id "{task_id}" and params {params_json}')
def when_we_overwrite_task(context, task_id, params_json):
    context.task_store.overwrite_task(
        task_id, MesosTaskParameters(**json.loads(params_json))
    )
