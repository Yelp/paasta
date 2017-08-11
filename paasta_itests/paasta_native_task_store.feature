Feature: ZKTaskStore
  Scenario: ZKTaskStore smoke test
    Given a working paasta cluster, with docker registry docker.io
      And a ZKTaskStore
     Then get_all_tasks should return {}
     When we overwrite_task with task_id "foo" and params {"health": "healthy"}
     Then get_all_tasks should return {"foo": {"health": "healthy"}}
     When we overwrite_task with task_id "foo" and params {"health": "unhealthy"}
     Then get_all_tasks should return {"foo": {"health": "unhealthy"}}
     When we overwrite_task with task_id "bar" and params {"health": "healthy"}
     Then get_all_tasks should return {"foo": {"health": "unhealthy"}, "bar": {"health": "healthy"}}
