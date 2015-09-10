import mock


from paasta_tools import cleanup_chronos_jobs


def test_cleanup_jobs():
    chronos_client = mock.Mock()
    returns = [None, None, Exception('boom')]

    def side_effect(*args):
        result = returns.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    chronos_client.delete = mock.Mock(side_effect=side_effect)
    result = cleanup_chronos_jobs.cleanup_jobs(chronos_client, ['foo', 'bar', 'baz'])

    # I'd like to just compare the lists, but you can't compare exception objects.
    print result
    assert result[0] == ('foo', None)
    assert result[1] == ('bar', None)
    assert result[2][0] == 'baz'
    assert isinstance(result[2][1], Exception)


def test_jobs_to_delete():
    configured_jobs = [('service1', 'job1'), ('service1', 'job2')]
    deployed_jobs = [('service1', 'job1', 'config'),  ('service1', 'job2', 'config')]
    assert cleanup_chronos_jobs.jobs_to_delete(configured_jobs, deployed_jobs) == []


def test_jobs_to_delete_unknown_job():
    configured_jobs = [('service1', 'job1'), ('service1', 'job2'), ('service1', 'job3')]
    deployed_jobs = [('service1', 'job1', 'config'), ('service1', 'job2', 'config'),
                   ('service1', 'job3', 'config'), ('service1', 'job5', 'config')]
    assert cleanup_chronos_jobs.jobs_to_delete(configured_jobs, deployed_jobs) == [('service1', 'job5', 'config')]


def test_format_list_output():
    assert cleanup_chronos_jobs.format_list_output("Successfully Removed:", ['foo', 'bar', 'baz']) \
        == "Successfully Removed:\n  foo\n  bar\n  baz"


def test_deployed_job_names():
    mock_client = mock.Mock()
    mock_client.list.return_value = [{'name': 'foo', 'blah': 'blah'}, {'name': 'bar', 'blah': 'blah'}]
    assert cleanup_chronos_jobs.deployed_job_names(mock_client) == ['foo', 'bar']
