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
    ## I'd like to just compare the lists, but you can't compare exception objects.
    print result
    assert result[0] == ('foo', None)
    assert result[1] == ('bar', None)
    assert result[2][0] == 'baz'
    assert isinstance(result[2][1], Exception)

def test_jobs_to_delete():
    expected_jobs = ['job_1', 'job_2']
    actual_jobs = ['job_1', 'job_2', 'job_3']
    assert cleanup_chronos_jobs.jobs_to_delete(expected_jobs, actual_jobs) == ['job_3']

def test_format_list_output():
    assert cleanup_chronos_jobs.format_list_output("Successfully Removed:", ['foo', 'bar', 'baz']) == "Successfully Removed:\n  foo\n  bar\n  baz"

def test_running_job_names():
    mock_client = mock.Mock()
    mock_client.list.return_value = [{ 'name': 'foo', 'blah': 'blah'},{ 'name': 'bar', 'blah': 'blah'}]
    assert cleanup_chronos_jobs.running_job_names(mock_client) == ['foo', 'bar']

def test_expected_job_names():
    service_job_pairs = [('myservice', 'myjob1'), ('myservice', 'myjob2')]
    assert cleanup_chronos_jobs.expected_job_names(service_job_pairs) == ['myjob1', 'myjob2']
