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
    chronos_client.delete_job = mock.Mock(side_effect=side_effect)
    result = cleanup_chronos_jobs.cleanup_jobs(chronos_client, ['foo', 'bar', 'baz'])
    ## I'd like to just compare the lists, but you can't compare exception objects.
    assert result[0] == ('foo', None)
    assert result[1] == ('bar', None)
    assert result[2][0] == 'baz'
    assert isinstance(result[2][1], Exception)
