import pytest

from paasta_tools.contrib.rightsizer_soaconfigs_update import (
    get_recommendations_by_service_file,
)


def _make_result(service, cluster, instance, **extra_fields):
    return {
        "service": service,
        "cluster": cluster,
        "instance": instance,
        **extra_fields,
    }


@pytest.fixture
def cassandra_keys():
    return ["cpus", "mem", "disk", "replicas", "cpu_burst_percent"]


class TestGetRecommendationsByServiceFile:
    def test_cassandracluster_produces_cassandra_recommendation(self, cassandra_keys):
        results = {
            "0": _make_result(
                service="myservice",
                cluster="cassandracluster-norcal-devc",
                instance="main",
                cpus="4.0",
                mem="8192",
                disk="50000",
                replicas="3",
                cpu_burst_percent="200",
            ),
        }
        recs = get_recommendations_by_service_file(results, cassandra_keys, set())
        key = ("myservice", "cassandracluster-norcal-devc")
        assert key in recs
        assert recs[key]["main"] == {
            "cpus": 4.0,
            "mem": "8192",
            "disk": "50000",
            "replicas": 3,
            "cpu_burst_percent": 200.0,
        }

    def test_cassandraclustereks_produces_cassandra_recommendation(
        self, cassandra_keys
    ):
        results = {
            "0": _make_result(
                service="myservice",
                cluster="cassandraclustereks-norcal-devc",
                instance="main",
                cpus="4.0",
                mem="8192",
                disk="50000",
                replicas="3",
                cpu_burst_percent="200",
            ),
        }
        recs = get_recommendations_by_service_file(results, cassandra_keys, set())
        key = ("myservice", "cassandraclustereks-norcal-devc")
        assert key in recs
        assert recs[key]["main"] == {
            "cpus": 4.0,
            "mem": "8192",
            "disk": "50000",
            "replicas": 3,
            "cpu_burst_percent": 200.0,
        }

    def test_kubernetes_produces_kubernetes_recommendation(self):
        results = {
            "0": _make_result(
                service="myservice",
                cluster="kubernetes-norcal-devc",
                instance="main",
                cpus="2.0",
                mem="1024",
                disk="512",
            ),
        }
        keys = ["cpus", "mem", "disk"]
        recs = get_recommendations_by_service_file(results, keys, set())
        key = ("myservice", "kubernetes-norcal-devc")
        assert key in recs
        assert recs[key]["main"] == {
            "cpus": 2.0,
            "mem": 1024,
            "disk": 512,
        }

    def test_excluded_cluster_is_skipped(self, cassandra_keys):
        results = {
            "0": _make_result(
                service="myservice",
                cluster="cassandraclustereks-norcal-devc",
                instance="main",
                cpus="4.0",
            ),
        }
        recs = get_recommendations_by_service_file(
            results, cassandra_keys, {"cassandraclustereks-norcal-devc"}
        )
        assert len(recs) == 0

    def test_unknown_instance_type_skipped(self):
        results = {
            "0": _make_result(
                service="myservice",
                cluster="flink-norcal-devc",
                instance="main",
                cpus="2.0",
            ),
        }
        recs = get_recommendations_by_service_file(results, ["cpus"], set())
        assert len(recs) == 0
