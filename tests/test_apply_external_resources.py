import os
from subprocess import CalledProcessError

import mock
import pytest

from paasta_tools.apply_external_resources import main


@pytest.fixture
def mock_run():
    with mock.patch(
        "paasta_tools.apply_external_resources.run", autospec=True
    ) as mock_runner:
        yield mock_runner


@pytest.fixture(autouse=True)
def setup_external_files(fs):
    fs.create_file(
        "/external_resources/00-common/10-foo/10-deployment.yaml",
        contents="foo: bar",
    )
    fs.create_file(
        "/external_resources/00-common/10-foo/20-service.yaml",
        contents="fizz: buzz",
    )
    fs.create_file(
        "/external_resources/20-common/10-foo/20-deployment.yaml",
        contents="baz: biz",
    )
    fs.create_file(
        "/external_resources/.applied/00-common/10-foo/10-deployment.yaml",
        contents="foo: bar",
    )
    fs.create_file(
        "/external_resources/.applied/00-common/10-foo/20-service.yaml",
        contents="fizz: buzz",
    )
    fs.create_file(
        "/external_resources/.applied/20-common/10-foo/20-deployment.yaml",
        contents="baz: biz",
    )


def test_no_changes(mock_run):
    assert main("/external_resources") == 0
    assert mock_run.call_count == 0


def test_resources_added_in_order(mock_run, fs):
    fs.create_file(
        "/external_resources/00-common/10-foo/30-hpa.yaml",
        contents="blah: blah",
    )
    fs.create_file(
        "/external_resources/00-common/10-foo/40-service.yaml",
        contents="blah: blah",
    )
    fs.create_file(
        "/external_resources/00-common/30-foo/10-deployment.yaml",
        contents="blah: blah",
    )
    assert main("/external_resources") == 0
    assert mock_run.call_args_list == [
        mock.call(
            [
                "kubectl",
                "apply",
                "-f",
                "/external_resources/00-common/10-foo/30-hpa.yaml",
            ],
            check=True,
        ),
        mock.call(
            [
                "kubectl",
                "apply",
                "-f",
                "/external_resources/00-common/10-foo/40-service.yaml",
            ],
            check=True,
        ),
        mock.call(
            [
                "kubectl",
                "apply",
                "-f",
                "/external_resources/00-common/30-foo/10-deployment.yaml",
            ],
            check=True,
        ),
    ]
    assert os.path.exists("/external_resources/.applied/00-common/10-foo/30-hpa.yaml")
    assert os.path.exists(
        "/external_resources/.applied/00-common/10-foo/40-service.yaml"
    )
    assert os.path.exists(
        "/external_resources/.applied/00-common/30-foo/10-deployment.yaml"
    )


def test_resources_deleted_in_reverse_order(mock_run, fs):
    fs.create_file(
        "/external_resources/.applied/00-common/10-foo/30-hpa.yaml",
        contents="blah: blah",
    )
    fs.create_file(
        "/external_resources/.applied/00-common/10-foo/40-service.yaml",
        contents="blah: blah",
    )
    assert main("/external_resources") == 0
    assert mock_run.call_args_list == [
        mock.call(
            [
                "kubectl",
                "delete",
                "--ignore-not-found=true",
                "-f",
                "/external_resources/.applied/00-common/10-foo/40-service.yaml",
            ],
            check=True,
        ),
        mock.call(
            [
                "kubectl",
                "delete",
                "--ignore-not-found=true",
                "-f",
                "/external_resources/.applied/00-common/10-foo/30-hpa.yaml",
            ],
            check=True,
        ),
    ]
    assert not os.path.exists(
        "/external_resources/.applied/00-common/10-foo/30-hpa.yaml"
    )
    assert not os.path.exists(
        "/external_resources/.applied/00-common/10-foo/40-service.yaml"
    )


def test_kubectl_fails(mock_run, fs):
    mock_run.side_effect = [CalledProcessError(cmd="kubectl", returncode=1), None]
    fs.create_file(
        "/external_resources/00-common/10-foo/30-hpa.yaml",
        contents="blah: blah",
    )
    fs.create_file(
        "/external_resources/00-common/10-foo/40-service.yaml",
        contents="blah: blah",
    )
    assert main("/external_resources") == 1
    assert not os.path.exists(
        "/external_resources/.applied/00-common/10-foo/30-hpa.yaml"
    )
    assert os.path.exists(
        "/external_resources/.applied/00-common/10-foo/40-service.yaml"
    )
