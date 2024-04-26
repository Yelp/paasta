import json
import os
from typing import Any
from typing import Dict


def merge_vscode_settings(file: str, setting_dict: Dict[str, Any]) -> None:
    os.makedirs("./.vscode", exist_ok=True)

    json_path = os.path.join("./.vscode", file)
    # first checks if the file exists
    # if it does then load the file content as json into settings
    # if it doesn't exist then set settings to empty
    if os.path.exists(json_path):
        with open(json_path, mode="r") as settings_file:
            settings = json.load(settings_file)
    else:
        settings = {}

    # update settings with the default configurations we want each file to have
    merge_dicts(setting_dict, settings)

    with open(json_path, mode="w") as settings_file:
        json.dump(settings, settings_file, indent=2)


def merge_dicts(merge_from: Dict[str, Any], merge_to: Dict[str, Any]) -> None:
    for key, value in merge_from.items():
        if key in merge_to:
            if value not in merge_to[key]:
                for dict_entry in merge_from[key]:
                    if dict_entry not in merge_to[key]:
                        if isinstance(merge_to[key], list):
                            merge_to[key].append(dict_entry)
                        else:
                            temp_list = [merge_to[key]]
                            temp_list.append(dict_entry)
                            merge_to[key] = temp_list
        else:
            merge_to[key] = value


def install_vscode_support() -> None:
    paasta_schema_settings = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "tox test",
                "type": "python",
                "request": "launch",
                "cwd": "${workspaceFolder}",
                "console": "integratedTerminal",
                "python": "${workspaceFolder}/.paasta/bin/python",
                "program": "${workspaceFolder}/.paasta/bin/tox",
                "subProcess": True,
                "args": ["-e", "py38-linux,docs,mypy,tests"],
            },
            {
                "name": "paasta cli",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
            },
            {
                "name": "paasta rollback",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": [
                    "rollback",
                    "--service",
                    "katamari_test_service",
                    "--deploy-group",
                    "dev.canary",
                    "--commit",
                    "fa7f2023c84736bd05201ef96ebd3c3fed6ab903",  # pragma: whitelist secret
                ],
            },
            {
                "name": "paasta mark-for-deployment",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": [
                    "mark-for-deployment",
                    "--service",
                    "katamari_test_service",
                    "--deploy-group",
                    "dev.canary",
                    "--commit",
                    "224173aca322207994e655c4316ddb16f00eaab7",  # pragma: whitelist secret
                    "--wait-for-deployment",
                ],
            },
            {
                "name": "paasta status",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": [
                    "status",
                    "--service",
                    "katamari_test_service",
                    "--clusters",
                    "norcal-devc",
                    "--instance",
                    "canary",
                ],
            },
            {
                "name": "paasta playground",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "justMyCode": False,
                "env": {"PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_playground/"},
                "args": [
                    "status",
                    "--service",
                    "compute-infra-test-service",
                    "--clusters",
                    "kind-${env:USER}-k8s-test",
                    "-d",
                    "./soa_config_playground/",
                ],
                "preLaunchTask": "Run API Playground",
            },
            {
                "name": "paasta status playground",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "justMyCode": False,
                "env": {"PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_playground/"},
                "args": [
                    "status",
                    "--service",
                    "compute-infra-test-service",
                    "--clusters",
                    "kind-${env:USER}-k8s-test",
                    "-d",
                    "./soa_config_playground/",
                ],
            },
            {
                "name": "paasta logs",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": [
                    "logs",
                    "--service",
                    "katamari_test_service",
                    "--cluster",
                    "norcal-devc",
                    "--instance",
                    "canary",
                ],
            },
            {
                "name": "paasta validate",
                # This command has to be ran from inside the service repo in yelpsoa-configs
                "cwd": "${userHome}/pg/yelpsoa-configs/",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": ["validate", "--service", "katamari_test_service"],
            },
            {
                # 1) Follow step 1 in "Running the PaaSTA HTTP API Locally" wiki
                # 2) Run this "paasta API" test to debug paasta API
                # 3) Run client command, e.g. PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_for_development/  .tox/py38-linux/bin/python paasta_tools/cli/cli.py status --clusters norcal-devc --service katamari_test_service
                "name": "paasta API",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.run-paasta-api-in-dev-mode",
                "env": {
                    "KUBECONFIG": "./etc_paasta_for_development/admin.conf",
                    "PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_for_development/",
                    "PAASTA_TEST_CLUSTER": "norcal-devc",
                    "PYDEVD_USE_CYTHON": "NO",
                    "PYTHONUNBUFFERED": "1",
                    "PAASTA_API_SINGLE_PROCESS": "true",
                },
            },
            {
                "name": "paasta API playground",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.run-paasta-api-playground",
                "justMyCode": False,
                "env": {
                    "KUBECONFIG": "./k8s_itests/kubeconfig",
                    "PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_playground/",
                    "PAASTA_TEST_CLUSTER": "kind-${env:USER}-k8s-test",
                    "PYDEVD_USE_CYTHON": "NO",
                    "PYTHONUNBUFFERED": "1",
                    "PAASTA_API_SINGLE_PROCESS": "true",
                    "PAASTA_API_SOA_DIR": "./soa_config_playground/",
                },
            },
            {
                "name": "Run setup k8s job in playground",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.setup_kubernetes_job",
                "justMyCode": False,
                "args": [
                    "-d",
                    "./soa_config_playground",
                    "-c",
                    "kind-${env:USER}-k8s-test",
                    "compute-infra-test-service.autoscaling",
                ],
                "env": {
                    "KUBECONFIG": "./k8s_itests/kubeconfig",
                    "PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_playground/",
                    "PAASTA_TEST_CLUSTER": "kind-${env:USER}-k8s-test",
                    "PYDEVD_USE_CYTHON": "NO",
                    "PYTHONUNBUFFERED": "1",
                },
            },
            {
                "name": "Generate deployments.json in playground",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py38-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.generate_deployments_for_service",
                "justMyCode": False,
                "args": [
                    "-d",
                    "./soa_config_playground",
                    "-s",
                    "compute-infra-test-service",
                    "-v",
                ],
                "env": {
                    "PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_playground/",
                    "PYDEVD_USE_CYTHON": "NO",
                    "PYTHONUNBUFFERED": "1",
                },
            },
        ],
    }
    merge_vscode_settings("launch.json", paasta_schema_settings)

    recommended_extensions = {
        "recommendations": [
            "redhat.vscode-yaml",
            "ms-python.python",
            "ms-python.vscode-pylance",
            "ms-vscode.makefile-tools",
        ]
    }
    merge_vscode_settings("extensions.json", recommended_extensions)

    playground_task = {
        "version": "2.0.0",
        "tasks": [
            {
                "label": "Run API Playground",
                "type": "shell",
                "isBackground": True,
                "command": "make playground-api",
                "presentation": {
                    "reveal": "always",
                    "panel": "new",
                },
                "problemMatcher": [
                    {
                        "pattern": [
                            {
                                "regexp": ".",
                                "file": 1,
                                "location": 2,
                                "message": 3,
                            }
                        ],
                        "background": {
                            "activeOnStart": True,
                            "beginsPattern": ".",
                            "endsPattern": "^(.*?)\\[INFO\\]",
                        },
                    }
                ],
            }
        ],
    }
    merge_vscode_settings("tasks.json", playground_task)
    print("VS Code IDE Helpers installed")


if __name__ == "__main__":
    install_vscode_support()
