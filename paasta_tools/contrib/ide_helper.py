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
    settings.update(setting_dict)

    with open(json_path, mode="w") as settings_file:
        json.dump(settings, settings_file, indent=2)


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
                "args": ["-e", "py37-linux,docs,mypy,tests"],
            },
            {
                "name": "paasta cli",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
            },
            {
                "name": "paasta rollback",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
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
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
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
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
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
                "name": "paasta logs",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
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
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.cli.cli",
                "args": ["validate", "--service", "katamari_test_service"],
            },
            {
                # 1) Follow step 1 in "Running the PaaSTA HTTP API Locally" wiki
                # 2) Run this "paasta API" test to debug paasta API
                # 3) Run client command, e.g. PAASTA_SYSTEM_CONFIG_DIR=./etc_paasta_for_development/  .tox/py37-linux/bin/python paasta_tools/cli/cli.py status --clusters norcal-devc --service katamari_test_service
                "name": "paasta API",
                "cwd": "${workspaceFolder}",
                "python": "${workspaceFolder}/.tox/py37-linux/bin/python",
                "type": "python",
                "request": "launch",
                "module": "paasta_tools.run-paasta-api-in-dev-mode",
                "env": {
                    "KUBECONFIG": "./etc_paasta_for_development/admin.conf",
                    "PAASTA_SYSTEM_CONFIG_DIR": "./etc_paasta_for_development/",
                    "PAASTA_TEST_CLUSTER": "norcal-devc",
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
    print("VS Code IDE Helpers installed")


if __name__ == "__main__":
    install_vscode_support()
