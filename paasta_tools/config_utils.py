import logging
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import yaml
from service_configuration_lib import read_extra_service_information

from paasta_tools.cli.cmds.validate import validate_schema
from paasta_tools.utils import AUTO_SOACONFIG_SUBDIR
from paasta_tools.utils import DEFAULT_SOA_DIR


try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper  # type: ignore

log = logging.getLogger(__name__)

# Must have a schema defined
KNOWN_CONFIG_TYPES = ("marathon", "kubernetes")


def write_auto_config_data(
    service: str, extra_info: str, data: Dict[str, Any], soa_dir: str = DEFAULT_SOA_DIR
) -> Optional[str]:
    """
    Replaces the contents of an automated config file for a service, or creates the file if it does not exist.

    Returns the filename of the modified file, or None if no file was written.
    """
    service_dir = f"{soa_dir}/{service}"
    if not os.path.exists(service_dir):
        log.warning(
            f"Service {service} does not exist in configs, skipping auto config update"
        )
        return None
    subdir = f"{service_dir}/{AUTO_SOACONFIG_SUBDIR}"
    if not os.path.exists(subdir):
        os.mkdir(subdir)
    filename = f"{subdir}/{extra_info}.yaml"
    with open(filename, "w") as f:
        f.write(yaml.dump(data, Dumper=Dumper))
    return filename


def _commit_files(files: List[str], message: str) -> bool:
    """
    Stages the given files and creates a commit with the given message.

    Returns True if a new commit was created, False if the files are unchanged.
    """
    subprocess.check_call(["git", "add"] + files)
    # Skip commit if no changes are staged
    result_code = subprocess.call(["git", "diff-index", "--quiet", "--cached", "HEAD"])
    if result_code == 0:
        return False
    else:
        subprocess.check_call(["git", "commit", "--no-verify", "--message", message])
        return True


class PushNotFastForwardError(Exception):
    pass


def _push_to_remote(branch: str) -> None:
    try:
        subprocess.check_output(
            ("git", "push", "origin", branch), stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        if "Updates were rejected" in str(e.stdout):
            raise PushNotFastForwardError()
        else:
            log.error(f"Push to {branch} failed with:\n {e.stdout}")
            raise


def validate_auto_config_file(filepath: str):
    basename = os.path.basename(filepath)
    for file_type in KNOWN_CONFIG_TYPES:
        if basename.startswith(file_type):
            return bool(validate_schema(filepath, f"auto/{file_type}"))
    else:
        logging.info(f"{filepath} is invalid because it has no validator defined")
        return False


class AutoConfigUpdater:
    """
    Helper class for updating automated paasta configs.

    Usage:

        updater = AutoConfigUpdater('about_me', 'git@git.me:my_configs', branch='test')
        # The context manager clones the repo into a local temp directory, then
        # cleans up afterwards.
        with updater:
            # The updater replaces the content of files, so get the existing data
            # first if you want to update it
            data = updater.get_existing_configs('service_foo', 'conf_file')
            data["new_key"] = "g_minor"

            # Now write the new data
            updater.write_configs('service_foo', 'conf_file', data)

            # Edit more files...

            # Once you're done editing files, commit. If all files pass validation,
            # the updater will commit the changes and push them to the desired branch
            # on the remote.
            updater.commit_to_remote(extra_message="Adding some extra context.")

    Raises PushNotFastForwardError if the updated branch does not include changes in the
    remote branch.
    """

    def __init__(
        self,
        config_source: str,
        git_remote: str,
        branch: str = "master",
        tmp_dir: Optional[str] = None,
    ):
        self.config_source = config_source
        self.git_remote = git_remote
        self.branch = branch
        self.tmp_dir = tmp_dir
        self.files_changed: Set[str] = set()

    def __enter__(self):
        self.working_dir = TemporaryDirectory(dir=self.tmp_dir)
        subprocess.check_call(["git", "clone", self.git_remote, self.working_dir.name])
        self.pwd = os.getcwd()
        os.chdir(self.working_dir.name)
        if self.branch != "master":
            subprocess.check_call(["git", "checkout", "-b", self.branch])
        return self

    def __exit__(self, type, value, traceback):
        os.chdir(self.pwd)
        self.working_dir.cleanup()

    def write_configs(self, service: str, extra_info: str, configs: Dict[str, Any]):
        result = write_auto_config_data(
            service, extra_info, configs, soa_dir=self.working_dir.name
        )
        if result:
            self.files_changed.add(result)

    def get_existing_configs(self, service: str, extra_info: str) -> Dict[str, Any]:
        return read_extra_service_information(
            service,
            f"{AUTO_SOACONFIG_SUBDIR}/{extra_info}",
            soa_dir=self.working_dir.name,
        )

    def validate(self):
        return_code = True
        for filepath in self.files_changed:
            # We don't short circuit after a failure so the caller gets info on all the failures
            return_code = validate_auto_config_file(filepath) and return_code
        return return_code

    def commit_to_remote(self, extra_message: str = ""):
        if not self.validate():
            log.error("Files failed validation, not committing changes")
            return

        # TODO: more identifying information, like hostname or paasta_tools version?
        message = f"Update to {AUTO_SOACONFIG_SUBDIR} configs from {self.config_source}"
        if extra_message:
            message = f"{message}\n\n{extra_message}"

        if _commit_files(list(self.files_changed), message):
            _push_to_remote(self.branch)
        else:
            log.info("No files changed, no push required.")
