import logging
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import ruamel.yaml as yaml

from paasta_tools.cli.cmds.validate import validate_schema
from paasta_tools.utils import DEFAULT_SOA_DIR


log = logging.getLogger(__name__)

# Must have a schema defined
KNOWN_CONFIG_TYPES = ("marathon", "kubernetes", "deploy", "smartstack")


def my_represent_none(self, data):
    return self.represent_scalar("tag:yaml.org,2002:null", "null")


def write_auto_config_data(
    service: str,
    extra_info: str,
    data: Dict[str, Any],
    soa_dir: str = DEFAULT_SOA_DIR,
    sub_dir: Optional[str] = None,
    comment: Optional[str] = None,
) -> Optional[str]:
    """
    Replaces the contents of an automated config file for a service, or creates the file if it does not exist.

    Returns the filename of the modified file, or None if no file was written.
    """
    yaml.YAML().representer.add_representer(type(None), my_represent_none)
    service_dir = f"{soa_dir}/{service}"
    if not os.path.exists(service_dir):
        log.warning(
            f"Service {service} does not exist in configs, skipping auto config update"
        )
        return None
    subdir = f"{service_dir}/{sub_dir}" if sub_dir else service_dir
    if not os.path.exists(subdir):
        os.mkdir(subdir)
    filename = f"{subdir}/{extra_info}.yaml"

    with open(filename, "w") as f:
        # TODO: this can be collapsed into one codeblock. It is separated as two
        # because doing content.update(data) results in losing comments from `data`
        # we should be able to handle adding a header comment and yaml with comments in it
        # without this if/else block
        if comment:
            content = (
                yaml.round_trip_load(
                    comment.format(regular_filename=f"{service}/{extra_info}.yaml")
                )
                if comment
                else {}
            )
            content.update(data)
        else:
            # avoids content.update to preserve comments in `data`
            content = data

        f.write(yaml.round_trip_dump(content))
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


class ValidationError(Exception):
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


def validate_auto_config_file(filepath: str, schema_subdir: str):
    basename = os.path.basename(filepath)
    for file_type in KNOWN_CONFIG_TYPES:
        schema_path = f"{schema_subdir}/{file_type}" if schema_subdir else file_type
        if basename.startswith(file_type):
            return bool(validate_schema(filepath, schema_path))
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
        working_dir: Optional[str] = None,
        do_clone: bool = True,
        validation_schema_path: str = "",
    ):
        self.config_source = config_source
        self.git_remote = git_remote
        self.branch = branch
        self.working_dir = working_dir
        self.do_clone = do_clone
        self.files_changed: Set[str] = set()
        self.validation_schema_path = validation_schema_path
        self.tmp_dir = None

    def __enter__(self):
        if self.do_clone:
            self.tmp_dir = TemporaryDirectory(dir=self.working_dir)
            self.working_dir = self.tmp_dir.name
            subprocess.check_call(["git", "clone", self.git_remote, self.working_dir])
        self.pwd = os.getcwd()
        os.chdir(self.working_dir)
        if self.branch != "master":
            subprocess.check_call(["git", "checkout", "-b", self.branch])
        return self

    def __exit__(self, type, value, traceback):
        os.chdir(self.pwd)
        if self.tmp_dir:
            self.tmp_dir.cleanup()

    def write_configs(
        self,
        service: str,
        extra_info: str,
        configs: Dict[str, Any],
        sub_dir: Optional[str] = None,
        comment: Optional[str] = None,
    ):
        result = write_auto_config_data(
            service,
            extra_info,
            configs,
            soa_dir=self.working_dir,
            sub_dir=sub_dir,
            comment=comment,
        )
        if result:
            self.files_changed.add(result)

    def get_existing_configs(
        self, service: str, file_name: str, sub_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        config_file_path = f"{sub_dir}/{file_name}" if sub_dir else file_name
        config_file_abs_path = os.path.join(
            os.path.abspath(self.working_dir),
            service,
            config_file_path + ".yaml",
        )
        try:
            return yaml.round_trip_load(open(config_file_abs_path))
        except FileNotFoundError:
            return {}

    def validate(self):
        return_code = True
        for filepath in self.files_changed:
            # We don't short circuit after a failure so the caller gets info on all the failures
            return_code = (
                validate_auto_config_file(filepath, self.validation_schema_path)
                and return_code
            )
        return return_code

    def commit_to_remote(self, extra_message: str = ""):
        if not self.validate():
            log.error("Files failed validation, not committing changes")
            raise ValidationError

        # TODO: more identifying information, like hostname or paasta_tools version?
        message = f"Update to configs from {self.config_source}"
        if extra_message:
            message = f"{message}\n\n{extra_message}"

        if _commit_files(list(self.files_changed), message):
            _push_to_remote(self.branch)
        else:
            log.info("No files changed, no push required.")
