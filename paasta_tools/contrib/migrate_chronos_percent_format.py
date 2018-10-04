#!/usr/bin/env python3.6
"""Script for migrating Chronos command string formats.

Used to upgrade from v0.80.11 to v0.81.0 or downgrade to a version <= v0.80.11.
"""
import argparse
import os
import re
import shutil
from tempfile import mkstemp

from service_configuration_lib import _read_yaml_file

from paasta_tools.tron_tools import parse_time_variables
from paasta_tools.utils import get_readable_files_in_glob


PERCENT_PATTERN = re.compile('%\(([-+\w]+)\)s')
# Only include words surrounded by single braces!
BRACE_PATTERN = re.compile('([^\{]+)(\{[-+\w]+\})([^\}]+)')


def from_percent(line):
    line = line.\
        replace('%%', '%').\
        replace('{', '{{').\
        replace('}', '}}')

    result = ''
    remaining = line
    while remaining:
        match = PERCENT_PATTERN.search(remaining)
        if match:
            result += remaining[:match.start()]
            result += '{' + match.group()[2:-2] + '}'
            remaining = remaining[match.end():]
        else:
            result += remaining
            remaining = ''
    return result


def to_percent(line):
    line = line.replace('%', '%%')
    result = ''
    remaining = line
    while remaining:
        match = BRACE_PATTERN.search(remaining)
        if match:
            result += remaining[:match.start(2)]
            result += '%(' + match.group(2)[1:-1] + ')s'
            remaining = remaining[match.end(2):]
        else:
            result += remaining
            remaining = ''
    result = result.replace('{{', '{').replace('}}', '}')
    return result


def get_new_command(command_parts, include_flag, downgrade):
    changed = False
    new_command = ''
    migration_fn = to_percent if downgrade else from_percent
    for part in command_parts:
        new = migration_fn(part)
        if new != part:
            changed = True
        new_command += new

    # Only add use_percent_format: false to upgraded commands
    if changed and include_flag and not downgrade:
        cmd_start = command_parts[0].find('c')
        whitespace = command_parts[0][:cmd_start]
        new_command += f'{whitespace}use_percent_format: false\n'
    return new_command


def translate_file(current_filename, include_flag, downgrade):
    instances = _read_yaml_file(current_filename)
    if downgrade:
        to_update = set(instances.keys())
    else:
        to_update = {
            instance_name for instance_name, config in instances.items()
            if config.get('use_percent_format') is not False
        }

    _, new_filename = mkstemp()
    key_pattern = re.compile('\s*([-\w]+:|#)')
    instance_pattern = re.compile('[-\w]+:')
    current_instance = None
    with open(new_filename, 'w') as new_file, open(current_filename, 'r') as current_file:
        command_parts = []
        for line in current_file:
            if command_parts and key_pattern.match(line):
                # Reached the end of a command, print it
                new_file.write(
                    get_new_command(command_parts, include_flag, downgrade),
                )
                command_parts = []

            if 'use_percent_format' in line:
                continue

            # Only modify commands of certain instances
            instance_match = instance_pattern.match(line)
            if instance_match:
                current_instance = instance_match.group()[:-1]
            if current_instance not in to_update:
                new_file.write(line)
                continue

            if line.strip().startswith('cmd:'):
                command_parts.append(line)
            elif len(command_parts) > 0:
                command_parts.append(line)
            else:
                new_file.write(line)

        if command_parts:
            new_file.write(
                get_new_command(command_parts, include_flag, downgrade),
            )

    return new_filename


def compare_files(current_filename, new_filename, downgrade):
    try:
        new_configs = _read_yaml_file(new_filename)
    except Exception as e:
        return False

    for instance_name, current_config in _read_yaml_file(current_filename).items():
        if 'cmd' not in current_config:
            continue
        new_config = new_configs[instance_name]
        current_command = parse_time_variables(
            current_config['cmd'],
            use_percent=(
                not downgrade and
                current_config.get('use_percent_format') is not False
            ),
        )
        new_command = parse_time_variables(
            new_config['cmd'],
            use_percent=downgrade,
        )
        if new_command != current_command:
            return False
        for key in current_config:
            if key == 'cmd' or key == 'use_percent_format':
                continue
            if current_config[key] != new_config.get(key):
                return False
    return True


def main(soa_dir, include_flag, downgrade):
    files = get_readable_files_in_glob('*/chronos-*.yaml', soa_dir)
    failed = []
    for filename in files:
        if os.path.islink(filename):
            continue

        tmp_new = translate_file(filename, include_flag, downgrade)
        if compare_files(filename, tmp_new, downgrade):
            shutil.move(tmp_new, filename)
            print(f'OK: {filename}')
        else:
            failed.append(f'  {filename} not updated; incorrect migrated config at {tmp_new}')

    if failed:
        print('ERROR: some migrated configs not equivalent to old config, did not overwrite.')
        print('Manually inspect differences and update.')
        print('\n'.join(failed))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--soa-dir',
        help='Directory of soa configs to migrate',
    )
    parser.add_argument(
        '--include-flag',
        action='store_true',
        help='Include use_percent_format flag when converting to new format',
    )
    parser.add_argument(
        '--downgrade',
        action='store_true',
        help='Go back to % format',
    )
    args = parser.parse_args()

    soa_dir = os.path.abspath(args.soa_dir)
    main(soa_dir, args.include_flag, args.downgrade)
