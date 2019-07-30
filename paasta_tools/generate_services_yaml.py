#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse

from paasta_tools.generate_services_file import write_yaml_file


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("output_path")
    args = parser.parse_args(argv)

    write_yaml_file(args.output_path)


if __name__ == "__main__":
    main()
