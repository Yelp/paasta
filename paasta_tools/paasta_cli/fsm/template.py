# Copyright 2015 Yelp Inc.
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

from os import path
# TODO: Use pystache instead of string.Template?
import string

from paasta_tools.paasta_cli.fsm import config


class NoSuchTemplate(Exception):
    pass


class Template(object):
    """A small convenience wrapper for the python string Template library

    The appropriate template will be loaded from the file of the same name
    in config.TEMPLATE_DIR
    """

    def __init__(self, name):
        self.name = name
        tmpl_file = path.join(config.TEMPLATE_DIR, name + '.tmpl')
        if not path.exists(tmpl_file):
            raise NoSuchTemplate("%s doesn't exist" % tmpl_file)
        with open(tmpl_file) as f:
            self.str_tmpl = string.Template(f.read())

    def substitute(self, d):
        return self.str_tmpl.substitute(d)
