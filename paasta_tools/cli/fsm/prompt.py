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

"""A small lib for a prompt driven CLI"""
import readline
from contextlib import contextmanager


@contextmanager
def prepopulate(default):
    """Prepopluates the input with the default text for the user to edit"""
    def hook():
        readline.insert_text(str(default))
        readline.redisplay()
    if default is not None:
        readline.set_pre_input_hook(hook)
    yield
    readline.set_pre_input_hook(None)


def ask(question, suggestion=None):
    """Prompt the user for input, with default text optionally pre-populated"""
    prompt_str = question
    # Make multi-line defaults look better by adding line breaks and separation
    if suggestion is not None and '\n' in str(suggestion):
        prompt_str += '\n------\n'
    elif not prompt_str.endswith(' '):
        prompt_str += ' '
    with prepopulate(suggestion):
        return raw_input(prompt_str).strip(' ')


def yes_no(question):
    """Asks the user a yes or no question"""
    while True:
        reply = raw_input(question + ' (y/n) ').lower()
        if len(reply) == 0 or not reply[0] in ['y', 'n']:
            continue
        return reply[0] == 'y'
