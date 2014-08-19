"""A small lib for a prompt driven CLI"""
import readline
from contextlib import contextmanager

@contextmanager
def prepopulate(default):
    """Prepopluates the input with the default text for the user to edit"""
    def hook():
        readline.insert_text(default)
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
    with prepopulate(str(suggestion)):
        return raw_input(prompt_str).strip(' ')

def yes_no(question):
    """Asks the user a yes or no question"""
    while True:
        reply = raw_input(question + ' (y/n) ').lower()
        if len(reply) == 0 or not reply[0] in ['y', 'n']:
            continue
        return reply[0] == 'y'
