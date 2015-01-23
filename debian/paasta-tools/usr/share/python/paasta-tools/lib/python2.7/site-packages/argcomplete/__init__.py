# Copyright 2012-2013, Andrey Kislyuk and argcomplete contributors.
# Licensed under the Apache License. See https://github.com/kislyuk/argcomplete for more info.

from __future__ import print_function, unicode_literals

import os, sys, argparse, contextlib, subprocess, locale, re

from . import my_shlex as shlex

USING_PYTHON2 = True if sys.version_info < (3, 0) else False

if not USING_PYTHON2:
    basestring = str

sys_encoding = locale.getpreferredencoding()

_DEBUG = '_ARC_DEBUG' in os.environ

debug_stream = sys.stderr

def debug(*args):
    if _DEBUG:
        print(file=debug_stream, *args)

BASH_FILE_COMPLETION_FALLBACK = 79
BASH_DIR_COMPLETION_FALLBACK = 80

safe_actions = (argparse._StoreAction,
                argparse._StoreConstAction,
                argparse._StoreTrueAction,
                argparse._StoreFalseAction,
                argparse._AppendAction,
                argparse._AppendConstAction,
                argparse._CountAction)

from . import completers
from .my_argparse import IntrospectiveArgumentParser, action_is_satisfied, action_is_open

@contextlib.contextmanager
def mute_stdout():
    stdout = sys.stdout
    sys.stdout = open(os.devnull, 'w')
    yield
    sys.stdout = stdout

@contextlib.contextmanager
def mute_stderr():
    stderr = sys.stderr
    sys.stderr = open(os.devnull, 'w')
    yield
    sys.stderr.close()
    sys.stderr = stderr

class ArgcompleteException(Exception):
    pass

def split_line(line, point):
    lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
    words = []

    def split_word(word):
        # TODO: make this less ugly
        point_in_word = len(word) + point - lexer.instream.tell()
        if isinstance(lexer.state, basestring) and lexer.state in lexer.whitespace:
            point_in_word += 1
        if point_in_word > len(word):
            debug("In trailing whitespace")
            words.append(word)
            word = ''
        prefix, suffix = word[:point_in_word], word[point_in_word:]
        prequote = ''
        # posix
        if lexer.state is not None and lexer.state in lexer.quotes:
            prequote = lexer.state
        # non-posix
        #if len(prefix) > 0 and prefix[0] in lexer.quotes:
        #    prequote, prefix = prefix[0], prefix[1:]

        first_colon_pos = lexer.first_colon_pos if ':' in word else None

        return prequote, prefix, suffix, words, first_colon_pos

    while True:
        try:
            word = lexer.get_token()
            if word == lexer.eof:
                # TODO: check if this is ever unsafe
                # raise ArgcompleteException("Unexpected end of input")
                return "", "", "", words, None
            if lexer.instream.tell() >= point:
                debug("word", word, "split, lexer state: '{s}'".format(s=lexer.state))
                return split_word(word)
            words.append(word)
        except ValueError:
            debug("word", lexer.token, "split (lexer stopped, state: '{s}')".format(s=lexer.state))
            if lexer.instream.tell() >= point:
                return split_word(lexer.token)
            else:
                raise ArgcompleteException("Unexpected internal state. Please report this bug at https://github.com/kislyuk/argcomplete/issues.")

def default_validator(completion, prefix):
    return completion.startswith(prefix)

class CompletionFinder(object):
    '''
    Inherit from this class if you wish to override any of the stages below. Otherwise, use ``argcomplete.autocomplete()``
    directly (it's a convenience instance of this class). It has the same signature as
    :meth:`CompletionFinder.__call__()`.
    '''
    def __init__(self):
        pass

    def __call__(self, argument_parser, always_complete_options=True, exit_method=os._exit, output_stream=None,
                 exclude=None, validator=None):
        '''
        :param argument_parser: The argument parser to autocomplete on
        :type argument_parser: :class:`argparse.ArgumentParser`
        :param always_complete_options: Whether or not to autocomplete options even if an option string opening character (normally ``-``) has not been entered
        :type always_complete_options: boolean
        :param exit_method: Method used to stop the program after printing completions. Defaults to :meth:`os._exit`. If you want to perform a normal exit that calls exit handlers, use :meth:`sys.exit`.
        :type exit_method: callable
        :param exclude: List of strings representing options to be omitted from autocompletion
        :type exclude: iterable
        :param validator: Function to filter all completions through before returning (called with two string arguments, completion and prefix; return value is evaluated as a boolean)
        :type validator: callable

        .. note:: If you are not subclassing CompletionFinder to override its behaviors, use ``argcomplete.autocomplete()`` directly. It has the same signature as this method.

        Produces tab completions for ``argument_parser``. See module docs for more info.

        Argcomplete only executes actions if their class is known not to have side effects. Custom action classes can be
        added to argcomplete.safe_actions, if their values are wanted in the ``parsed_args`` completer argument, or their
        execution is otherwise desirable.
        '''

        if '_ARGCOMPLETE' not in os.environ:
            # not an argument completion invocation
            return

        global debug_stream
        try:
            debug_stream = os.fdopen(9, 'w')
        except:
            debug_stream = sys.stderr

        if output_stream is None:
            try:
                output_stream = os.fdopen(8, 'wb')
            except:
                debug("Unable to open fd 8 for writing, quitting")
                exit_method(1)

        if validator is None:
            validator = default_validator
        self.validator = validator

        self.always_complete_options = always_complete_options
        self.exclude = exclude

        # print("", stream=debug_stream)
        # for v in 'COMP_CWORD', 'COMP_LINE', 'COMP_POINT', 'COMP_TYPE', 'COMP_KEY', '_ARGCOMPLETE_COMP_WORDBREAKS', 'COMP_WORDS':
        #     print(v, os.environ[v], stream=debug_stream)

        ifs = os.environ.get('_ARGCOMPLETE_IFS', '\013')
        if len(ifs) != 1:
            debug("Invalid value for IFS, quitting [{v}]".format(v=ifs))
            exit_method(1)

        comp_line = os.environ['COMP_LINE']
        comp_point = int(os.environ['COMP_POINT'])

        # Adjust comp_point for wide chars
        if USING_PYTHON2:
            comp_point = len(comp_line[:comp_point].decode(sys_encoding))
        else:
            comp_point = len(comp_line.encode(sys_encoding)[:comp_point].decode(sys_encoding))

        if USING_PYTHON2:
            comp_line = comp_line.decode(sys_encoding)

        cword_prequote, cword_prefix, cword_suffix, comp_words, first_colon_pos = split_line(comp_line, comp_point)

        if os.environ['_ARGCOMPLETE'] == "2": # Hook recognized the first word as the interpreter
            comp_words.pop(0)
        debug(u"\nLINE: '{l}'\nPREQUOTE: '{pq}'\nPREFIX: '{p}'".format(l=comp_line, pq=cword_prequote, p=cword_prefix), u"\nSUFFIX: '{s}'".format(s=cword_suffix), u"\nWORDS:", comp_words)

        active_parsers = [argument_parser]
        parsed_args = argparse.Namespace()
        visited_actions = []

        '''
        Since argparse doesn't support much introspection, we monkey-patch it to replace the parse_known_args method and
        all actions with hooks that tell us which action was last taken or about to be taken, and let us have the parser
        figure out which subparsers need to be activated (then recursively monkey-patch those).
        We save all active ArgumentParsers to extract all their possible option names later.
        '''
        def patchArgumentParser(parser):
            parser.__class__ = IntrospectiveArgumentParser
            for action in parser._actions:
                # TODO: accomplish this with super
                class IntrospectAction(action.__class__):
                    def __call__(self, parser, namespace, values, option_string=None):
                        debug('Action stub called on', self)
                        debug('\targs:', parser, namespace, values, option_string)
                        debug('\torig class:', self._orig_class)
                        debug('\torig callable:', self._orig_callable)

                        visited_actions.append(self)

                        if self._orig_class == argparse._SubParsersAction:
                            debug('orig class is a subparsers action: patching and running it')
                            active_subparser = self._name_parser_map[values[0]]
                            patchArgumentParser(active_subparser)
                            active_parsers.append(active_subparser)
                            self._orig_callable(parser, namespace, values, option_string=option_string)
                        elif self._orig_class in safe_actions:
                            self._orig_callable(parser, namespace, values, option_string=option_string)
                if getattr(action, "_orig_class", None):
                    debug("Action", action, "already patched")
                action._orig_class = action.__class__
                action._orig_callable = action.__call__
                action.__class__ = IntrospectAction

        patchArgumentParser(argument_parser)

        try:
            debug("invoking parser with", comp_words[1:])
            with mute_stderr():
                a = argument_parser.parse_known_args(comp_words[1:], namespace=parsed_args)
            debug("parsed args:", a)
        except BaseException as e:
            debug("\nexception", type(e), str(e), "while parsing args")

        debug("Active parsers:", active_parsers)
        debug("Visited actions:", visited_actions)
        debug("Parse result namespace:", parsed_args)

        completions = self.collect_completions(active_parsers, parsed_args, cword_prefix, debug)
        completions = self.filter_completions(completions)
        completions = self.quote_completions(completions, cword_prequote, first_colon_pos)

        debug("\nReturning completions:", completions)
        output_stream.write(ifs.join(completions).encode(sys_encoding))
        output_stream.flush()
        debug_stream.flush()
        exit_method(0)

    def collect_completions(self, active_parsers, parsed_args, cword_prefix, debug):
        '''
        Visits the active parsers and their actions, executes their completers or introspects them to collect their
        option strings. Returns the resulting completions as a list of strings.

        This method is exposed for overriding in subclasses; there is no need to use it directly.
        '''
        completions = []
        for parser in active_parsers:
            debug("Examining parser", parser)
            for action in parser._actions:
                debug("Examining action", action)
                if isinstance(action, argparse._SubParsersAction):
                    subparser_activated = False
                    for subparser in action._name_parser_map.values():
                        if subparser in active_parsers:
                            subparser_activated = True
                    if subparser_activated:
                        # Parent parser completions are not valid in the subparser, so flush them
                        completions = []
                    else:
                        completions += [subcmd for subcmd in action.choices.keys() if subcmd.startswith(cword_prefix)]
                elif self.always_complete_options or (len(cword_prefix) > 0 and cword_prefix[0] in parser.prefix_chars):
                    completions += [option for option in action.option_strings if option.startswith(cword_prefix)]

            debug("Active actions (L={l}): {a}".format(l=len(parser.active_actions), a=parser.active_actions))

            # Only run completers if current word does not start with - (is not an optional)
            if len(cword_prefix) == 0 or cword_prefix[0] not in parser.prefix_chars:
                for active_action in parser.active_actions:
                    if not active_action.option_strings: # action is a positional
                        if action_is_satisfied(active_action) and not action_is_open(active_action):
                            debug("Skipping", active_action)
                            continue

                    debug("Activating completion for", active_action, active_action._orig_class)
                    #completer = getattr(active_action, 'completer', DefaultCompleter())
                    completer = getattr(active_action, 'completer', None)

                    if completer is None and active_action.choices is not None:
                        if not isinstance(active_action, argparse._SubParsersAction):
                            completer = completers.ChoicesCompleter(active_action.choices)

                    if completer:
                        if len(active_action.option_strings) > 0: # only for optionals
                            if not action_is_satisfied(active_action):
                                # This means the current action will fail to parse if the word under the cursor is not given
                                # to it, so give it exclusive control over completions (flush previous completions)
                                debug("Resetting completions because", active_action, "is unsatisfied")
                                completions = []
                        if callable(completer):
                            completions += [c for c in completer(prefix=cword_prefix, action=active_action,
                                                                 parsed_args=parsed_args)
                                            if self.validator(c, cword_prefix)]
                        else:
                            debug("Completer is not callable, trying the readline completer protocol instead")
                            for i in range(9999):
                                next_completion = completer.complete(cword_prefix, i)
                                if next_completion is None:
                                    break
                                if self.validator(next_completion, cword_prefix):
                                    completions.append(next_completion)
                        debug("Completions:", completions)
                    elif not isinstance(active_action, argparse._SubParsersAction):
                        debug("Completer not available, falling back")
                        try:
                            # TODO: what happens if completions contain newlines? How do I make compgen use IFS?
                            bashcomp_cmd = ['bash', '-c', "compgen -A file -- '{p}'".format(p=cword_prefix)]
                            completions += subprocess.check_output(bashcomp_cmd).decode(sys_encoding).splitlines()
                        except subprocess.CalledProcessError:
                            pass
        return completions

    def filter_completions(self, completions):
        '''
        Ensures collected completions are Unicode text, de-duplicates them, and excludes those specified by ``exclude``.
        Returns the filtered completions as an iterable.

        This method is exposed for overriding in subclasses; there is no need to use it directly.
        '''
        # On Python 2, we have to make sure all completions are unicode objects before we continue and output them.
        # Otherwise, because python disobeys the system locale encoding and uses ascii as the default encoding, it will try
        # to implicitly decode string objects using ascii, and fail.
        if USING_PYTHON2:
            for i in range(len(completions)):
                if type(completions[i]) != unicode:
                    completions[i] = completions[i].decode(sys_encoding)

        # De-duplicate completions and remove excluded ones
        if self.exclude is None:
            self.exclude = set()
        seen = set(self.exclude)
        return [c for c in completions if c not in seen and not seen.add(c)]

    def quote_completions(self, completions, cword_prequote, first_colon_pos):
        '''
        If the word under the cursor started with a quote (as indicated by a nonempty ``cword_prequote``), escapes
        occurrences of that quote character in the completions, and adds the quote to the beginning of each completion.
        Otherwise, escapes all characters that bash splits words on (``COMP_WORDBREAKS``), and removes portions of
        completions before the first colon.

        If there is only one completion, and it doesn't end with a **continuation character** (``/``, ``:``, or ``=``),
        adds a space after the completion.

        This method is exposed for overriding in subclasses; there is no need to use it directly.
        '''
        comp_wordbreaks = os.environ.get('_ARGCOMPLETE_COMP_WORDBREAKS', os.environ.get('COMP_WORDBREAKS', " \t\"'@><=;|&(:."))
        if USING_PYTHON2:
            comp_wordbreaks = comp_wordbreaks.decode(sys_encoding)

        punctuation_chars = u'();<>|&!`'
        for char in punctuation_chars:
            if char not in comp_wordbreaks:
                comp_wordbreaks += char

        # If the word under the cursor was quoted, escape the quote char and add the leading quote back in.
        # Otherwise, escape all COMP_WORDBREAKS chars.
        if cword_prequote == '':
            # Bash mangles completions which contain colons.
            # This workaround has the same effect as __ltrim_colon_completions in bash_completion.
            if first_colon_pos:
                completions = [c[first_colon_pos+1:] for c in completions]

            for wordbreak_char in comp_wordbreaks:
                completions = [c.replace(wordbreak_char, '\\'+wordbreak_char) for c in completions]
        else:
            if cword_prequote == '"':
                for char in '`$!':
                    completions = [c.replace(char, '\\'+char) for c in completions]
            completions = [cword_prequote+c.replace(cword_prequote, '\\'+cword_prequote) for c in completions]

        # Note: similar functionality in bash is turned off by supplying the "-o nospace" option to complete.
        # We can't use that functionality because bash is not smart enough to recognize continuation characters (/) for
        # which no space should be added.
        continuation_chars = '=/:'
        if len(completions) == 1 and completions[0][-1] not in continuation_chars:
            if cword_prequote == '' and not completions[0].endswith(' '):
                completions[0] += ' '

        return completions

autocomplete = CompletionFinder()
autocomplete.__doc__ = ''' Use this to access argcomplete. See :meth:`argcomplete.CompletionFinder.__call__()`. '''

def warn(*args):
    '''
    Prints **args** to standard error when running completions. This will interrupt the user's command line interaction;
    use it to indicate an error condition that is preventing your completer from working.
    '''
    print("\n", file=debug_stream, *args)
