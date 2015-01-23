# Copyright 2012-2013, Andrey Kislyuk and argcomplete contributors.
# Licensed under the Apache License. See https://github.com/kislyuk/argcomplete for more info.

import os
import sys
import subprocess

def _wrapcall(*args, **kargs):
    try:
        return subprocess.check_output(*args,**kargs).decode().splitlines()
    except AttributeError:
        return _wrapcall_2_6(*args, **kargs)
    except subprocess.CalledProcessError:
        return []

def _wrapcall_2_6(*args, **kargs):
    try:
        # no check_output in 2.6,
        if 'stdout' in kargs:
            raise ValueError('stdout argument not allowed, it will be overridden.')
        process = subprocess.Popen(
            stdout=subprocess.PIPE, *args, **kargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kargs.get("args")
            if cmd is None:
                cmd = args[0]
            raise subprocess.CalledProcessError(retcode, cmd)
        return output.decode().splitlines()
    except subprocess.CalledProcessError:
        return []


class ChoicesCompleter(object):
    def __init__(self, choices=[]):
        self.choices = choices

    def __call__(self, prefix, **kwargs):
        return (c for c in self.choices if c.startswith(prefix))

def EnvironCompleter(prefix, **kwargs):
    return (v for v in os.environ if v.startswith(prefix))

class FilesCompleter(object):
    'File completer class, optionally takes a list of allowed extensions'
    def __init__(self,allowednames=(),directories=True):
        # Fix if someone passes in a string instead of a list
        if type(allowednames) is str:
            allowednames = [allowednames]

        self.allowednames = [x.lstrip('*').lstrip('.') for x in allowednames]
        self.directories = directories

    def __call__(self, prefix, **kwargs):
        completion = []
        if self.allowednames:
            if self.directories:
                files = _wrapcall(['bash','-c',
                    "compgen -A directory -- '{p}'".format(p=prefix)])
                completion += [ f + '/' for f in files]
            for x in self.allowednames:
                completion += _wrapcall(['bash', '-c',
                    "compgen -A file -X '!*.{0}' -- '{p}'".format(x,p=prefix)])
        else:
            completion += _wrapcall(['bash', '-c',
                "compgen -A file -- '{p}'".format(p=prefix)])

            anticomp = _wrapcall(['bash', '-c',
                "compgen -A directory -- '{p}'".format(p=prefix)])

            completion = list( set(completion) - set(anticomp))

            if self.directories:
                completion += [f + '/' for f in anticomp]
        return completion

