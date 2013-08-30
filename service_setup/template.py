from os import path
import string

from service_setup import config

class NoSuchTemplate(Exception): pass

class Template(object):

    def __init__(self, name):
        self.name = name
        tmpl_file = path.join(config.TEMPLATE_DIR, name + '.tmpl')
        if not path.exists(tmpl_file):
            raise NoSuchTemplate("%s doesn't exist" % tmpl_file)
        with open(tmpl_file) as f:
            self.str_tmpl = string.Template(f.read())

    def substitute(self, d):
        return self.str_tmpl.substitute(d)
