#!/usr/bin/env python2.7
import compiler
import sys
from compiler.visitor import ASTVisitor


class MockChecker(ASTVisitor):
    def __init__(self):
        self.errors = 0
        self.init_module_imports()

    def init_module_imports(self):
        self.imported_patch = False
        self.imported_mock = False

    def check_files(self, files):
        for file in files:
            self.check_file(file)

    def check_file(self, filename):
        self.current_filename = filename
        try:
            ast = compiler.parseFile(filename)
        except SyntaxError, error:
            print "SyntaxError on file %s:%d" % (filename, error.lineno)
            return
        compiler.walk(ast, self)
        self.init_module_imports()

    def visitImport(self, node):
        if (name for name in node.names if 'mock' in name):
            self.imported_mock = True

    def visitFrom(self, node):
        if node.modname == 'mock' and (name for name in node.names if 'patch' in name):
            self.imported_patch = True

    def visitCallFunc(self, node):
        try:
            if (self.imported_patch and node.node.name == 'patch') or \
                    (self.imported_mock and node.node.expr.name == 'mock' and node.node.attrname == 'patch'):
                if not any(
                        [arg for arg in node.args if isinstance(arg, compiler.ast.Keyword) and arg.name == 'autospec']):
                    print "%s:%d: Found a mock without an autospec!" % (self.current_filename, node.lineno)
                    self.errors += 1
        except AttributeError:
            pass
        for child in node.getChildNodes():
            self.visit(child)


def main(filenames):
    checker = MockChecker()
    checker.check_files(filenames)
    if checker.errors == 0:
        sys.exit(0)
    else:
        print "You probably meant to specify 'autospec=True' in these tests."
        print "If you really don't want to, specify 'autospec=None'"
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
