#!/usr/bin/env python3.7
import ast
import sys


class MockChecker(ast.NodeVisitor):
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
            with open(filename, "r") as fd:
                try:
                    file_ast = ast.parse(fd.read())
                except SyntaxError as error:
                    print("SyntaxError on file %s:%d" % (filename, error.lineno))
                    return
        except IOError:
            print("Error opening filename: %s" % filename)
            return
        self.init_module_imports()
        self.visit(file_ast)

    def _call_uses_patch(self, node):
        try:
            return node.func.id == "patch"
        except AttributeError:
            return False

    def _call_uses_mock_patch(self, node):
        try:
            return node.func.value.id == "mock" and node.func.attr == "patch"
        except AttributeError:
            return False

    def visit_Import(self, node):
        if [name for name in node.names if "mock" == name.name]:
            self.imported_mock = True

    def visit_ImportFrom(self, node):
        if node.module == "mock" and (
            name for name in node.names if "patch" == name.name
        ):
            self.imported_patch = True

    def visit_Call(self, node):
        try:
            if (self.imported_patch and self._call_uses_patch(node)) or (
                self.imported_mock and self._call_uses_mock_patch(node)
            ):
                if not any(
                    [keyword for keyword in node.keywords if keyword.arg == "autospec"]
                ):
                    print(
                        "%s:%d: Found a mock without an autospec!"
                        % (self.current_filename, node.lineno)
                    )
                    self.errors += 1
        except AttributeError:
            pass
        self.generic_visit(node)


def main(filenames):
    checker = MockChecker()
    checker.check_files(filenames)
    if checker.errors == 0:
        sys.exit(0)
    else:
        print("You probably meant to specify 'autospec=True' in these tests.")
        print("If you really don't want to, specify 'autospec=None'")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
