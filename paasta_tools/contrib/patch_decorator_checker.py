#!/usr/bin/env python3.11
"""Fail if mock.patch (or patch.object/dict/multiple) is used as a decorator."""
import ast
import sys

PATCH_HELPERS = {"object", "dict", "multiple"}

DECORATED_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def is_patch_decorator(node: ast.expr) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr in PATCH_HELPERS:
        func = func.value
    if isinstance(func, ast.Name):
        return func.id == "patch"
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "patch"
        and isinstance(func.value, ast.Name)
        and func.value.id == "mock"
    )


def check_file(filename: str) -> int:
    with open(filename) as f:
        tree = ast.parse(f.read(), filename=filename)
    errors = 0
    for node in ast.walk(tree):
        if not isinstance(node, DECORATED_NODES):
            continue
        for decorator in node.decorator_list:
            if is_patch_decorator(decorator):
                print(f"{filename}:{decorator.lineno}: mock.patch used as a decorator")
                errors += 1
    return errors


def main(filenames: list[str]) -> int:
    errors = sum(check_file(filename) for filename in filenames)
    if errors:
        print(
            "Use mock.patch as a context manager instead, e.g. "
            "`with mock.patch(..., autospec=True) as mock_thing:`"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
