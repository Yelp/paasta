#!/usr/bin/env python3
import argparse
import ast
import sys
from pathlib import Path

import tomli


def find_pyproject_toml(start_path: Path) -> Path | None:
    """Find pyproject.toml by walking up the directory tree until repo root."""
    current = start_path.resolve()
    while current != current.parent:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject
        # Stop at git repo root - we could use `git rev-parse --show-toplevel`,
        # but then we'd have to shell out
        if (current / ".git").exists():
            break
        current = current.parent
    return None


def read_line_length_config(file_path: Path) -> int:
    """Read line-length from pyproject.toml, default to 88."""
    pyproject_path = find_pyproject_toml(file_path.parent)
    if not pyproject_path:
        return 88

    try:
        with open(pyproject_path, "rb") as f:
            config = tomli.load(f)
            return config.get("tool", {}).get("black", {}).get("line-length", 88)
    except Exception:
        return 88


def is_patch_decorator(decorator: ast.expr) -> tuple[bool, str | None]:
    """
    Check if a decorator is a patch decorator and return its type.
    Returns (is_patch, patch_type) where patch_type is one of:
    'patch', 'patch.object', 'patch.dict', 'patch.multiple'
    """
    # Handle @patch(...)
    if isinstance(decorator, ast.Call):
        func = decorator.func
        if isinstance(func, ast.Name) and func.id == "patch":
            return True, "patch"
        elif isinstance(func, ast.Attribute):
            if isinstance(func.value, ast.Name) and func.value.id == "patch":
                if func.attr in ("object", "dict", "multiple"):
                    return True, f"patch.{func.attr}"
    # Handle @patch.object (without call)
    elif isinstance(decorator, ast.Attribute):
        if isinstance(decorator.value, ast.Name) and decorator.value.id == "patch":
            if decorator.attr in ("object", "dict", "multiple"):
                return True, f"patch.{decorator.attr}"
    # Handle @patch (without call - rare but possible)
    elif isinstance(decorator, ast.Name) and decorator.id == "patch":
        return True, "patch"

    return False, None


class PatchDecoratorRewriter(ast.NodeTransformer):
    """AST transformer that rewrites @patch decorators to context managers."""

    def __init__(self, line_length: int, check_only: bool = False):
        self.line_length = line_length
        self.check_only = check_only
        self.modified = False
        self.violations: list[tuple[int, str]] = []  # (line_number, function_name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        return self._process_function(node)

    def visit_AsyncFunctionDef(
        self, node: ast.AsyncFunctionDef
    ) -> ast.AsyncFunctionDef:
        return self._process_function(node)

    def _process_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> ast.FunctionDef | ast.AsyncFunctionDef:
        """Process both sync and async function definitions."""
        # Find all patch decorators
        patch_decorators = []
        other_decorators = []

        for decorator in node.decorator_list:
            is_patch, patch_type = is_patch_decorator(decorator)
            if is_patch:
                patch_decorators.append(decorator)
            else:
                other_decorators.append(decorator)

        if not patch_decorators:
            # No patch decorators, continue visiting children
            self.generic_visit(node)
            return node

        # We have patch decorators to rewrite
        self.modified = True

        # Collect violation information for check-only mode
        if self.check_only:
            for decorator in patch_decorators:
                line_number = (
                    decorator.lineno if hasattr(decorator, "lineno") else node.lineno
                )
                self.violations.append((line_number, node.name))

        # Extract parameter names that correspond to mocks
        # Decorators are applied bottom-to-top, so parameters are in reverse order
        mock_params = []
        num_patches = len(patch_decorators)

        # Get the first N parameters (where N = number of patch decorators)
        for i in range(min(num_patches, len(node.args.args))):
            mock_params.append(node.args.args[i].arg)

        # Remove the mock parameters from the function signature
        new_args = ast.arguments(
            posonlyargs=node.args.posonlyargs,
            args=node.args.args[num_patches:],
            vararg=node.args.vararg,
            kwonlyargs=node.args.kwonlyargs,
            kw_defaults=node.args.kw_defaults,
            kwarg=node.args.kwarg,
            defaults=node.args.defaults,
        )

        # Create the with statement with all patches
        # Reverse the decorators to match parameter order
        with_items = []
        for decorator, param_name in zip(reversed(patch_decorators), mock_params):
            # Convert decorator to context manager
            if isinstance(decorator, ast.Call):
                context_expr = decorator
            else:
                # Decorator without parentheses - add empty call
                context_expr = ast.Call(func=decorator, args=[], keywords=[])

            with_items.append(
                ast.withitem(
                    context_expr=context_expr,
                    optional_vars=ast.Name(id=param_name, ctx=ast.Store()),
                )
            )

        # Create the with statement
        with_stmt = ast.With(
            items=with_items,
            body=node.body,
        )

        # Update the function
        new_node = node
        new_node.decorator_list = other_decorators
        new_node.args = new_args
        new_node.body = [with_stmt]

        # Continue visiting children
        self.generic_visit(new_node)
        return new_node


def format_with_line_length(code: str, line_length: int) -> str:
    """
    Break long with statements across multiple lines if they exceed line_length.
    This is a simple formatter that handles the common case of long with statements.
    """
    lines = code.split("\n")
    formatted_lines = []

    for line in lines:
        stripped = line.lstrip()
        indent = line[: len(line) - len(stripped)]

        # Check if this is a with statement that's too long
        if stripped.startswith("with ") and len(line) > line_length:
            # Try to break after commas
            if ", " in stripped:
                # Split the with statement
                parts = stripped.split(", ")
                formatted_lines.append(indent + parts[0] + ", \\")
                for part in parts[1:-1]:
                    formatted_lines.append(indent + "     " + part + ", \\")
                formatted_lines.append(indent + "     " + parts[-1])
            else:
                # Can't break it nicely, just keep it as is
                formatted_lines.append(line)
        else:
            formatted_lines.append(line)

    return "\n".join(formatted_lines)


def extract_leading_comments(source: str) -> str:
    """Extract leading comments and blank lines from source code."""
    lines = source.split("\n")
    leading_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            leading_lines.append(line)
        else:
            # Stop at first non-comment, non-blank line
            break
    return "\n".join(leading_lines) if leading_lines else ""


def rewrite_file(filename: str, check_only: bool, line_length: int) -> bool:
    """
    Rewrite patch decorators in a file.
    Returns True if file was modified (or would be modified in check_only mode).
    """
    try:
        with open(filename, "r") as f:
            source = f.read()
    except IOError as e:
        print(f"Error reading {filename}: {e}", file=sys.stderr)
        return False

    # Extract leading comments (copyright headers, etc.)
    leading_comments = extract_leading_comments(source)

    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as e:
        print(f"Syntax error in {filename}:{e.lineno}: {e.msg}", file=sys.stderr)
        return False

    # Rewrite the AST
    rewriter = PatchDecoratorRewriter(line_length, check_only=check_only)
    new_tree = rewriter.visit(tree)

    if not rewriter.modified:
        # No changes needed
        return False

    if check_only:
        print(f"{filename}: Contains @patch decorators that should be rewritten")
        for line_num, func_name in sorted(rewriter.violations):
            print(
                f"  {filename}:{line_num}: @patch decorator in function '{func_name}'"
            )
        return True

    # Fix missing location information in the AST
    ast.fix_missing_locations(new_tree)

    # Generate new code
    try:
        new_code = ast.unparse(new_tree)
    except Exception as e:
        print(f"Error unparsing {filename}: {e}", file=sys.stderr)
        return False

    # Apply line length formatting
    new_code = format_with_line_length(new_code, line_length)

    # Prepend leading comments if they exist
    if leading_comments:
        new_code = leading_comments + "\n" + new_code

    # Write back
    try:
        with open(filename, "w") as f:
            f.write(new_code)
        print(f"{filename}: Rewrote @patch decorators to context managers")
        return True
    except IOError as e:
        print(f"Error writing {filename}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Rewrite @patch decorators to context manager form"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Check for violations without modifying files",
    )
    parser.add_argument("filenames", nargs="+", help="Files to check/fix")

    args = parser.parse_args()

    # Determine line length from first file's pyproject.toml
    line_length = 88
    if args.filenames:
        line_length = read_line_length_config(Path(args.filenames[0]))

    violations_found = False
    errors_occurred = False

    for filename in args.filenames:
        try:
            if rewrite_file(filename, args.check_only, line_length):
                violations_found = True
        except Exception as e:
            print(f"Error processing {filename}: {e}", file=sys.stderr)
            errors_occurred = True

    if errors_occurred:
        sys.exit(1)
    elif violations_found and args.check_only:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
