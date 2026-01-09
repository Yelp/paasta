#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

import libcst as cst
import tomli


def find_pyproject_toml(start_path: Path) -> Path | None:
    """Find pyproject.toml by walking up the directory tree until repo root."""
    current = start_path.resolve()
    while current != current.parent:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject
        # stop at git repo root so that we don't go all the way to the fs root
        # we could use `git rev-parse --show-toplevel`,
        # but then we'd have to shell out :p
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


def is_patch_decorator(decorator: cst.Decorator) -> bool:
    """Check if a decorator is a patch decorator (@patch, @patch.object, etc.)."""
    dec = decorator.decorator

    # Handle @patch(...) or @patch
    if isinstance(dec, cst.Call):
        func = dec.func
    else:
        func = dec

    # Check for patch or patch.object/dict/multiple
    if isinstance(func, cst.Name) and func.value == "patch":
        return True
    elif isinstance(func, cst.Attribute):
        if isinstance(func.value, cst.Name) and func.value.value == "patch":
            if func.attr.value in ("object", "dict", "multiple"):
                return True
    return False


class VariableUsageCollector(cst.CSTVisitor):
    """Visitor to collect all variable names used in a function body."""

    def __init__(self):
        self.used_names: set[str] = set()

    def visit_Name(self, node: cst.Name) -> None:
        """Collect all Name nodes (variable references)."""
        self.used_names.add(node.value)


def check_variable_usage(body: cst.IndentedBlock, var_names: list[str]) -> set[str]:
    """Return set of var_names that are actually used in the body."""
    collector = VariableUsageCollector()
    body.visit(collector)
    return {name for name in var_names if name in collector.used_names}


class PatchDecoratorRewriter(cst.CSTTransformer):
    """libcst transformer that rewrites @patch decorators to context managers."""

    def __init__(
        self, line_length: int, check_only: bool = False, skip_unused: bool = False
    ):
        self.line_length = line_length
        self.check_only = check_only
        self.skip_unused = skip_unused
        self.modified = False
        self.violations: list[tuple[int, str]] = []

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        """Rewrite functions that have @patch decorators."""
        # Find all patch decorators
        patch_decorators = []
        other_decorators = []

        for decorator in original_node.decorators:
            if is_patch_decorator(decorator):
                patch_decorators.append(decorator)
            else:
                other_decorators.append(decorator)

        if not patch_decorators:
            return updated_node

        # Mark as modified
        self.modified = True

        # In check-only mode, just collect violations
        if self.check_only:
            for decorator in patch_decorators:
                # Get line number from decorator
                pos = self.get_metadata(cst.metadata.PositionProvider, decorator)
                line_num = pos.start.line if pos else 0
                self.violations.append((line_num, original_node.name.value))
            return updated_node

        # Extract mock parameter names (reverse order)
        num_patches = len(patch_decorators)
        mock_params = []

        # Get first N parameters
        params = original_node.params.params
        for i in range(min(num_patches, len(params))):
            mock_params.append(params[i].name.value)

        # Remove mock parameters from function signature
        remaining_params = params[num_patches:]
        new_params = original_node.params.with_changes(params=remaining_params)

        # Detect which mock variables are actually used (if flag enabled)
        used_mocks = (
            check_variable_usage(original_node.body, mock_params)
            if self.skip_unused
            else set(mock_params)
        )

        # Build with statement items (reversed to match parameter order)
        with_items = []
        for i, (decorator, param_name) in enumerate(
            zip(reversed(patch_decorators), mock_params)
        ):
            # Convert decorator to call expression
            if isinstance(decorator.decorator, cst.Call):
                context_expr = decorator.decorator
            else:
                # Decorator without call - add empty call
                context_expr = cst.Call(func=decorator.decorator, args=[])

            # Add comma for all but the last item
            comma = cst.Comma() if i < num_patches - 1 else cst.MaybeSentinel.DEFAULT

            # Only add 'as' clause if variable is used
            if param_name in used_mocks:
                with_items.append(
                    cst.WithItem(
                        item=context_expr,
                        asname=cst.AsName(name=cst.Name(param_name)),
                        comma=comma,
                    )
                )
            else:
                with_items.append(
                    cst.WithItem(
                        item=context_expr,
                        comma=comma,
                    )
                )

        # Create with statement - use parentheses if multiple items
        if num_patches > 1:
            # Parenthesized with statement for clean multi-line
            with_stmt = cst.With(
                items=with_items,
                body=original_node.body,  # Use original to preserve comments
                lpar=cst.LeftParen(),
                rpar=cst.RightParen(),
            )
        else:
            # Single item, no parentheses needed
            with_stmt = cst.With(items=with_items, body=original_node.body)

        # Return updated function
        return updated_node.with_changes(
            decorators=other_decorators,
            params=new_params,
            body=cst.IndentedBlock(body=[with_stmt]),
        )


def rewrite_file(
    filename: str, check_only: bool, line_length: int, skip_unused: bool = False
) -> bool:
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

    try:
        module = cst.parse_module(source)
    except cst.ParserSyntaxError as e:
        print(f"Syntax error in {filename}: {e}", file=sys.stderr)
        return False

    # Set up metadata wrapper for position tracking
    wrapper = cst.metadata.MetadataWrapper(module)

    # Rewrite the module
    rewriter = PatchDecoratorRewriter(
        line_length, check_only=check_only, skip_unused=skip_unused
    )

    try:
        new_module = wrapper.visit(rewriter)
    except Exception as e:
        print(f"Error rewriting {filename}: {e}", file=sys.stderr)
        return False

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

    # Generate new code
    new_code = new_module.code

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
    parser.add_argument(
        "--skip-unused-mock-assignment",
        action="store_true",
        help="Skip 'as var_name' for mocks that are not used in the function body",
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
            if rewrite_file(
                filename, args.check_only, line_length, args.skip_unused_mock_assignment
            ):
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
