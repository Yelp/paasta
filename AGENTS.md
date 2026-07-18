# AGENTS.md

## Project
PaaSTA - container orchestration platform for deploying services on Kubernetes at Yelp.

## Workflow
1. **Plan first** - work with the user to write a plan/spec before implementing features
2. Write unit tests; also write acceptance tests (`general_itests/`, behave framework) when feasible
3. Run tests during iteration; `make test` before commits

## Setup
```bash
make dev           # create virtualenv via tox
make install-hooks # pre-commit hooks
```

Tox manages virtualenvs in `.tox/py310-linux/`. Use `tox` or `make` targets to ensure the env is built.

## Dependencies
- Dependencies are defined in `pyproject.toml` under `[project]` and `[project.optional-dependencies]` sections
- `[project].dependencies`: top-level production deps. Do not pin unless strictly necessary; if you must, use a loose lower-bound and note why.
- `[project.optional-dependencies].dev`: top-level dev deps. Do not pin unless strictly necessary; if you must, use a loose lower-bound and note why.
- Do not list transitive-only deps in pyproject.toml; those live in the fully pinned `requirements*.txt` files.
- `requirements.txt`: fully pinned production deps (including transitive), used for installs.
- `requirements-dev.txt`: fully pinned dev deps (including transitive), installed alongside `requirements.txt` in dev/test.
- For bulk dependency updates, regenerate the pinned files from pyproject.toml using standard pip tools.

## Testing
```bash
# Iterate with pytest directly
.tox/py310-linux/bin/pytest tests/path/to/test_foo.py -x

# Full suite before committing
make test         # pre-commit, mypy, pytest, coverage

# Integration tests
make itest        # acceptance tests (behave)
make k8s_itests   # kubernetes integration (requires Kind)
```

## Git & Commits
- **Bisectable history**: every commit passes tests and is independently releasable
- **Atomic commits**: one logical change per commit
- Large changes: break into a series of atomic commits, each functional and building toward the goal

## Code Style
Style is enforced by pre-commit (black, flake8, import ordering) and mypy.

```bash
# Iterate on specific files
.tox/py310-linux/bin/pre-commit run --files path/to/file.py
.tox/py310-linux/bin/mypy path/to/file.py

# Check all staged files
.tox/py310-linux/bin/pre-commit run

# Full check
make test
```

## Typing
- **Strongly typed by default.** Use granular types; avoid `Any`.
- Use advanced constructs where helpful: `Literal`, `TypedDict`, `TypeVar`, `Protocol`, etc.
- If interacting with code that exposes `Any`, constrain the type in your call flow.
- See `paasta_tools/utils.py` for `TypedDict` patterns (e.g., `TopologySpreadConstraintDict`).

## Config Schema Changes
When adding/modifying fields in service configs (yelpsoa-configs), you **must**:

1. Update the JSON schema in `paasta_tools/cli/schemas/`
2. Update the docs in `docs/source/yelpsoa_configs.rst`

Schema guidelines:
- Constrain tightly: prefer `enum`, `pattern` (regex), specific formats over bare `string`/`integer`
- Example commit: `97c04cbee` - adds `topology_spread_constraints` with enum constraint + TypedDict + tests

Schemas:
- `eks_schema.json` - kubernetes.yaml
- `kubernetes_schema.json` - legacy; do not modify
- `tron_schema.json` - tron jobs
- `smartstack_schema.json` - service discovery
- `adhoc_schema.json` - adhoc instances

## Code Conventions
- **No inline imports** - all imports at top of module
- **No module-level side effects** (other than imports)
- **Avoid adding to `paasta_tools/utils.py`** - it's already large; prefer more specific modules
- `mock.patch` must use `autospec=True` (enforced by pre-commit)

## Legacy Code
- `paasta_tools/mesos/` - **deprecated and unused**; do not modify or extend

## Structure
- `paasta_tools/` - main source
- `paasta_tools/cli/` - CLI subcommands
- `paasta_tools/kubernetes/` - K8s integration
- `paasta_tools/api/` - REST API
- `tests/` - pytest unit tests
- `general_itests/` - behave acceptance tests

## Docs
- `docs/source/` - Sphinx docs
- https://paasta.readthedocs.io
