# tyvrana-core

Local orchestration and adapter connections for Tyvrana. Shared application
contracts are defined in the independent
[tyvrana-protocol](https://github.com/tyvrana/tyvrana-protocol) package.

## Development

Requires Python 3.12+ and uv. Create this repository's environment and install
the package and development dependencies:

```sh
uv sync --locked --python 3.12
```

The protocol dependency uses a public HTTPS Git URL pinned to a commit. The pin
is included in package metadata and `uv.lock`, so installations do not require a
sibling checkout or a package registry release.

Run the checks and build both package formats:

```sh
uv run --locked pytest
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy
uv build
```

To format Python files, run `uv run --locked ruff format .`.
