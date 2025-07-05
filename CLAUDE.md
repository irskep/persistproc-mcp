# Claude Development Notes

## Running Tests

Use `uv run python -m pytest` to run tests.

## Linting and Type Checking

- Linting: `uv run ruff check`
- Formatting: `uv run ruff format`

## Development Guidelines

**NEVER run persistproc commands manually during development**. The persistproc CLI should only be invoked through the test suite. Manual CLI usage can interfere with test servers and cause unexpected failures.

Instead:
- Use the test suite to verify functionality
- Use the test helpers in `tests/helpers.py` for programmatic testing
- Debug issues through test output and logging