# Contributing to RowQuery

Thank you for your interest in contributing! This document provides guidelines for contributing to RowQuery.

## Development Setup

### Prerequisites

- Python >=3.10
- [uv](https://github.com/astral-sh/uv) package manager

### Setup

```bash
# Clone repository
git clone https://github.com/maksim-shevtsov/RowQuery.git
cd RowQuery

# Install all dependencies (including all database drivers)
uv sync --extra all --extra dev

# Install pre-commit hooks
uv run pre-commit install
```

## Running Tests

```bash
# All tests
uv run pytest

# Unit tests only
uv run pytest tests/unit/

# Contract tests (adapter protocol compliance)
uv run pytest tests/contract/

# Integration tests (SQLite)
uv run pytest tests/integration/ -m integration

# With coverage
uv run pytest --cov=row_query --cov-report=html
```

## Code Quality

This project uses strict quality standards enforced by CI.

### Linting and Formatting

```bash
# Check for issues
uv run ruff check row_query/ tests/

# Auto-fix issues
uv run ruff check --fix row_query/ tests/

# Format code
uv run ruff format row_query/ tests/
```

### Type Checking

```bash
# Strict mypy type checking
uv run mypy row_query/
```

### Pre-commit Hooks

Pre-commit hooks run automatically on commit. To run manually:

```bash
uv run pre-commit run --all-files
```

## Branching Strategy

**`main` is protected** — direct commits and force-pushes are blocked. All changes must go through a pull request from a feature branch.

### Branch Naming

Use a short numeric prefix followed by a kebab-case description:

```
NNN-short-description
```

Examples:
```
001-sql-projection-engine
002-add-postgres-pooling
003-fix-aggregate-null-handling
```

### Workflow

```
main (protected)
 └── 001-my-feature        ← work here
      └── commits...
      └── open PR → review → merge into main
```

Never commit directly to `main`. Always:

```bash
git checkout main && git pull          # start from latest main
git checkout -b 042-my-feature         # create feature branch
# ... make changes ...
git push -u origin 42-my-feature       # push feature branch
gh pr create                           # open PR targeting main
```

Feature branches should be **rebased or merged up-to-date with `main`** before the PR can be merged (enforced by branch protection).

## Pull Request Process

### 1. Create a Feature Branch

```bash
git checkout main && git pull
git checkout -b NNN-short-description
```

### 2. Make Changes

- Write code following existing patterns
- Add tests for new functionality
- Update docstrings
- Update `CHANGELOG.md` under `[Unreleased]`
- Run quality checks locally

### 3. Commit

We encourage (but don't require) conventional commits:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `chore:` - Maintenance tasks

Examples:
```bash
git commit -m "feat: add support for custom query namespaces"
git commit -m "fix: handle null values in aggregate mapping"
git commit -m "docs: add examples for async transactions"
```

### 4. Test

- Ensure all tests pass: `uv run pytest`
- Verify no lint errors: `uv run ruff check row_query/ tests/`
- Verify formatting: `uv run ruff format --check row_query/ tests/`
- Check types: `uv run mypy row_query/`

### 5. Open a PR

```bash
git push -u origin NNN-short-description
gh pr create --base main
```

- Fill out the PR template
- Wait for CI to pass
- Address review comments
- Keep the branch up-to-date with `main` before merging

## Code Style Guidelines

- Follow PEP 8 (enforced by Ruff)
- Use type hints everywhere (enforced by mypy strict mode)
- Write docstrings for public APIs (Google style)
- Keep functions focused and testable
- Prefer composition over inheritance
- Use protocol classes for polymorphism

## Testing Guidelines

### Test Organization

- **Unit tests** (`tests/unit/`): Test individual functions/classes in isolation
- **Contract tests** (`tests/contract/`): Verify protocol compliance (adapters, mappers)
- **Integration tests** (`tests/integration/`): Test full workflows with real databases

### Best Practices

- Aim for 80%+ coverage
- Use pytest fixtures for common setup
- Mark integration tests: `@pytest.mark.integration`
- Test both sync and async code paths
- Test error conditions and edge cases

## Database Testing

- **SQLite**: Always test (in-memory, fast, no setup)
- **PostgreSQL**: Test locally if making adapter changes
- **MySQL/Oracle**: Test in specialized environments or CI

## Documentation

- Update README.md for user-facing changes
- Update CHANGELOG.md following [Keep a Changelog](https://keepachangelog.com) format
- Add examples in `examples/` for new features
- Keep CLAUDE.md updated for architecture changes

## Questions?

- Open a discussion on GitHub
- File an issue for bugs or feature requests

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
