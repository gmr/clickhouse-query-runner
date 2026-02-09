# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Development
- `uv sync --group dev` - Install package with dev dependencies
- `uv run ruff check` - Run linting checks
- `uv run ruff format` - Format code (uses single quotes, 79 char line length)
- `uv run pre-commit run --all-files` - Run pre-commit hooks on all files

### Testing & Coverage
- `uv run pytest` - Run tests
- `uv run pytest --cov --cov-report=term-missing` - Run tests with coverage (minimum 90%)
- Tests use `unittest` framework with `pytest` as the runner
- Async tests use `unittest.IsolatedAsyncioTestCase`

### Running the Tool
- `uv run clickhouse-query-runner --help` - Show CLI help and all options
- `uv run clickhouse-query-runner queries.sql` - Run queries from a file
- `uv run clickhouse-query-runner --dry-run queries.sql` - Parse without executing

## Architecture

A CLI tool for executing SQL queries from a file against a ClickHouse cluster with parallel execution, checkpoint/resume via Valkey, and Rich progress monitoring.

### Core Components

**Settings (`settings.py`)**
- Uses Pydantic Settings with `env_prefix='CLICKHOUSE_'` and `cli_parse_args=True`
- Connection fields use `CLICKHOUSE_*` env vars; Valkey URL uses `VALKEY_URL`
- `query_file` is a `CliPositionalArg` (not a flag)
- Boolean flags require explicit `True`/`False` (pydantic-settings limitation)

**Runner (`runner.py`)**
- Core async execution engine with round-robin dispatch across cluster nodes
- Concurrency controlled by `asyncio.Semaphore`
- Polls `system.processes` for per-query progress updates
- `_QueryFailure` is a `NamedTuple` holding failure details

**Checkpoint (`checkpoint.py`)**
- Async Valkey client (`valkey.asyncio`) for checkpoint persistence
- Graceful degradation: continues without checkpointing if Valkey is unavailable
- Raises `OSError` if Valkey unreachable for >30 seconds
- Keys follow pattern `cqr:{run_id}:{query_hash}`

**Parser (`parser.py`)**
- Splits SQL files on semicolons while respecting string literals and comments
- SHA-256 hash of each statement for checkpoint identity
- Handles single-line (`--`), block (`/* */`) comments, and quoted strings

**Progress (`progress.py`)**
- Rich Live display with batch progress bar and per-query detail table
- Tracks active queries, completion rate, and estimated time remaining

**CLI (`cli.py`)**
- Entry point: file validation, dry-run display, async main, keyboard interrupt handling
- Exit codes: 0 (success), 1 (error), 130 (keyboard interrupt)

### Key Patterns

- **asynch driver**: `conn.cursor()` is sync returning an async context manager. Use `cursor.set_query_id(query_id)` before `execute()`. `fetchone()`/`fetchall()` are async and must be awaited. Uses `%(name)s` parameter style.
- **Exception handling**: Catch `(OSError, asynch_errors.ServerException, asynch_errors.UnexpectedPacketFromServerError)` for ClickHouse operations, `(OSError, valkey.exceptions.ValkeyError)` for Valkey. Never bare `except:`.
- **Mocking asynch in tests**: Use `MagicMock` (not `AsyncMock`) for connections since `cursor()` is sync. Use a custom `_MockCursor` class implementing `__aenter__`/`__aexit__`.
