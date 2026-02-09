"""ClickHouse query runner CLI application."""

from __future__ import annotations

import asyncio
import logging
import pathlib
import sys

from rich import console
from rich import logging as rich_logging

from clickhouse_query_runner import parser, runner, settings

LOGGER = logging.getLogger(__name__)


def setup_logging(
    rich_console: console.Console, verbose: bool = False
) -> None:
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(message)s',
        handlers=[
            rich_logging.RichHandler(
                console=rich_console, rich_tracebacks=True
            )
        ],
    )


def main() -> None:
    """Main entry point."""
    runner_settings = settings.RunnerSettings()
    rich_console = console.Console()
    setup_logging(rich_console, runner_settings.verbose)

    query_file = pathlib.Path(runner_settings.query_file)
    if not query_file.exists():
        rich_console.print(f'[red]Error: File not found: {query_file}[/red]')
        sys.exit(1)

    queries = parser.parse_sql_file(str(query_file))
    if not queries:
        rich_console.print(
            '[yellow]Query file is empty, nothing to do[/yellow]'
        )
        sys.exit(0)

    if runner_settings.dry_run:
        _display_dry_run(rich_console, queries)
        sys.exit(0)

    try:
        asyncio.run(_async_main(runner_settings, queries, rich_console))
    except KeyboardInterrupt:
        rich_console.print('\n[yellow]Operation cancelled[/yellow]')
        sys.exit(130)


async def _async_main(
    runner_settings: settings.RunnerSettings,
    queries: list[tuple[str, str]],
    rich_console: console.Console,
) -> None:
    """Async entry point for the runner."""
    query_runner = runner.QueryRunner(runner_settings)
    try:
        await query_runner.connect()

        if runner_settings.reset:
            count = await query_runner.reset()
            rich_console.print(
                f'Cleared {count} checkpoints for run '
                f'{query_runner.checkpoint_mgr.run_id}'
            )
            return

        success = await query_runner.run(queries, console=rich_console)

        if not success:
            failure = query_runner.failure
            if failure is not None:
                rich_console.print()
                rich_console.print(
                    '[red bold]Query execution failed[/red bold]'
                )
                rich_console.print(f'[red]Node:[/red] {failure.node}')
                rich_console.print(f'[red]Offset:[/red] {failure.offset}')
                rich_console.print(f'[red]Error:[/red] {failure.error}')
                rich_console.print('[red]Query:[/red]')
                rich_console.print(failure.query_text)
            sys.exit(1)

        rich_console.print('[green]All queries completed successfully[/green]')
    except (OSError, ValueError, TypeError, RuntimeError) as err:
        LOGGER.debug('Fatal error', exc_info=True)
        rich_console.print(f'[red]Error: {err}[/red]')
        sys.exit(1)
    finally:
        await query_runner.close()


def _display_dry_run(
    rich_console: console.Console, queries: list[tuple[str, str]]
) -> None:
    """Display parsed queries without executing."""
    rich_console.print(f'[bold]Parsed {len(queries)} queries:[/bold]\n')
    for i, (query_hash, query_text) in enumerate(queries):
        rich_console.print(f'[cyan]--- Query {i} ---[/cyan]')
        rich_console.print(f'[dim]Hash: {query_hash[:16]}[/dim]')
        preview = query_text[:200]
        if len(query_text) > 200:
            preview += '...'
        rich_console.print(preview)
        rich_console.print()
