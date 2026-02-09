"""Rich progress display for ClickHouse query runner."""

from __future__ import annotations

import collections
import time

from rich import console as rich_console
from rich import live, progress, table


class QueryProgress:
    """Manages the Rich live display for batch and per-query progress."""

    def __init__(
        self,
        total_queries: int,
        filename: str = '',
        console: rich_console.Console | None = None,
        concurrency: int = 1,
    ) -> None:
        self.total_queries = total_queries
        self.completed_count = 0
        self._concurrency = max(concurrency, 1)
        self._console = console or rich_console.Console()
        self._recent_times: collections.deque[float] = collections.deque(
            maxlen=10
        )
        self.batch_progress = progress.Progress(
            progress.SpinnerColumn(),
            progress.TextColumn('[progress.description]{task.description}'),
            progress.BarColumn(),
            progress.TaskProgressColumn(),
            progress.MofNCompleteColumn(),
            progress.TextColumn('queries'),
            progress.TextColumn('{task.fields[rate]}'),
            progress.TextColumn('{task.fields[eta]}'),
        )
        description = (
            f'Overall Progress for {filename}'
            if filename
            else ('Overall Progress')
        )
        self.batch_task = self.batch_progress.add_task(
            description, total=total_queries, rate='', eta=''
        )
        self._active_queries: dict[str, _ActiveQuery] = {}
        self._live: live.Live | None = None

    def __rich__(self) -> table.Table:
        """Support dynamic rendering by Rich Live auto-refresh."""
        return self._render()

    def start(self) -> live.Live:
        """Create and return the Live context manager."""
        self._live = live.Live(
            self, console=self._console, refresh_per_second=4
        )
        return self._live

    def mark_started(self, query_id: str, node: str, offset: int) -> None:
        """Record that a query has started executing."""
        self._active_queries[query_id] = _ActiveQuery(
            node=node, offset=offset, start_time=time.monotonic()
        )

    def update_query(
        self,
        query_id: str,
        read_rows: int,
        total_rows: int,
        written_rows: int,
        memory_usage: int,
    ) -> None:
        """Update progress for an active query."""
        if query_id in self._active_queries:
            aq = self._active_queries[query_id]
            aq.read_rows = read_rows
            aq.total_rows = total_rows
            aq.written_rows = written_rows
            aq.memory_usage = memory_usage

    def mark_completed(self, query_id: str, elapsed: float) -> None:
        """Record that a query has completed."""
        self._active_queries.pop(query_id, None)
        self._recent_times.append(elapsed)
        self.completed_count += 1
        rate = self._format_rate()
        eta = self._format_eta()
        self.batch_progress.update(
            self.batch_task, completed=self.completed_count, rate=rate, eta=eta
        )
        self._refresh()

    def mark_skipped(self, count: int) -> None:
        """Advance progress for skipped (checkpointed) queries."""
        self.completed_count += count
        self.batch_progress.update(
            self.batch_task, completed=self.completed_count
        )

    def _format_rate(self) -> str:
        """Format the rolling average rate."""
        if not self._recent_times:
            return ''
        avg = sum(self._recent_times) / len(self._recent_times)
        return f'{avg:.1f}s/q'

    def _format_eta(self) -> str:
        """Format estimated time remaining.

        Uses the rolling average per-query time divided by concurrency
        to estimate wall-clock time for remaining queries.
        """
        if not self._recent_times:
            return ''
        remaining = self.total_queries - self.completed_count
        if remaining <= 0:
            return '0:00'
        avg = sum(self._recent_times) / len(self._recent_times)
        seconds = remaining * avg / self._concurrency
        return _format_elapsed(seconds)

    def _render(self) -> table.Table:
        """Build the composite display."""
        layout = table.Table.grid()
        layout.add_row(self.batch_progress)
        if self._active_queries:
            query_table = table.Table(
                box=None, show_header=True, pad_edge=False
            )
            query_table.add_column('Node', style='cyan', min_width=16)
            query_table.add_column(
                'Offset', style='white', justify='right', min_width=6
            )
            query_table.add_column('Progress', min_width=36)
            query_table.add_column(
                'Rows', style='green', justify='right', min_width=14
            )
            query_table.add_column(
                'Memory', style='magenta', justify='right', min_width=10
            )
            query_table.add_column(
                'Elapsed', style='blue', justify='right', min_width=8
            )
            for aq in self._active_queries.values():
                rows, total = _query_metrics(aq)
                bar = progress.ProgressBar(
                    total=max(total, rows, 1), completed=rows, width=32
                )
                rows_str = _format_rows(rows, total)
                memory_str = _human_bytes(aq.memory_usage)
                elapsed_str = _format_elapsed(time.monotonic() - aq.start_time)
                query_table.add_row(
                    aq.node.split('.', maxsplit=1)[0],
                    str(aq.offset),
                    bar,
                    rows_str,
                    memory_str,
                    elapsed_str,
                )
            layout.add_row(query_table)
        return layout

    def _refresh(self) -> None:
        """Refresh the live display."""
        if self._live is not None:
            self._live.refresh()


class _ActiveQuery:
    """Track state of a currently executing query."""

    __slots__ = (
        'memory_usage',
        'node',
        'offset',
        'read_rows',
        'start_time',
        'total_rows',
        'written_rows',
    )

    def __init__(self, node: str, offset: int, start_time: float) -> None:
        self.node = node
        self.offset = offset
        self.start_time = start_time
        self.read_rows = 0
        self.total_rows = 0
        self.written_rows = 0
        self.memory_usage = 0


def _query_metrics(aq: _ActiveQuery) -> tuple[int, int]:
    """Pick the best available row metrics for display.

    Returns (active_rows, total_rows) preferring write metrics
    for write-heavy queries and read metrics otherwise.
    """
    if aq.written_rows > 0 and aq.written_rows >= aq.read_rows:
        return aq.written_rows, aq.total_rows
    return aq.read_rows, aq.total_rows


def _format_rows(active: int, total: int) -> str:
    """Format row counts for display."""
    if total:
        return f'{_human_number(active)} / {_human_number(total)}'
    if active:
        return _human_number(active)
    return ''


def _human_number(n: int) -> str:
    """Format a number with K/M/B suffixes."""
    if n >= 1_000_000_000:
        return f'{n / 1_000_000_000:.1f}B'
    if n >= 1_000_000:
        return f'{n / 1_000_000:.1f}M'
    if n >= 1_000:
        return f'{n / 1_000:.1f}K'
    return str(n)


def _human_bytes(n: int) -> str:
    """Format bytes with KiB/MiB/GiB suffixes."""
    if n >= 1_073_741_824:
        return f'{n / 1_073_741_824:.1f} GiB'
    if n >= 1_048_576:
        return f'{n / 1_048_576:.1f} MiB'
    if n >= 1_024:
        return f'{n / 1_024:.1f} KiB'
    if n > 0:
        return f'{n} B'
    return ''


def _format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as H:MM:SS or M:SS."""
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours:
        return f'{hours}:{minutes:02d}:{secs:02d}'
    return f'{minutes}:{secs:02d}'
