"""Tests for Rich progress display."""

from __future__ import annotations

import unittest
from unittest import mock

from rich import live

from clickhouse_query_runner import progress


class QueryProgressInitTests(unittest.TestCase):
    """Tests for QueryProgress initialization."""

    def test_initial_state(self) -> None:
        qp = progress.QueryProgress(10)
        self.assertEqual(qp.total_queries, 10)
        self.assertEqual(qp.completed_count, 0)
        self.assertIsNone(qp._live)

    def test_start_returns_live(self) -> None:
        qp = progress.QueryProgress(5)
        result = qp.start()
        self.assertIsInstance(result, live.Live)
        self.assertIsNotNone(qp._live)


class RichProtocolTests(unittest.TestCase):
    """Tests for Rich protocol support."""

    def test_rich_returns_renderable(self) -> None:
        qp = progress.QueryProgress(5)
        result = qp.__rich__()
        self.assertIsNotNone(result)

    def test_rich_reflects_active_queries(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        result = qp.__rich__()
        self.assertIsNotNone(result)


class QueryProgressTrackingTests(unittest.TestCase):
    """Tests for query tracking."""

    def test_mark_started(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        self.assertIn('q1', qp._active_queries)
        aq = qp._active_queries['q1']
        self.assertEqual(aq.node, 'node-a')
        self.assertEqual(aq.offset, 0)

    def test_update_query_read(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp.update_query(
            'q1',
            read_rows=500,
            total_rows=1000,
            written_rows=0,
            memory_usage=4096,
        )
        aq = qp._active_queries['q1']
        self.assertEqual(aq.read_rows, 500)
        self.assertEqual(aq.total_rows, 1000)
        self.assertEqual(aq.memory_usage, 4096)

    def test_update_query_write(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp.update_query(
            'q1',
            read_rows=0,
            total_rows=1000,
            written_rows=200,
            memory_usage=8192,
        )
        aq = qp._active_queries['q1']
        self.assertEqual(aq.written_rows, 200)
        self.assertEqual(aq.memory_usage, 8192)

    def test_update_unknown_query(self) -> None:
        qp = progress.QueryProgress(5)
        qp.update_query(
            'unknown',
            read_rows=500,
            total_rows=1000,
            written_rows=0,
            memory_usage=0,
        )
        self.assertNotIn('unknown', qp._active_queries)

    def test_mark_completed(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp.mark_completed('q1', 1.5)
        self.assertNotIn('q1', qp._active_queries)
        self.assertEqual(qp.completed_count, 1)

    def test_mark_completed_unknown(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_completed('unknown', 1.0)
        self.assertEqual(qp.completed_count, 1)

    def test_mark_skipped(self) -> None:
        qp = progress.QueryProgress(10)
        qp.mark_skipped(3)
        self.assertEqual(qp.completed_count, 3)

    def test_mark_skipped_then_completed(self) -> None:
        qp = progress.QueryProgress(10)
        qp.mark_skipped(3)
        qp.mark_completed('q1', 1.0)
        self.assertEqual(qp.completed_count, 4)


class FormatRateTests(unittest.TestCase):
    """Tests for _format_rate."""

    def test_no_times(self) -> None:
        qp = progress.QueryProgress(5)
        self.assertEqual(qp._format_rate(), '')

    def test_single_time(self) -> None:
        qp = progress.QueryProgress(5)
        qp._recent_times.append(2.0)
        self.assertEqual(qp._format_rate(), '2.0s/q')

    def test_multiple_times_averaged(self) -> None:
        qp = progress.QueryProgress(5)
        qp._recent_times.append(1.0)
        qp._recent_times.append(3.0)
        self.assertEqual(qp._format_rate(), '2.0s/q')


class FormatEtaTests(unittest.TestCase):
    """Tests for _format_eta."""

    def test_no_times(self) -> None:
        qp = progress.QueryProgress(10, concurrency=2)
        self.assertEqual(qp._format_eta(), '')

    def test_calculates_with_concurrency(self) -> None:
        qp = progress.QueryProgress(100, concurrency=4)
        qp.completed_count = 10
        qp._recent_times.append(60.0)
        # 90 remaining * 60s / 4 concurrency = 1350s = 22:30
        self.assertEqual(qp._format_eta(), '22:30')

    def test_all_complete(self) -> None:
        qp = progress.QueryProgress(5, concurrency=2)
        qp.completed_count = 5
        qp._recent_times.append(10.0)
        self.assertEqual(qp._format_eta(), '0:00')

    def test_single_concurrency(self) -> None:
        qp = progress.QueryProgress(10, concurrency=1)
        qp.completed_count = 5
        qp._recent_times.append(10.0)
        # 5 remaining * 10s / 1 = 50s
        self.assertEqual(qp._format_eta(), '0:50')

    def test_hours_format(self) -> None:
        qp = progress.QueryProgress(500, concurrency=4)
        qp.completed_count = 10
        qp._recent_times.append(65.0)
        # 490 remaining * 65s / 4 = 7962.5s ≈ 2:12:42
        self.assertEqual(qp._format_eta(), '2:12:42')


class RenderTests(unittest.TestCase):
    """Tests for _render."""

    def test_render_without_active_queries(self) -> None:
        qp = progress.QueryProgress(5)
        layout = qp._render()
        self.assertIsNotNone(layout)

    def test_render_with_read_progress(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp._active_queries['q1'].read_rows = 500
        qp._active_queries['q1'].total_rows = 1000
        qp._active_queries['q1'].memory_usage = 4096
        layout = qp._render()
        self.assertIsNotNone(layout)

    def test_render_with_write_progress(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp._active_queries['q1'].written_rows = 200
        qp._active_queries['q1'].total_rows = 1000
        qp._active_queries['q1'].memory_usage = 8192
        layout = qp._render()
        self.assertIsNotNone(layout)

    def test_render_with_no_progress(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        layout = qp._render()
        self.assertIsNotNone(layout)


class RefreshTests(unittest.TestCase):
    """Tests for _refresh."""

    def test_refresh_without_live(self) -> None:
        qp = progress.QueryProgress(5)
        qp._refresh()  # Should not raise

    def test_refresh_with_live(self) -> None:
        qp = progress.QueryProgress(5)
        qp._live = mock.MagicMock()
        qp._refresh()
        qp._live.refresh.assert_called_once()


class QueryMetricsTests(unittest.TestCase):
    """Tests for _query_metrics."""

    def test_read_only(self) -> None:
        aq = progress._ActiveQuery('n1', 0, 100.0)
        aq.read_rows = 500
        aq.total_rows = 1000
        rows, total = progress._query_metrics(aq)
        self.assertEqual(rows, 500)
        self.assertEqual(total, 1000)

    def test_write_only(self) -> None:
        aq = progress._ActiveQuery('n1', 0, 100.0)
        aq.written_rows = 200
        aq.total_rows = 1000
        rows, total = progress._query_metrics(aq)
        self.assertEqual(rows, 200)
        self.assertEqual(total, 1000)

    def test_both_prefers_larger(self) -> None:
        aq = progress._ActiveQuery('n1', 0, 100.0)
        aq.read_rows = 100
        aq.written_rows = 200
        aq.total_rows = 1000
        rows, total = progress._query_metrics(aq)
        self.assertEqual(rows, 200)

    def test_no_activity(self) -> None:
        aq = progress._ActiveQuery('n1', 0, 100.0)
        rows, total = progress._query_metrics(aq)
        self.assertEqual(rows, 0)
        self.assertEqual(total, 0)


class HumanNumberTests(unittest.TestCase):
    """Tests for _human_number."""

    def test_small(self) -> None:
        self.assertEqual(progress._human_number(0), '0')
        self.assertEqual(progress._human_number(999), '999')

    def test_thousands(self) -> None:
        self.assertEqual(progress._human_number(1000), '1.0K')
        self.assertEqual(progress._human_number(1500), '1.5K')
        self.assertEqual(progress._human_number(999_999), '1000.0K')

    def test_millions(self) -> None:
        self.assertEqual(progress._human_number(1_000_000), '1.0M')
        self.assertEqual(progress._human_number(2_500_000), '2.5M')

    def test_billions(self) -> None:
        self.assertEqual(progress._human_number(1_000_000_000), '1.0B')
        self.assertEqual(progress._human_number(3_500_000_000), '3.5B')


class HumanBytesTests(unittest.TestCase):
    """Tests for _human_bytes."""

    def test_zero(self) -> None:
        self.assertEqual(progress._human_bytes(0), '')

    def test_bytes(self) -> None:
        self.assertEqual(progress._human_bytes(512), '512 B')

    def test_kibibytes(self) -> None:
        self.assertEqual(progress._human_bytes(1024), '1.0 KiB')
        self.assertEqual(progress._human_bytes(1536), '1.5 KiB')

    def test_mebibytes(self) -> None:
        self.assertEqual(progress._human_bytes(1_048_576), '1.0 MiB')

    def test_gibibytes(self) -> None:
        self.assertEqual(progress._human_bytes(1_073_741_824), '1.0 GiB')


class FormatRowsTests(unittest.TestCase):
    """Tests for _format_rows."""

    def test_format_with_total(self) -> None:
        self.assertEqual(progress._format_rows(500, 999), '500 / 999')

    def test_format_large(self) -> None:
        self.assertEqual(
            progress._format_rows(1_500_000, 4_800_000), '1.5M / 4.8M'
        )

    def test_format_active_only(self) -> None:
        self.assertEqual(progress._format_rows(500, 0), '500')

    def test_format_none(self) -> None:
        self.assertEqual(progress._format_rows(0, 0), '')


class FormatElapsedTests(unittest.TestCase):
    """Tests for _format_elapsed."""

    def test_seconds_only(self) -> None:
        self.assertEqual(progress._format_elapsed(5.0), '0:05')

    def test_minutes_and_seconds(self) -> None:
        self.assertEqual(progress._format_elapsed(65.0), '1:05')

    def test_hours(self) -> None:
        self.assertEqual(progress._format_elapsed(3661.0), '1:01:01')

    def test_zero(self) -> None:
        self.assertEqual(progress._format_elapsed(0.0), '0:00')


class ActiveQueryTests(unittest.TestCase):
    """Tests for _ActiveQuery."""

    def test_defaults(self) -> None:
        aq = progress._ActiveQuery('node1', 5, 100.0)
        self.assertEqual(aq.node, 'node1')
        self.assertEqual(aq.offset, 5)
        self.assertEqual(aq.start_time, 100.0)
        self.assertEqual(aq.read_rows, 0)
        self.assertEqual(aq.total_rows, 0)
        self.assertEqual(aq.written_rows, 0)
        self.assertEqual(aq.memory_usage, 0)
