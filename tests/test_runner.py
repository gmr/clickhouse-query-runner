"""Tests for the async execution engine."""

from __future__ import annotations

import asyncio
import contextlib
import unittest
from unittest import mock

from asynch import errors as asynch_errors

from clickhouse_query_runner import runner


def _make_settings(**overrides: object) -> mock.MagicMock:
    """Create a mock RunnerSettings."""
    defaults = {
        'host': 'node1,node2',
        'port': 9440,
        'database': 'default',
        'user': 'user',
        'password': mock.MagicMock(get_secret_value=lambda: 'pass'),
        'secure': True,
        'concurrency': 2,
        'run_id': 'test-run',
        'query_file': 'test.sql',
        'valkey_url': 'redis://localhost:6379/0',
        'checkpoint_ttl': 604800,
        'poll_interval': 0.01,
        'cancel_on_failure': False,
        'dry_run': False,
        'reset': False,
        'verbose': False,
    }
    defaults.update(overrides)
    s = mock.MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


class _MockCursor:
    """Async context manager cursor mock."""

    def __init__(
        self,
        rowcount: int = 0,
        execute_side_effect: BaseException | None = None,
        fetchone_return: object = None,
        fetchall_return: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.rowcount = rowcount
        self._execute_side_effect = execute_side_effect
        self._fetchone_return = fetchone_return
        self._fetchall_return = fetchall_return or []
        self._query_id = ''
        self.set_query_id = mock.MagicMock(
            side_effect=lambda qid='': setattr(self, '_query_id', qid)
        )
        self.execute = mock.AsyncMock(side_effect=execute_side_effect)
        self.fetchone = mock.AsyncMock(return_value=fetchone_return)
        self.fetchall = mock.AsyncMock(return_value=self._fetchall_return)

    async def __aenter__(self) -> _MockCursor:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False


def _mock_conn(
    rowcount: int = 0,
    execute_side_effect: BaseException | None = None,
    fetchone_return: object = None,
    fetchall_return: list[tuple[object, ...]] | None = None,
) -> mock.MagicMock:
    """Create a mock asynch Connection.

    cursor() is a sync method returning an async context manager,
    so we use MagicMock (not AsyncMock) for the connection and set
    cursor's return_value to the _MockCursor directly.
    """
    cursor = _MockCursor(
        rowcount=rowcount,
        execute_side_effect=execute_side_effect,
        fetchone_return=fetchone_return,
        fetchall_return=fetchall_return,
    )
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    conn.close = mock.AsyncMock()
    conn.connect = mock.AsyncMock()
    return conn


def _make_pool(*conns: mock.MagicMock) -> asyncio.Queue[mock.MagicMock]:
    """Create an asyncio.Queue connection pool from mock connections."""
    pool: asyncio.Queue[mock.MagicMock] = asyncio.Queue()
    for conn in conns:
        pool.put_nowait(conn)
    return pool


class QueryRunnerInitTests(unittest.TestCase):
    """Tests for QueryRunner initialization."""

    def test_nodes_parsed(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='a, b, c'))
        self.assertEqual(qr.nodes, ['a', 'b', 'c'])

    def test_single_node(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='single'))
        self.assertEqual(qr.nodes, ['single'])

    def test_initial_state(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        self.assertIsNone(qr._failure)
        self.assertFalse(qr._stop_dispatch)
        self.assertIsNone(qr._progress)

    def test_failure_property(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        self.assertIsNone(qr.failure)


class QueryRunnerConnectTests(unittest.IsolatedAsyncioTestCase):
    """Tests for connect."""

    async def test_connects_all_nodes(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1,n2'))
        mock_conn = mock.AsyncMock()
        with (
            mock.patch('asynch.connection.Connection', return_value=mock_conn),
            mock.patch.object(
                qr.checkpoint_mgr, 'connect', new_callable=mock.AsyncMock
            ),
        ):
            await qr.connect()
        self.assertEqual(len(qr._conn_pools), 2)
        self.assertIn('n1', qr._conn_pools)
        self.assertIn('n2', qr._conn_pools)

    async def test_creates_enough_connections(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1,n2', concurrency=4))
        mock_conn = mock.AsyncMock()
        with (
            mock.patch('asynch.connection.Connection', return_value=mock_conn),
            mock.patch.object(
                qr.checkpoint_mgr, 'connect', new_callable=mock.AsyncMock
            ),
        ):
            await qr.connect()
        # concurrency=4, 2 nodes => 2 connections per node
        self.assertEqual(qr._conn_pools['n1'].qsize(), 2)
        self.assertEqual(qr._conn_pools['n2'].qsize(), 2)

    async def test_creates_poll_connections(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1,n2'))
        mock_conn = mock.AsyncMock()
        with (
            mock.patch('asynch.connection.Connection', return_value=mock_conn),
            mock.patch.object(
                qr.checkpoint_mgr, 'connect', new_callable=mock.AsyncMock
            ),
        ):
            await qr.connect()
        self.assertIn('n1', qr._poll_conns)
        self.assertIn('n2', qr._poll_conns)


class QueryRunnerCloseTests(unittest.IsolatedAsyncioTestCase):
    """Tests for close."""

    async def test_close_connections(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        conn = mock.AsyncMock()
        qr._conn_pools = {'n1': _make_pool(conn)}
        qr._poll_conns = {}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()
        conn.close.assert_awaited_once()

    async def test_close_handles_oserror(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        conn = mock.AsyncMock()
        conn.close.side_effect = OSError('fail')
        qr._conn_pools = {'n1': _make_pool(conn)}
        qr._poll_conns = {}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()  # Should not raise

    async def test_close_cancels_poll_task(self) -> None:
        qr = runner.QueryRunner(_make_settings())

        async def _noop() -> None:
            await asyncio.sleep(10)

        poll_task = asyncio.create_task(_noop())
        qr._poll_task = poll_task
        qr._conn_pools = {}
        qr._poll_conns = {}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()
        self.assertTrue(poll_task.cancelled())

    async def test_close_poll_connections(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        poll_conn = mock.AsyncMock()
        qr._conn_pools = {}
        qr._poll_conns = {'n1': poll_conn}
        with mock.patch.object(
            qr.checkpoint_mgr, 'close', new_callable=mock.AsyncMock
        ):
            await qr.close()
        poll_conn.close.assert_awaited_once()


class QueryRunnerRunTests(unittest.IsolatedAsyncioTestCase):
    """Tests for run."""

    async def test_all_completed_returns_true(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        with mock.patch.object(
            qr.checkpoint_mgr,
            'load_completed',
            return_value={'hash1', 'hash2'},
        ):
            result = await qr.run([('hash1', 'Q1'), ('hash2', 'Q2')])
        self.assertTrue(result)

    async def test_skips_completed_queries(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(concurrency=1, poll_interval=0.001, host='n1,n2')
        )
        conn = _mock_conn(rowcount=0)
        qr._conn_pools = {
            'n1': _make_pool(conn),
            'n2': _make_pool(_mock_conn()),
        }
        qr._poll_conns = {
            'n1': _mock_conn(fetchall_return=[]),
            'n2': _mock_conn(fetchall_return=[]),
        }
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'load_completed', return_value={'hash1'}
            ),
            mock.patch.object(
                qr.checkpoint_mgr,
                'mark_completed',
                new_callable=mock.AsyncMock,
            ),
        ):
            result = await qr.run([('hash1', 'Q1'), ('hash2', 'Q2')])
        self.assertTrue(result)

    async def test_run_returns_false_on_failure(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(host='n1', concurrency=1, poll_interval=0.001)
        )
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException(
                'syntax error', 62
            )
        )
        qr._conn_pools = {'n1': _make_pool(conn)}
        qr._poll_conns = {'n1': _mock_conn(fetchall_return=[])}
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'load_completed', return_value=set()
            ),
            mock.patch.object(
                qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
            ),
        ):
            result = await qr.run([('hash1', 'BAD QUERY')])
        self.assertFalse(result)
        self.assertIsNotNone(qr.failure)
        self.assertEqual(qr.failure.node, 'n1')

    async def test_run_detects_unexpected_exception(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(host='n1', concurrency=1, poll_interval=0.001)
        )
        conn = _mock_conn(execute_side_effect=TypeError('unexpected'))
        qr._conn_pools = {'n1': _make_pool(conn)}
        qr._poll_conns = {'n1': _mock_conn(fetchall_return=[])}
        with mock.patch.object(
            qr.checkpoint_mgr, 'load_completed', return_value=set()
        ):
            result = await qr.run([('hash1', 'SELECT 1')])
        self.assertFalse(result)
        self.assertIsNotNone(qr.failure)
        self.assertIn('TypeError', qr.failure.error)


class QueryRunnerResetTests(unittest.IsolatedAsyncioTestCase):
    """Tests for reset."""

    async def test_delegates_to_checkpoint_mgr(self) -> None:
        qr = runner.QueryRunner(_make_settings())
        with mock.patch.object(qr.checkpoint_mgr, 'reset', return_value=5):
            result = await qr.reset()
        self.assertEqual(result, 5)


class ExecuteQueryTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _execute_query."""

    async def test_successful_execution(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(rowcount=42)
        qr._conn_pools = {'n1': _make_pool(conn)}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_completed', new_callable=mock.AsyncMock
        ) as mark_mock:
            await qr._execute_query('n1', 0, 'hash1', 'SELECT 1', semaphore)
        mark_mock.assert_awaited_once()
        self.assertIsNone(qr._failure)
        # Connection returned to pool
        self.assertEqual(qr._conn_pools['n1'].qsize(), 1)

    async def test_failed_execution_sets_failure(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException('bad', 62)
        )
        qr._conn_pools = {'n1': _make_pool(conn)}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'BAD', semaphore)
        self.assertTrue(qr._stop_dispatch)
        self.assertIsNotNone(qr._failure)
        # Connection returned to pool even on failure
        self.assertEqual(qr._conn_pools['n1'].qsize(), 1)

    async def test_failed_with_cancel_on_failure(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(
                host='n1', cancel_on_failure=True, poll_interval=0.001
            )
        )
        conn = _mock_conn(
            execute_side_effect=asynch_errors.ServerException('bad', 62)
        )
        qr._conn_pools = {'n1': _make_pool(conn)}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with (
            mock.patch.object(
                qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
            ),
            mock.patch.object(
                qr, '_cancel_in_flight', new_callable=mock.AsyncMock
            ) as cancel_mock,
        ):
            await qr._execute_query('n1', 0, 'hash1', 'BAD', semaphore)
        cancel_mock.assert_awaited_once()

    async def test_updates_progress(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(rowcount=0)
        qr._conn_pools = {'n1': _make_pool(conn)}
        mock_progress = mock.MagicMock()
        qr._progress = mock_progress
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_completed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'Q', semaphore)
        mock_progress.mark_started.assert_called_once()
        mock_progress.mark_completed.assert_called_once()

    async def test_sets_query_id_on_cursor(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(rowcount=0)
        qr._conn_pools = {'n1': _make_pool(conn)}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_completed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'Q', semaphore)
        cursor = conn.cursor.return_value
        self.assertNotEqual(cursor._query_id, '')
        # execute should be called with query text only, not query_id kwarg
        cursor.execute.assert_awaited_once_with('Q')

    async def test_oserror_sets_failure(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        conn = _mock_conn(execute_side_effect=OSError('disconnect'))
        qr._conn_pools = {'n1': _make_pool(conn)}
        semaphore = asyncio.Semaphore(1)
        await semaphore.acquire()
        with mock.patch.object(
            qr.checkpoint_mgr, 'mark_failed', new_callable=mock.AsyncMock
        ):
            await qr._execute_query('n1', 0, 'hash1', 'Q', semaphore)
        self.assertIsNotNone(qr.failure)
        self.assertIn('disconnect', qr.failure.error)


class PollAllProgressTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _poll_all_progress."""

    async def test_cancellation(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        qr._poll_conns = {'n1': _mock_conn(fetchall_return=[])}
        task = asyncio.create_task(qr._poll_all_progress())
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self.assertTrue(task.done())

    async def test_updates_progress_on_rows(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        # Tuple: (query_id, read_rows, total_rows_approx, elapsed,
        #         written_rows, memory_usage)
        qr._poll_conns = {
            'n1': _mock_conn(
                fetchall_return=[('qid-1', 500, 1000, 2.5, 200, 4096)]
            )
        }
        mock_progress = mock.MagicMock()
        qr._progress = mock_progress
        task = asyncio.create_task(qr._poll_all_progress())
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        mock_progress.update_query.assert_called()
        call_kwargs = mock_progress.update_query.call_args
        self.assertEqual(call_kwargs.kwargs['query_id'], 'qid-1')
        self.assertEqual(call_kwargs.kwargs['read_rows'], 500)

    async def test_handles_server_error(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1', poll_interval=0.001))
        qr._poll_conns = {
            'n1': _mock_conn(
                execute_side_effect=asynch_errors.ServerException('err', 62)
            )
        }
        task = asyncio.create_task(qr._poll_all_progress())
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def test_multiple_nodes(self) -> None:
        qr = runner.QueryRunner(
            _make_settings(host='n1,n2', poll_interval=0.001)
        )
        qr._poll_conns = {
            'n1': _mock_conn(fetchall_return=[('q1', 100, 500, 1.0, 0, 1024)]),
            'n2': _mock_conn(fetchall_return=[('q2', 0, 0, 0.5, 50, 512)]),
        }
        mock_progress = mock.MagicMock()
        qr._progress = mock_progress
        task = asyncio.create_task(qr._poll_all_progress())
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self.assertGreaterEqual(mock_progress.update_query.call_count, 2)


class CancelInFlightTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _cancel_in_flight."""

    async def test_sends_kill_query(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1'))
        conn = _mock_conn()
        with mock.patch('asynch.connection.Connection', return_value=conn):
            await qr._cancel_in_flight()
        cursor = conn.cursor.return_value
        cursor.execute.assert_awaited_once()

    async def test_handles_error(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1'))
        conn = _mock_conn(execute_side_effect=OSError('fail'))
        with mock.patch('asynch.connection.Connection', return_value=conn):
            await qr._cancel_in_flight()  # Should not raise

    async def test_handles_unexpected_packet_error(self) -> None:
        qr = runner.QueryRunner(_make_settings(host='n1'))
        conn = _mock_conn(
            execute_side_effect=(
                asynch_errors.UnexpectedPacketFromServerError('bad')
            )
        )
        with mock.patch('asynch.connection.Connection', return_value=conn):
            await qr._cancel_in_flight()  # Should not raise


class QueryFailureTests(unittest.TestCase):
    """Tests for _QueryFailure."""

    def test_fields(self) -> None:
        f = runner._QueryFailure(
            query_text='SELECT 1', node='n1', error='bad', offset=3
        )
        self.assertEqual(f.query_text, 'SELECT 1')
        self.assertEqual(f.node, 'n1')
        self.assertEqual(f.error, 'bad')
        self.assertEqual(f.offset, 3)
