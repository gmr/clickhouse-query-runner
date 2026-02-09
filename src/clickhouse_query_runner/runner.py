"""Core async execution engine for ClickHouse query runner."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import itertools
import logging
import time
import typing
import uuid

from asynch import connection as asynch_connection
from asynch import errors as asynch_errors
from rich import console as rich_console

from clickhouse_query_runner import checkpoint, progress, settings

LOGGER = logging.getLogger(__name__)

PROGRESS_QUERY = """\
SELECT query_id,
       read_rows,
       total_rows_approx,
       elapsed,
       written_rows,
       memory_usage
  FROM system.processes
 WHERE user = currentUser()
   AND query_id != ''"""


class QueryRunner:
    """Executes SQL queries against a ClickHouse cluster."""

    def __init__(self, runner_settings: settings.RunnerSettings) -> None:
        self.settings = runner_settings
        self.nodes = [h.strip() for h in runner_settings.host.split(',')]
        self._conn_pools: dict[
            str, asyncio.Queue[asynch_connection.Connection]
        ] = {}
        self.node_cycle = itertools.cycle(self.nodes)
        self.checkpoint_mgr = checkpoint.CheckpointManager(
            valkey_url=runner_settings.valkey_url,
            run_id=runner_settings.run_id,
            file_path=runner_settings.query_file,
            ttl=runner_settings.checkpoint_ttl,
        )
        self._failure: _QueryFailure | None = None
        self._stop_dispatch = False
        self._progress: progress.QueryProgress | None = None
        self._poll_conns: dict[str, asynch_connection.Connection] = {}
        self._poll_task: asyncio.Task[None] | None = None

    def _conn_kwargs(self, node: str) -> dict[str, object]:
        """Return connection kwargs for a node."""
        return {
            'host': node,
            'port': self.settings.port,
            'database': self.settings.database,
            'user': self.settings.user,
            'password': self.settings.password.get_secret_value(),
            'secure': self.settings.secure,
            'client_name': 'clickhouse-query-runner',
        }

    async def connect(self) -> None:
        """Establish connection pools for all ClickHouse nodes."""
        conns_per_node = max(
            1, -(-self.settings.concurrency // len(self.nodes))
        )
        for node in self.nodes:
            pool: asyncio.Queue[asynch_connection.Connection] = asyncio.Queue()
            try:
                for _ in range(conns_per_node):
                    conn = asynch_connection.Connection(
                        **self._conn_kwargs(node)
                    )
                    await conn.connect()
                    pool.put_nowait(conn)
                poll_conn = asynch_connection.Connection(
                    **self._conn_kwargs(node)
                )
                await poll_conn.connect()
            except (
                OSError,
                asynch_errors.ServerException,
                asynch_errors.UnexpectedPacketFromServerError,
            ):
                while not pool.empty():
                    with contextlib.suppress(OSError):
                        await pool.get_nowait().close()
                raise
            self._conn_pools[node] = pool
            self._poll_conns[node] = poll_conn
            LOGGER.debug(
                'Connected %d + 1 poll connections to %s', conns_per_node, node
            )
        await self.checkpoint_mgr.connect()

    async def close(self) -> None:
        """Close all connections."""
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, OSError):
                await self._poll_task
        for node, pool in self._conn_pools.items():
            while not pool.empty():
                conn = pool.get_nowait()
                try:
                    await conn.close()
                except OSError:
                    LOGGER.debug('Error closing connection to %s', node)
        for node, conn in self._poll_conns.items():
            try:
                await conn.close()
            except OSError:
                LOGGER.debug('Error closing poll connection to %s', node)
        await self.checkpoint_mgr.close()

    async def run(
        self,
        queries: list[tuple[str, str]],
        console: rich_console.Console | None = None,
    ) -> bool:
        """Execute queries with concurrency control.

        Args:
            queries: List of (query_hash, query_text) tuples.
            console: Optional Rich console for progress display.

        Returns:
            True if all queries completed successfully.
        """
        completed = await self.checkpoint_mgr.load_completed()
        pending = [
            (i, qh, qt)
            for i, (qh, qt) in enumerate(queries)
            if qh not in completed
        ]
        skipped = len(queries) - len(pending)
        if skipped:
            LOGGER.debug('Skipping %d already-completed queries', skipped)

        if not pending:
            LOGGER.info('All queries already completed, nothing to do')
            return True

        filename = str(self.settings.query_file).rsplit('/', maxsplit=1)[-1]
        self._progress = progress.QueryProgress(
            len(queries),
            filename=filename,
            console=console,
            concurrency=self.settings.concurrency,
        )
        if skipped:
            self._progress.mark_skipped(skipped)

        live_ctx = self._progress.start()
        semaphore = asyncio.Semaphore(self.settings.concurrency)
        tasks: list[asyncio.Task[None]] = []

        self._poll_task = asyncio.create_task(self._poll_all_progress())

        with live_ctx:
            for offset, query_hash, query_text in pending:
                if self._stop_dispatch:
                    break
                await semaphore.acquire()
                if self._stop_dispatch:
                    semaphore.release()
                    break
                node = next(self.node_cycle)
                task = asyncio.create_task(
                    self._execute_query(
                        node, offset, query_hash, query_text, semaphore
                    )
                )
                tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        self._stop_dispatch = True
                        if self._failure is None:
                            self._failure = _QueryFailure(
                                query_text='<unknown>',
                                node='<unknown>',
                                error=f'{type(result).__name__}: {result}',
                                offset=-1,
                            )

        self._poll_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, OSError):
            await self._poll_task

        return self._failure is None

    async def reset(self) -> int:
        """Clear all checkpoints for this run."""
        return await self.checkpoint_mgr.reset()

    async def _execute_query(
        self,
        node: str,
        offset: int,
        query_hash: str,
        query_text: str,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """Execute a single query on the specified node."""
        query_id = str(uuid.uuid4())
        started_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
        start_time = time.monotonic()

        if self._progress is not None:
            self._progress.mark_started(query_id, node, offset)

        conn = await self._conn_pools[node].get()
        try:
            async with conn.cursor() as cursor:
                cursor.set_query_id(query_id)
                await cursor.execute(query_text)
                rows_read = cursor.rowcount or 0

            elapsed = time.monotonic() - start_time
            completed_at = datetime.datetime.now(tz=datetime.UTC).isoformat()

            await self.checkpoint_mgr.mark_completed(
                query_hash=query_hash,
                node=node,
                started_at=started_at,
                completed_at=completed_at,
                rows_read=rows_read,
                elapsed=elapsed,
            )

            if self._progress is not None:
                self._progress.mark_completed(query_id, elapsed)

            LOGGER.debug(
                'Query %d completed on %s in %.1fs', offset, node, elapsed
            )
        except (
            OSError,
            asynch_errors.ServerException,
            asynch_errors.UnexpectedPacketFromServerError,
        ) as err:
            elapsed = time.monotonic() - start_time
            self._stop_dispatch = True
            self._failure = _QueryFailure(
                query_text=query_text, node=node, error=str(err), offset=offset
            )
            await self.checkpoint_mgr.mark_failed(
                query_hash=query_hash,
                node=node,
                started_at=started_at,
                error=str(err),
            )
            if self._progress is not None:
                self._progress.mark_completed(query_id, elapsed)
            LOGGER.error('Query %d failed on %s: %s', offset, node, err)

            if self.settings.cancel_on_failure:
                await self._cancel_in_flight()
        finally:
            self._conn_pools[node].put_nowait(conn)
            semaphore.release()

    async def _poll_all_progress(self) -> None:
        """Poll system.processes on all nodes for active query progress.

        Uses persistent poll connections created during connect() to
        avoid per-query connection overhead.
        """
        try:
            while True:
                await asyncio.sleep(self.settings.poll_interval)
                if self._progress is None:
                    continue
                for _node, poll_conn in self._poll_conns.items():
                    try:
                        async with poll_conn.cursor() as cursor:
                            await cursor.execute(PROGRESS_QUERY)
                            rows = await cursor.fetchall()
                            for row in rows:
                                # PROGRESS_QUERY columns: query_id[0],
                                # read_rows[1], total_rows_approx[2],
                                # elapsed[3], written_rows[4],
                                # memory_usage[5]
                                self._progress.update_query(
                                    query_id=row[0],
                                    read_rows=row[1] or 0,
                                    total_rows=row[2] or 0,
                                    written_rows=row[4] or 0,
                                    memory_usage=row[5] or 0,
                                )
                    except (  # noqa: S110
                        OSError,
                        asynch_errors.ServerException,
                        asynch_errors.UnexpectedPacketFromServerError,
                    ):
                        pass  # Progress polling is best-effort
        except asyncio.CancelledError:
            return

    async def _cancel_in_flight(self) -> None:
        """Cancel all in-flight queries on all nodes."""
        LOGGER.info('Cancelling in-flight queries')
        for node in self.nodes:
            try:
                conn = asynch_connection.Connection(**self._conn_kwargs(node))
                await conn.connect()
                try:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            'KILL QUERY WHERE user = currentUser()'
                        )
                finally:
                    await conn.close()
            except (
                OSError,
                asynch_errors.ServerException,
                asynch_errors.UnexpectedPacketFromServerError,
            ):
                LOGGER.debug('Error cancelling queries on %s', node)

    @property
    def failure(self) -> _QueryFailure | None:
        """Return failure details if a query failed."""
        return self._failure


class _QueryFailure(typing.NamedTuple):
    """Details of a failed query."""

    query_text: str
    node: str
    error: str
    offset: int
