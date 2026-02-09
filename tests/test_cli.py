"""Tests for the CLI application."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

import pydantic

from clickhouse_query_runner import cli


class SetupLoggingTests(unittest.TestCase):
    """Tests for setup_logging."""

    def setUp(self) -> None:
        import logging

        root = logging.getLogger()
        self._orig_level = root.level
        self._orig_handlers = root.handlers[:]
        root.handlers.clear()
        root.manager.loggerClass = None
        logging.root.handlers = []

    def tearDown(self) -> None:
        import logging

        root = logging.getLogger()
        root.handlers = self._orig_handlers
        root.setLevel(self._orig_level)

    def test_default_level(self) -> None:
        import logging

        from rich import console

        cli.setup_logging(console.Console(), verbose=False)
        self.assertEqual(logging.getLogger().level, logging.INFO)

    def test_verbose_level(self) -> None:
        import logging

        from rich import console

        cli.setup_logging(console.Console(), verbose=True)
        self.assertEqual(logging.getLogger().level, logging.DEBUG)


class EnvVarForFieldTests(unittest.TestCase):
    """Tests for _env_var_for_field."""

    def test_default_prefix(self) -> None:
        self.assertEqual(cli._env_var_for_field('host'), 'CLICKHOUSE_HOST')

    def test_validation_alias_field(self) -> None:
        self.assertEqual(cli._env_var_for_field('valkey_url'), 'VALKEY_URL')

    def test_unknown_field(self) -> None:
        self.assertEqual(
            cli._env_var_for_field('nonexistent'), 'CLICKHOUSE_NONEXISTENT'
        )


class MainTests(unittest.TestCase):
    """Tests for main entry point."""

    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_validation_error_missing_fields(
        self, mock_cls: mock.MagicMock
    ) -> None:
        mock_cls.side_effect = pydantic.ValidationError.from_exception_data(
            'RunnerSettings',
            [
                {
                    'type': 'missing',
                    'loc': ('host',),
                    'msg': 'Field required',
                    'input': {},
                }
            ],
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_validation_error_invalid_value(
        self, mock_cls: mock.MagicMock
    ) -> None:
        mock_cls.side_effect = pydantic.ValidationError.from_exception_data(
            'RunnerSettings',
            [
                {
                    'type': 'int_parsing',
                    'loc': ('concurrency',),
                    'msg': 'Input should be a valid integer',
                    'input': 'abc',
                }
            ],
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    def _make_settings(self, **overrides: object) -> mock.MagicMock:
        defaults = {
            'verbose': False,
            'query_file': '/nonexistent_cqr_test.sql',
            'dry_run': False,
            'reset': False,
        }
        defaults.update(overrides)
        s = mock.MagicMock()
        for k, v in defaults.items():
            setattr(s, k, v)
        return s

    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_file_not_found(self, mock_cls: mock.MagicMock) -> None:
        mock_cls.return_value = self._make_settings(
            query_file='/nonexistent_cqr_test.sql'
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_empty_file(self, mock_cls: mock.MagicMock) -> None:
        fd, path = tempfile.mkstemp(suffix='.sql')
        os.close(fd)
        self.addCleanup(os.unlink, path)
        mock_cls.return_value = self._make_settings(query_file=path)
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 0)

    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_dry_run(self, mock_cls: mock.MagicMock) -> None:
        fd, path = tempfile.mkstemp(suffix='.sql')
        os.write(fd, b'SELECT 1;')
        os.close(fd)
        self.addCleanup(os.unlink, path)
        mock_cls.return_value = self._make_settings(
            query_file=path, dry_run=True
        )
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 0)

    @mock.patch('clickhouse_query_runner.cli.asyncio')
    @mock.patch('clickhouse_query_runner.cli.settings.RunnerSettings')
    def test_keyboard_interrupt(
        self, mock_cls: mock.MagicMock, mock_asyncio: mock.MagicMock
    ) -> None:
        fd, path = tempfile.mkstemp(suffix='.sql')
        os.write(fd, b'SELECT 1;')
        os.close(fd)
        self.addCleanup(os.unlink, path)
        mock_cls.return_value = self._make_settings(query_file=path)
        mock_asyncio.run.side_effect = KeyboardInterrupt()
        with self.assertRaises(SystemExit) as ctx:
            cli.main()
        self.assertEqual(ctx.exception.code, 130)


class AsyncMainTests(unittest.IsolatedAsyncioTestCase):
    """Tests for _async_main."""

    def _make_settings(self, **overrides: object) -> mock.MagicMock:
        defaults = {
            'verbose': False,
            'query_file': 'test.sql',
            'dry_run': False,
            'reset': False,
            'host': 'n1',
            'port': 9440,
            'database': 'default',
            'user': 'user',
            'password': mock.MagicMock(get_secret_value=lambda: 'pass'),
            'secure': True,
            'concurrency': 1,
            'run_id': 'test',
            'valkey_url': 'redis://localhost:6379/0',
            'checkpoint_ttl': 604800,
            'poll_interval': 0.01,
            'cancel_on_failure': False,
        }
        defaults.update(overrides)
        s = mock.MagicMock()
        for k, v in defaults.items():
            setattr(s, k, v)
        return s

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_reset_mode(self, mock_runner_cls: mock.MagicMock) -> None:
        s = self._make_settings(reset=True)
        mock_runner = mock.AsyncMock()
        mock_runner.reset.return_value = 5
        mock_runner.checkpoint_mgr.run_id = 'test-run'
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        await cli._async_main(s, [('h', 'Q')], console)
        mock_runner.reset.assert_awaited_once()

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_success(self, mock_runner_cls: mock.MagicMock) -> None:
        s = self._make_settings()
        mock_runner = mock.AsyncMock()
        mock_runner.run.return_value = True
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        await cli._async_main(s, [('h', 'Q')], console)
        mock_runner.run.assert_awaited_once()

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_failure_exits(
        self, mock_runner_cls: mock.MagicMock
    ) -> None:
        from clickhouse_query_runner.runner import _QueryFailure

        s = self._make_settings()
        mock_runner = mock.AsyncMock()
        mock_runner.run.return_value = False
        mock_runner.failure = _QueryFailure(
            query_text='BAD', node='n1', error='err', offset=0
        )
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        with self.assertRaises(SystemExit) as ctx:
            await cli._async_main(s, [('h', 'Q')], console)
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_failure_without_details(
        self, mock_runner_cls: mock.MagicMock
    ) -> None:
        s = self._make_settings()
        mock_runner = mock.AsyncMock()
        mock_runner.run.return_value = False
        mock_runner.failure = None
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        with self.assertRaises(SystemExit) as ctx:
            await cli._async_main(s, [('h', 'Q')], console)
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_oserror_exits(
        self, mock_runner_cls: mock.MagicMock
    ) -> None:
        s = self._make_settings()
        mock_runner = mock.AsyncMock()
        mock_runner.connect.side_effect = OSError('connection')
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        with self.assertRaises(SystemExit) as ctx:
            await cli._async_main(s, [('h', 'Q')], console)
        self.assertEqual(ctx.exception.code, 1)

    @mock.patch('clickhouse_query_runner.cli.runner.QueryRunner')
    async def test_close_always_called(
        self, mock_runner_cls: mock.MagicMock
    ) -> None:
        s = self._make_settings()
        mock_runner = mock.AsyncMock()
        mock_runner.connect.side_effect = ValueError('bad')
        mock_runner_cls.return_value = mock_runner
        console = mock.MagicMock()
        with self.assertRaises(SystemExit):
            await cli._async_main(s, [('h', 'Q')], console)
        mock_runner.close.assert_awaited_once()


class DisplayDryRunTests(unittest.TestCase):
    """Tests for _display_dry_run."""

    def test_displays_queries(self) -> None:
        console = mock.MagicMock()
        queries = [('abc123', 'SELECT 1'), ('def456', 'SELECT 2')]
        cli._display_dry_run(console, queries)
        self.assertTrue(console.print.called)
        calls = [str(c) for c in console.print.call_args_list]
        found_q0 = any('Query 0' in c for c in calls)
        found_q1 = any('Query 1' in c for c in calls)
        self.assertTrue(found_q0)
        self.assertTrue(found_q1)

    def test_truncates_long_queries(self) -> None:
        console = mock.MagicMock()
        long_query = 'SELECT ' + 'x' * 300
        queries = [('abc123', long_query)]
        cli._display_dry_run(console, queries)
        calls = [str(c) for c in console.print.call_args_list]
        found_ellipsis = any('...' in c for c in calls)
        self.assertTrue(found_ellipsis)

    def test_short_query_not_truncated(self) -> None:
        console = mock.MagicMock()
        queries = [('abc123', 'SELECT 1')]
        cli._display_dry_run(console, queries)
        calls = [str(c) for c in console.print.call_args_list]
        found_ellipsis = any('...' in c for c in calls)
        self.assertFalse(found_ellipsis)
