"""executor 测试：argv 构造、路径穿越防护、真实子进程执行/超时/取消。"""
import asyncio
import tempfile
import unittest
from pathlib import Path

from ops_agent.config import Config
from ops_agent.executor import JobExecution, PayloadError, build_argv, resolve_within


def make_config(tmp, **overrides):
    defaults = dict(
        manager_url='ws://x/', token='t', agent_name='a',
        data_dir=str(tmp), log_dir=str(tmp), max_forks=10,
        job_timeout_max=3600, ansible_project_dir=str(tmp),
        project_sync_cmd='', max_job_log_lines=100,
    )
    defaults.update(overrides)
    return Config(**defaults)


class BuildArgvTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.config = make_config(self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    def test_command_argv(self):
        argv, cwd = build_argv({
            'step_type': 'command', 'command': 'echo hi',
            'forks': 5, 'extra_vars': ['{"a": 1}', 'b=2'],
        }, '/tmp/inv.ini', self.config)
        self.assertEqual(argv[:5], ['ansible', 'all', '-i', '/tmp/inv.ini', '--forks'])
        self.assertEqual(argv[5], '5')
        self.assertIn('{"a": 1}', argv)
        self.assertIn('b=2', argv)
        self.assertEqual(argv[-3:], ['-m', 'shell', '-a', 'echo hi'][-3:])
        self.assertIsNone(cwd)

    def test_forks_capped_by_config(self):
        argv, _ = build_argv({'step_type': 'command', 'command': 'x', 'forks': 999},
                             '/tmp/inv.ini', self.config)
        self.assertEqual(argv[argv.index('--forks') + 1], '10')

    def test_playbook_argv_with_tags(self):
        (self.tmp / 'site.yml').write_text('- hosts: all\n', encoding='utf-8')
        argv, cwd = build_argv({
            'step_type': 'playbook', 'playbook': 'site.yml', 'tags': 'restart',
        }, '/tmp/inv.ini', self.config)
        self.assertEqual(argv[0], 'ansible-playbook')
        self.assertIn('--tags', argv)
        self.assertEqual(cwd, str(self.tmp))

    def test_playbook_escape_rejected(self):
        with self.assertRaises(PayloadError):
            build_argv({'step_type': 'playbook', 'playbook': '../../etc/passwd'},
                       '/tmp/inv.ini', self.config)

    def test_unknown_step_type_rejected(self):
        with self.assertRaises(PayloadError):
            build_argv({'step_type': 'method'}, '/tmp/inv.ini', self.config)

    def test_resolve_within(self):
        self.assertTrue(resolve_within(str(self.tmp), 'a/b.yml').startswith(str(self.tmp)))
        with self.assertRaises(PayloadError):
            resolve_within(str(self.tmp), '/etc/passwd')


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    """用普通 shell 命令替身验证子进程流式/超时/取消（不依赖 ansible）。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.config = make_config(self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    async def _run_fake(self, execution, argv, timeout=30):
        """绕过 build_argv 直接执行给定 argv（测试子进程管理逻辑）。"""
        from unittest import mock

        with mock.patch('ops_agent.executor.build_argv', return_value=(argv, None)):
            execution.payload.setdefault('step_type', 'command')
            execution.payload.setdefault('command', 'x')
            execution.payload['timeout'] = timeout
            lines = []

            async def collect(batch):
                lines.extend(batch)

            rc, elapsed = await execution.run(collect)
            return rc, lines

    async def test_stream_and_rc(self):
        execution = JobExecution('j1', {}, self.config)
        rc, lines = await self._run_fake(
            execution, ['sh', '-c', 'echo one; echo two; exit 3'])
        self.assertEqual(rc, 3)
        self.assertIn('one', lines)
        self.assertIn('two', lines)
        # 本地 artifacts 留存
        self.assertEqual((self.config.artifacts_dir / 'j1' / 'rc').read_text(), '3')
        self.assertIn('one', (self.config.artifacts_dir / 'j1' / 'stdout.log').read_text())

    async def test_timeout_kills_process_group(self):
        execution = JobExecution('j2', {}, self.config)
        rc, lines = await self._run_fake(execution, ['sh', '-c', 'sleep 30'], timeout=1)
        self.assertEqual(rc, 124)
        self.assertTrue(any('超时' in line for line in lines))

    async def test_cancel(self):
        execution = JobExecution('j3', {}, self.config)

        async def cancel_soon():
            await asyncio.sleep(0.3)
            await execution.cancel()

        canceller = asyncio.ensure_future(cancel_soon())
        rc, _ = await self._run_fake(execution, ['sh', '-c', 'sleep 30'])
        await canceller
        self.assertEqual(rc, 143)

    async def test_log_truncation(self):
        execution = JobExecution('j4', {}, self.config)  # max_job_log_lines=100
        rc, lines = await self._run_fake(
            execution, ['sh', '-c', 'seq 1 200'])
        self.assertEqual(rc, 0)
        self.assertTrue(any('截断' in line for line in lines))
        self.assertLessEqual(len(lines), 102)
        # 本地留存不截断
        log_text = (self.config.artifacts_dir / 'j4' / 'stdout.log').read_text()
        self.assertIn('200', log_text)


if __name__ == '__main__':
    unittest.main()
