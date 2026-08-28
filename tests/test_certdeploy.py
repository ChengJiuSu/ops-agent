"""cert_deploy 执行单元：剧本本地生成、payload 校验、证书明文不落盘。"""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ops_agent import executor
from ops_agent.executor import (
    JobExecution, PayloadError, build_cert_playbook, redact_payload,
)

from test_executor import make_config

CERT_FILES = [
    {'name': 'web-cert.crt', 'mode': '0644', 'content': 'CERTDATA'},
    {'name': 'web-cert.key', 'mode': '0600', 'content': 'PRIVATEKEYDATA'},
]


def payload(**overrides):
    base = {
        'step_type': 'cert_deploy',
        'inventory': 'web-01 ansible_host=10.0.0.8 ansible_user=root\n',
        'deploy_path': '/etc/nginx/ssl',
        'reload_cmd': 'nginx -s reload',
        'files': [dict(f) for f in CERT_FILES],
        'timeout': 30,
    }
    base.update(overrides)
    return base


class BuildCertPlaybookTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_playbook_tasks_and_file_modes(self):
        import json

        path = build_cert_playbook(payload(), self.tmp)
        plays = json.loads(Path(path).read_text(encoding='utf-8'))
        tasks = plays[0]['tasks']
        # 目录 + 两个文件 + reload
        self.assertEqual(len(tasks), 4)
        self.assertEqual(tasks[0]['ansible.builtin.file']['mode'], '0750')
        copies = [t['ansible.builtin.copy'] for t in tasks[1:3]]
        self.assertEqual([c['dest'] for c in copies],
                         ['/etc/nginx/ssl/web-cert.crt', '/etc/nginx/ssl/web-cert.key'])
        self.assertEqual([c['mode'] for c in copies], ['0644', '0600'])
        self.assertTrue(all(c['backup'] for c in copies))
        self.assertEqual(tasks[3]['ansible.builtin.shell'], 'nginx -s reload')
        # 源文件落在私有目录且 0600
        for copy in copies:
            src = Path(copy['src'])
            self.assertEqual(src.parent, self.tmp)
            self.assertEqual(oct(src.stat().st_mode & 0o777), '0o600')

    def test_reload_cmd_optional(self):
        import json

        path = build_cert_playbook(payload(reload_cmd=''), self.tmp)
        tasks = json.loads(Path(path).read_text(encoding='utf-8'))[0]['tasks']
        self.assertEqual(len(tasks), 3)

    def test_rejects_bad_payload(self):
        cases = [
            payload(deploy_path=''),
            payload(deploy_path='etc/nginx/ssl'),          # 相对路径
            payload(deploy_path='/etc/nginx/../../ssl'),   # 含 ..
            payload(files=[]),
            payload(files=[{'name': '../evil.key', 'mode': '0600', 'content': 'x'}]),
            payload(files=[{'name': 'a/b.key', 'mode': '0600', 'content': 'x'}]),
            payload(files=[{'name': 'a.key', 'mode': '777', 'content': 'x'}]),
            payload(files=[{'name': 'a.key', 'mode': '0600', 'content': ''}]),
            payload(files=['a.key']),
        ]
        for case in cases:
            with self.assertRaises(PayloadError, msg=case):
                build_cert_playbook(case, self.tmp)

    def test_rejects_oversized_content(self):
        big = 'x' * (executor.CERT_FILE_MAX_BYTES + 1)
        with self.assertRaises(PayloadError):
            build_cert_playbook(
                payload(files=[{'name': 'a.key', 'mode': '0600', 'content': big}]),
                self.tmp)

    def test_redact_payload_drops_files(self):
        redacted = redact_payload(payload())
        self.assertEqual(redacted['files'], executor.SECRET_PLACEHOLDER)
        self.assertEqual(redacted['deploy_path'], '/etc/nginx/ssl')
        # 原 payload 不被改动（执行时仍要用真内容）
        self.assertIsInstance(payload()['files'], list)
        # 无敏感字段时原样返回（不多拷一份）
        plain = {'step_type': 'command', 'command': 'x'}
        self.assertIs(redact_payload(plain), plain)


class CertDeployExecutionTests(unittest.IsolatedAsyncioTestCase):
    """证书明文只在私有临时目录停留，执行完连目录一起删。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.config = make_config(self.tmp)
        self.secure_dir = self.tmp / 'secure'
        self.secure_dir.mkdir()

    def tearDown(self):
        self._tmp.cleanup()

    async def test_runs_generated_playbook_then_wipes_secrets(self):
        real_build_argv = executor.build_argv
        seen = {}

        def fake_build_argv(job_payload, inventory_path, config, workdir=None):
            argv, cwd = real_build_argv(job_payload, inventory_path, config, workdir=workdir)
            seen['playbook'] = argv[1]
            seen['inventory'] = inventory_path
            seen['srcs'] = sorted(p.name for p in Path(workdir).glob('src*.pem'))
            # 用 cat 替身跑一遍：验证剧本与源文件此刻真的在盘上
            return ['sh', '-c', f'cat {argv[1]}'], cwd

        lines = []

        async def collect(batch):
            lines.extend(batch)

        execution = JobExecution('cert1', payload(), self.config)
        with mock.patch('ops_agent.executor.tempfile.mkdtemp',
                        return_value=str(self.secure_dir)), \
                mock.patch('ops_agent.executor.build_argv', side_effect=fake_build_argv):
            rc, _elapsed = await execution.run(collect)

        self.assertEqual(rc, 0)
        self.assertEqual(seen['srcs'], ['src0.pem', 'src1.pem'])
        # 剧本与 inventory 都在私有目录，不进 artifacts
        self.assertTrue(seen['playbook'].startswith(str(self.secure_dir)))
        self.assertTrue(seen['inventory'].startswith(str(self.secure_dir)))
        self.assertFalse((self.config.artifacts_dir / 'cert1' / 'inventory.ini').exists())
        # 私有目录（含私钥明文）执行后被删
        self.assertFalse(self.secure_dir.exists())
        # 回传日志与本地留存都不含私钥内容
        text = '\n'.join(lines)
        self.assertNotIn('PRIVATEKEYDATA', text)
        self.assertIn('deploy_cert.yml', seen['playbook'])
        stdout_log = (self.config.artifacts_dir / 'cert1' / 'stdout.log').read_text()
        self.assertNotIn('PRIVATEKEYDATA', stdout_log)

    async def test_secure_dir_wiped_when_execution_raises(self):
        execution = JobExecution('cert2', payload(), self.config)
        with mock.patch('ops_agent.executor.tempfile.mkdtemp',
                        return_value=str(self.secure_dir)), \
                mock.patch('ops_agent.executor.asyncio.create_subprocess_exec',
                           side_effect=FileNotFoundError('ansible-playbook 不存在')):
            with self.assertRaises(FileNotFoundError):
                await execution.run(lambda _batch: asyncio.sleep(0))
        self.assertFalse(self.secure_dir.exists())


if __name__ == '__main__':
    unittest.main()
