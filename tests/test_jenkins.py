"""jenkins 执行器测试：起一个假 Jenkins（http.server）跑真实 HTTP 流程。

覆盖：触发→出队→日志增量→结果、CSRF crumb 重试、构建失败/UNSTABLE、
取消（含 /stop 中止）、等待超时、payload/配置错误、回传行数截断。
"""
import asyncio
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ops_agent.config import Config, JenkinsConfig
from ops_agent.executor import job_kind, make_execution
from ops_agent.jenkins import JenkinsExecution

JOB = 'build-app'


class _Handler(BaseHTTPRequestHandler):
    """按 server.state 应答的假 Jenkins。"""

    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):  # 静音测试输出
        pass

    # ── 应答 ──────────────────────────────────────────────

    def _send(self, status, body=b'', headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _json(self, payload):
        self._send(200, json.dumps(payload).encode('utf-8'),
                   {'Content-Type': 'application/json'})

    def do_POST(self):
        state = self.server.state
        path = urlparse(self.path).path
        state['requests'].append(('POST', path))
        if path.endswith('/stop'):
            state['stopped'] = True
            return self._send(200)
        if path.startswith(f'/job/{JOB}/'):
            state['trigger_params'] = parse_qs(urlparse(self.path).query)
            if state.get('csrf_required') and not self.headers.get('Jenkins-Crumb'):
                return self._send(403, b'No valid crumb was included in the request')
            queue_url = f'http://{self.headers["Host"]}/queue/item/1/'
            return self._send(201, b'', {'Location': queue_url})
        self._send(404)

    def do_GET(self):
        state = self.server.state
        parsed = urlparse(self.path)
        path = parsed.path
        state['requests'].append(('GET', path))
        if path == '/crumbIssuer/api/json':
            return self._json({'crumbRequestField': 'Jenkins-Crumb', 'crumb': 'c0ffee'})
        if path == '/queue/item/1/api/json':
            if state.get('queue_cancelled'):
                return self._json({'cancelled': True})
            if state['queue_polls'] > 0:
                state['queue_polls'] -= 1
                return self._json({})
            host = self.headers['Host']
            return self._json({'executable': {
                'number': 7, 'url': f'http://{host}/job/{JOB}/7/'}})
        if path == f'/job/{JOB}/7/logText/progressiveText':
            return self._console(state)
        if path == f'/job/{JOB}/7/api/json':
            return self._json({'result': state['result']})
        self._send(404)

    def _console(self, state):
        chunks = state['console']
        text = chunks.pop(0) if chunks else ''
        state['served'] += len(text)
        more = bool(chunks) or state.get('console_forever')
        self._send(200, text.encode('utf-8'), {
            'X-Text-Size': str(state['served']),
            'X-More-Data': 'true' if more else 'false',
        })


class FakeJenkins:
    """上下文管理器：起假 Jenkins，退出时关停。"""

    def __init__(self, **state):
        defaults = dict(requests=[], queue_polls=1, console=[], served=0,
                        result='SUCCESS', stopped=False)
        defaults.update(state)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
        self.server.state = defaults
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def state(self):
        return self.server.state

    @property
    def url(self):
        host, port = self.server.server_address
        return f'http://{host}:{port}'

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def make_config(tmp, jenkins=None, **overrides):
    defaults = dict(
        manager_url='ws://x/', token='t', agent_name='a',
        data_dir=str(tmp), log_dir=str(tmp), job_timeout_max=3600,
        max_job_log_lines=100000,
        jenkins=jenkins if jenkins is not None else JenkinsConfig(),
    )
    defaults.update(overrides)
    return Config(**defaults)


def jenkins_config(url, **overrides):
    defaults = dict(url=url, user='ci', api_token='tok', poll_interval=0.01)
    defaults.update(overrides)
    return JenkinsConfig(**defaults)


class JenkinsExecutionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.lines = []

    def tearDown(self):
        self._tmp.cleanup()

    async def _collect(self, lines):
        self.lines.extend(lines)

    def _run(self, fake, payload, **config_overrides):
        config = make_config(self.tmp, jenkins=jenkins_config(fake.url), **config_overrides)
        execution = JenkinsExecution('job-1', payload, config)
        return asyncio.run(execution.run(self._collect))

    # ── 正常流程 ──────────────────────────────────────────

    def test_success_streams_console(self):
        with FakeJenkins(console=['Started by user\nBuilding…', ' done\nFinished\n']) as fake:
            rc, elapsed = self._run(fake, {
                'step_type': 'jenkins', 'job_name': JOB,
                'params': {'BRANCH': 'release'}, 'timeout': 30})
        self.assertEqual(rc, 0)
        self.assertGreaterEqual(elapsed, 0)
        text = '\n'.join(self.lines)
        self.assertIn('[jenkins] job=build-app build=#7', text)
        self.assertIn('Started by user', text)
        # 分片边界处被截半的行拼回完整一行，不出现 "Building…" 单独成行
        self.assertIn('Building… done', text)
        self.assertIn('[jenkins] 构建结果：SUCCESS', text)
        self.assertEqual(fake.state['trigger_params'], {'BRANCH': ['release']})
        # 本地全量留存
        stdout = (self.tmp / 'artifacts' / 'job-1' / 'stdout.log').read_text(encoding='utf-8')
        self.assertIn('Finished', stdout)
        self.assertEqual((self.tmp / 'artifacts' / 'job-1' / 'rc').read_text(), '0')

    def test_console_note_and_ansi_stripped(self):
        with FakeJenkins(console=['\x1b[8mha:////4AeQ==\x1b[0m\x1b[1;32mOK\x1b[0m\n']) as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 30})
        self.assertEqual(rc, 0)
        self.assertIn('OK', self.lines)

    def test_no_params_uses_build_endpoint(self):
        with FakeJenkins(console=['x\n']) as fake:
            self._run(fake, {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 30})
        self.assertIn(('POST', f'/job/{JOB}/build'), fake.state['requests'])

    def test_failure_result_maps_rc_1(self):
        with FakeJenkins(console=['boom\n'], result='FAILURE') as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 30})
        self.assertEqual(rc, 1)

    def test_unstable_needs_allow_unstable(self):
        with FakeJenkins(console=['flaky\n'], result='UNSTABLE') as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 30})
            self.assertEqual(rc, 1)
        with FakeJenkins(console=['flaky\n'], result='UNSTABLE') as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB,
                                     'timeout': 30, 'allow_unstable': True})
        self.assertEqual(rc, 0)

    def test_crumb_retry_on_403(self):
        with FakeJenkins(console=['ok\n'], csrf_required=True) as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB,
                                     'params': {'A': '1'}, 'timeout': 30})
            requests = fake.state['requests']
        self.assertEqual(rc, 0)
        self.assertIn(('GET', '/crumbIssuer/api/json'), requests)
        self.assertEqual(sum(1 for m, p in requests if m == 'POST'
                             and p == f'/job/{JOB}/buildWithParameters'), 2)

    # ── 失败与中断 ────────────────────────────────────────

    def test_trigger_http_error(self):
        with FakeJenkins() as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': 'nope',
                                     'timeout': 30})
        self.assertEqual(rc, 1)
        self.assertTrue(any('触发 Jenkins 构建失败' in line for line in self.lines))

    def test_connection_error(self):
        config = make_config(self.tmp, jenkins=jenkins_config('http://127.0.0.1:1'))
        execution = JenkinsExecution('job-1', {'step_type': 'jenkins', 'job_name': JOB,
                                               'timeout': 5}, config)
        rc, _ = asyncio.run(execution.run(self._collect))
        self.assertEqual(rc, 1)
        self.assertTrue(any('Jenkins 请求失败' in line for line in self.lines))

    def test_missing_job_name(self):
        config = make_config(self.tmp, jenkins=jenkins_config('http://x'))
        rc, _ = asyncio.run(
            JenkinsExecution('job-1', {'step_type': 'jenkins'}, config).run(self._collect))
        self.assertEqual(rc, 126)
        self.assertTrue(any('未指定 Jenkins job_name' in line for line in self.lines))

    def test_agent_jenkins_not_configured(self):
        config = make_config(self.tmp)  # 未配置 jenkins 段
        rc, _ = asyncio.run(JenkinsExecution(
            'job-1', {'step_type': 'jenkins', 'job_name': JOB}, config).run(self._collect))
        self.assertEqual(rc, 126)
        self.assertTrue(any('agent 未配置 Jenkins 连接信息' in line for line in self.lines))

    def test_timeout_aborts_build(self):
        with FakeJenkins(console=['running\n'], console_forever=True) as fake:
            config = make_config(self.tmp, jenkins=jenkins_config(fake.url))
            execution = JenkinsExecution(
                'job-1', {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 1}, config)
            rc, _ = asyncio.run(execution.run(self._collect))
            stopped = fake.state['stopped']
        self.assertEqual(rc, 124)
        self.assertTrue(stopped)
        self.assertTrue(any('等待 Jenkins 构建完成超时' in line for line in self.lines))

    def test_cancel_aborts_build(self):
        async def scenario(execution):
            task = asyncio.ensure_future(execution.run(self._collect))
            await asyncio.sleep(0.2)
            await execution.cancel()
            return await task

        with FakeJenkins(console=['running\n'], console_forever=True) as fake:
            config = make_config(self.tmp, jenkins=jenkins_config(fake.url))
            execution = JenkinsExecution(
                'job-1', {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 60}, config)
            rc, _ = asyncio.run(scenario(execution))
            stopped = fake.state['stopped']
        self.assertEqual(rc, 143)
        self.assertTrue(stopped)
        self.assertTrue(any('被 manager 取消' in line for line in self.lines))

    def test_cancel_keeps_build_when_abort_disabled(self):
        async def scenario(execution):
            task = asyncio.ensure_future(execution.run(self._collect))
            await asyncio.sleep(0.2)
            await execution.cancel()
            return await task

        with FakeJenkins(console=['running\n'], console_forever=True) as fake:
            config = make_config(self.tmp, jenkins=jenkins_config(
                fake.url, abort_on_cancel=False))
            execution = JenkinsExecution(
                'job-1', {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 60}, config)
            rc, _ = asyncio.run(scenario(execution))
            stopped = fake.state['stopped']
        self.assertEqual(rc, 143)
        self.assertFalse(stopped)

    def test_queue_cancelled(self):
        with FakeJenkins(queue_cancelled=True) as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB,
                                     'timeout': 30})
        self.assertEqual(rc, 1)
        self.assertTrue(any('队列中被取消' in line for line in self.lines))

    def test_log_lines_truncated(self):
        console = ['\n'.join(f'line{i}' for i in range(20)) + '\n']
        with FakeJenkins(console=console) as fake:
            rc, _ = self._run(fake, {'step_type': 'jenkins', 'job_name': JOB, 'timeout': 30},
                              max_job_log_lines=5)
        self.assertEqual(rc, 0)
        self.assertTrue(any('已截断' in line for line in self.lines))
        self.assertNotIn('line19', self.lines)
        # 本地留存不受回传截断影响
        stdout = (self.tmp / 'artifacts' / 'job-1' / 'stdout.log').read_text(encoding='utf-8')
        self.assertIn('line19', stdout)


class FactoryTests(unittest.TestCase):
    def test_job_kind(self):
        self.assertEqual(job_kind({'step_type': 'command'}), 'ansible')
        self.assertEqual(job_kind({'step_type': 'playbook'}), 'ansible')
        self.assertEqual(job_kind({'step_type': 'jenkins'}), 'jenkins')
        self.assertIsNone(job_kind({'step_type': 'whatever'}))

    def test_make_execution_picks_jenkins(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), jenkins=jenkins_config('http://x'))
            execution = make_execution('j', {'step_type': 'jenkins'}, config)
        self.assertIsInstance(execution, JenkinsExecution)
        self.assertEqual(execution.kind, 'jenkins')


if __name__ == '__main__':
    unittest.main()
