"""jumpserver 执行器测试：起一个假 JumpServer（http.server）跑真实 HTTP 流程。

覆盖：查重跳过 / 创建成功 / 401 / 创建失败 / 重名中止整批、结构化 host_results、
payload 与配置错误、取消、org/node/platform 头与字段、槽位归属。
"""
import asyncio
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ops_agent.config import Config, JumpServerConfig
from ops_agent.executor import job_kind, make_execution
from ops_agent.jumpserver import JumpServerExecution

HOSTS_PATH = '/api/v1/assets/hosts/'


class _Handler(BaseHTTPRequestHandler):
    """按 server.state 应答的假 JumpServer。

    state:
        existing      已存在的资产名集合（查重命中 → 跳过）
        duplicated    创建时报资产名重名的主机名集合（400 + name）
        create_status 指定主机名的创建返回码（默认 201）
        unauthorized  True 时所有请求返回 401
    """

    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):  # 静音测试输出
        pass

    def _send(self, status, body=b'', headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _json(self, payload, status=200):
        self._send(status, json.dumps(payload).encode('utf-8'),
                   {'Content-Type': 'application/json'})

    def do_GET(self):
        state = self.server.state
        parsed = urlparse(self.path)
        name = parse_qs(parsed.query).get('name', [''])[0]
        state['requests'].append(('GET', parsed.path, name))
        state['org_headers'].append(self.headers.get('X-JMS-ORG'))
        state['auth_headers'].append(self.headers.get('Authorization'))
        if state.get('unauthorized'):
            return self._json({'detail': 'Invalid token'}, status=401)
        if parsed.path != HOSTS_PATH:
            return self._send(404)
        results = [{'id': 'x', 'name': name}] if name in state['existing'] else []
        self._json({'count': len(results), 'results': results})

    def do_POST(self):
        state = self.server.state
        length = int(self.headers.get('Content-Length') or 0)
        body = json.loads(self.rfile.read(length) or b'{}')
        state['requests'].append(('POST', urlparse(self.path).path, body.get('name')))
        state['created'].append(body)
        if state.get('unauthorized'):
            return self._json({'detail': 'Invalid token'}, status=401)
        name = body.get('name')
        if name in state['duplicated']:
            return self._json({'name': ['资产名称已存在']}, status=400)
        status = state['create_status'].get(name, 201)
        if status >= 400:
            return self._json({'detail': '创建失败'}, status=status)
        self._json({'id': 'new', 'name': name}, status=status)


class FakeJumpServer:
    """上下文管理器：起假 JumpServer，退出时关停。"""

    def __init__(self, **state):
        defaults = dict(requests=[], created=[], existing=set(), duplicated=set(),
                        create_status={}, org_headers=[], auth_headers=[])
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


def make_config(tmp, jumpserver=None, **overrides):
    defaults = dict(
        manager_url='ws://x/', token='t', agent_name='a',
        data_dir=str(tmp), log_dir=str(tmp), job_timeout_max=3600,
        jumpserver=jumpserver if jumpserver is not None else JumpServerConfig(),
    )
    defaults.update(overrides)
    return Config(**defaults)


def host(name, address='10.0.0.1', **overrides):
    return {'key': name, 'name': name, 'address': address, **overrides}


class JumpServerExecutionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.lines = []

    def tearDown(self):
        self._tmp.cleanup()

    async def _collect(self, lines):
        self.lines.extend(lines)

    def _execution(self, fake, hosts, js_overrides=None, **payload_extra):
        js = JumpServerConfig(url=fake.url if fake else '', token='tok',
                              **(js_overrides or {}))
        config = make_config(self.tmp, jumpserver=js)
        payload = {'step_type': 'jumpserver', 'hosts': hosts, 'timeout': 30,
                   **payload_extra}
        return JumpServerExecution('job-1', payload, config)

    def _run(self, fake, hosts, js_overrides=None, **payload_extra):
        execution = self._execution(fake, hosts, js_overrides, **payload_extra)
        rc, elapsed = asyncio.run(execution.run(self._collect))
        return rc, execution

    @property
    def text(self):
        return '\n'.join(self.lines)

    # ── 正常流程 ──────────────────────────────────────────

    def test_creates_and_skips_existing(self):
        with FakeJumpServer(existing={'web-02'}) as fake:
            rc, execution = self._run(
                fake, [host('web-01', '10.0.0.1'), host('web-02', '10.0.0.2')])
        self.assertEqual(rc, 0)
        self.assertEqual(execution.host_results['web-01']['msg'], '注册成功')
        self.assertEqual(execution.host_results['web-02']['msg'], '已存在，跳过')
        self.assertTrue(all(r['ok'] for r in execution.host_results.values()))
        # 已存在的主机不再发创建请求
        self.assertEqual([b['name'] for b in fake.state['created']], ['web-01'])
        self.assertIn('[jumpserver] 完成：成功 2，失败 0', self.text)
        stdout = (self.tmp / 'artifacts' / 'job-1' / 'stdout.log').read_text(encoding='utf-8')
        self.assertIn('web-01(10.0.0.1)：注册成功', stdout)

    def test_create_payload_fields(self):
        with FakeJumpServer() as fake:
            rc, _ = self._run(
                fake, [host('web-01', '10.0.0.1', ssh_port=2222, comment='测试机')],
                js_overrides={'org_id': 'org-9', 'platform_id': '5', 'node_id': 'node-3'})
        self.assertEqual(rc, 0)
        body = fake.state['created'][0]
        self.assertEqual(body['address'], '10.0.0.1')
        self.assertEqual(body['protocols'], [{'name': 'ssh', 'port': 2222}])
        self.assertEqual(body['platform'], {'pk': 5})
        self.assertEqual(body['nodes'], [{'pk': 'node-3'}])
        self.assertEqual(body['comment'], '测试机')
        self.assertTrue(body['is_active'])
        self.assertEqual(fake.state['org_headers'][0], 'org-9')
        self.assertEqual(fake.state['auth_headers'][0], 'Token tok')

    def test_default_ssh_port_22(self):
        with FakeJumpServer() as fake:
            self._run(fake, [host('web-01')])
        self.assertEqual(fake.state['created'][0]['protocols'], [{'name': 'ssh', 'port': 22}])

    # ── 失败路径 ──────────────────────────────────────────

    def test_partial_failure_returns_rc1(self):
        with FakeJumpServer(create_status={'web-02': 500}) as fake:
            rc, execution = self._run(fake, [host('web-01'), host('web-02')])
        self.assertEqual(rc, 1)
        self.assertTrue(execution.host_results['web-01']['ok'])
        self.assertFalse(execution.host_results['web-02']['ok'])
        self.assertIn('HTTP 500', execution.host_results['web-02']['msg'])
        self.assertIn('[jumpserver] 完成：成功 1，失败 1', self.text)

    def test_unauthorized_marks_each_host_failed(self):
        with FakeJumpServer(unauthorized=True) as fake:
            rc, execution = self._run(fake, [host('web-01')])
        self.assertEqual(rc, 1)
        self.assertIn('认证失败', execution.host_results['web-01']['msg'])

    def test_duplicate_name_aborts_batch(self):
        with FakeJumpServer(duplicated={'web-01'}) as fake:
            rc, execution = self._run(fake, [host('web-01'), host('web-02')])
        self.assertEqual(rc, 1)
        self.assertIn('与 JumpServer 已有资产重名', self.text)
        # 中止整批：第二台既不查重也不创建
        self.assertNotIn('web-02', [name for _m, _p, name in fake.state['requests']])
        self.assertNotIn('web-02', execution.host_results)

    def test_connection_error_is_per_host_message(self):
        with FakeJumpServer() as fake:
            url = fake.url
        # 服务已关停：连接被拒
        execution = JumpServerExecution(
            'job-1', {'step_type': 'jumpserver', 'hosts': [host('web-01')], 'timeout': 30},
            make_config(self.tmp, jumpserver=JumpServerConfig(url=url, token='tok')))
        rc, _elapsed = asyncio.run(execution.run(self._collect))
        self.assertEqual(rc, 1)
        self.assertIn('请求异常', execution.host_results['web-01']['msg'])

    # ── payload / 配置校验 ────────────────────────────────

    def test_not_configured(self):
        execution = JumpServerExecution(
            'job-1', {'step_type': 'jumpserver', 'hosts': [host('web-01')]},
            make_config(self.tmp))
        rc, _elapsed = asyncio.run(execution.run(self._collect))
        self.assertEqual(rc, 126)
        self.assertIn('agent 未配置 JumpServer 连接信息', self.text)

    def test_empty_hosts(self):
        with FakeJumpServer() as fake:
            rc, _ = self._run(fake, [])
        self.assertEqual(rc, 126)
        self.assertIn('未携带待注册主机清单', self.text)

    def test_host_missing_address(self):
        with FakeJumpServer() as fake:
            rc, _ = self._run(fake, [{'key': 'k', 'name': 'web-01'}])
        self.assertEqual(rc, 126)
        self.assertIn('缺少 name 或 address', self.text)

    def test_too_many_hosts(self):
        with FakeJumpServer() as fake:
            rc, _ = self._run(fake, [host(f'web-{i}') for i in range(501)])
        self.assertEqual(rc, 126)
        self.assertIn('超过上限', self.text)

    # ── 取消 ──────────────────────────────────────────────

    def test_cancel_stops_before_next_host(self):
        with FakeJumpServer() as fake:
            execution = self._execution(fake, [host('web-01'), host('web-02')])

            async def scenario():
                await execution.cancel()
                return await execution.run(self._collect)

            rc, _elapsed = asyncio.run(scenario())
        self.assertEqual(rc, 143)
        self.assertIn('注册被 manager 取消', self.text)
        self.assertEqual(fake.state['requests'], [])

    # ── 工厂与槽位 ────────────────────────────────────────

    def test_factory_and_slot_kind(self):
        payload = {'step_type': 'jumpserver', 'hosts': [host('web-01')]}
        execution = make_execution('job-1', payload, make_config(self.tmp))
        self.assertIsInstance(execution, JumpServerExecution)
        self.assertEqual(execution.kind, 'jumpserver')
        self.assertEqual(job_kind(payload), 'jumpserver')


if __name__ == '__main__':
    unittest.main()
