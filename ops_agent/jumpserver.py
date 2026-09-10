"""JumpServer 资产注册执行器：把 manager 下发的主机清单注册为 JumpServer 资产。

manager 在公网而 JumpServer 在内网时，manager 侧（资产页批量注册 / jumpserver
方法模块）把执行单元（`step_type='jumpserver'`）下发到本 agent，由 agent 走
JumpServer REST API：

    GET  /api/v1/assets/hosts/?name=<主机名>   查重（幂等：已存在跳过）
    POST /api/v1/assets/hosts/                 创建主机资产

JumpServer 连接信息只在 agent 本地 config.yaml 的 `jumpserver:` 段维护（与
Jenkins、SSH 私钥同口径：凭据不出通道、不入 manager 库），payload 只带主机
清单（名称 / 地址 / SSH 端口 / 备注）。

与 executor.JobExecution 同接口（kind / run / cancel），故复用 connection 的
LogPump、spool 分片回传、ack 补传、artifacts 留存全套设施；逐台结果另经
`host_results` 结构化回传，manager 侧据此还原「每台成功/跳过/失败」列表。

判定口径与 manager 直连路径（common/jumpserver.py）逐条对齐：查重按主机名、
已存在跳过、创建重名硬拒绝并中止整批（不自动改名）。
"""
import asyncio
import logging
import time
from pathlib import Path

from .httpclient import HttpClient, HttpError

logger = logging.getLogger('ops_agent.jumpserver')

RC_TIMEOUT = 124         # 等待超时
RC_PAYLOAD_ERROR = 126   # payload / agent 配置错误
RC_CANCELLED = 143       # manager 取消
MAX_HOSTS = 500          # 单次下发主机数上限：超出说明调用方没分批


class JumpServerError(Exception):
    """整批失败：携带 rc 与中文原因，在 run 出口统一转为返回值。"""

    def __init__(self, rc, message):
        super().__init__(message)
        self.rc = rc
        self.message = message


class JumpServerClient:
    """JumpServer REST 客户端：共用 HttpClient，连接层失败抛 HttpError。"""

    def __init__(self, config):
        headers = {'Authorization': f'Token {config.token}'}
        if config.org_id:
            headers['X-JMS-ORG'] = config.org_id
        self._base = config.base_url
        self._client = HttpClient(
            headers=headers, verify_tls=config.verify_tls, ca_file=config.ca_file,
            timeout=config.request_timeout)

    def get(self, path, params=None):
        return self._client.request('GET', f'{self._base}{path}', params=params)

    def post(self, path, json_body):
        return self._client.request('POST', f'{self._base}{path}', json_body=json_body)


class JumpServerExecution:
    """一次 JumpServer 注册单元：run() 逐台注册并流式回传，cancel() 中止后续。"""

    kind = 'jumpserver'

    def __init__(self, job_id: str, payload: dict, config):
        self.job_id = job_id
        self.payload = payload
        self.config = config
        self.jumpserver = config.jumpserver
        self.artifacts = Path(config.artifacts_dir) / job_id
        # 逐台结构化结果（manager 侧还原注册结果表）：{key: {ok, msg, name, address}}
        self.host_results = {}
        self._client = None
        self._cancelled = False
        self._log_file = None

    async def run(self, on_lines):
        """逐台注册，返回 (rc, elapsed)。

        rc：0 = 全部成功（含已存在跳过）；1 = 有主机失败或整批中止；
        124 超时；126 payload 或 agent 配置错误；143 manager 取消。
        """
        started = time.monotonic()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        timeout = min(int(self.payload.get('timeout') or 300), self.config.job_timeout_max)
        deadline = started + timeout
        self._log_file = (self.artifacts / 'stdout.log').open('w', encoding='utf-8')
        try:
            rc = await self._execute(on_lines, deadline)
        except JumpServerError as exc:
            await self._emit(on_lines, [f'✖ {exc.message}'])
            rc = exc.rc
        finally:
            self._log_file.close()
        elapsed = round(time.monotonic() - started, 2)
        (self.artifacts / 'rc').write_text(str(rc), encoding='utf-8')
        logger.info('job %s 结束：rc=%s elapsed=%ss 主机 %s 台',
                    self.job_id, rc, elapsed, len(self.host_results))
        return rc, elapsed

    async def cancel(self):
        """manager 取消 / agent 停机：标记取消，下一台之前收尾。"""
        self._cancelled = True

    # ── 主流程 ────────────────────────────────────────────

    async def _execute(self, on_lines, deadline):
        hosts = self._parse_hosts()
        if not self.jumpserver.configured:
            raise JumpServerError(RC_PAYLOAD_ERROR,
                                  'agent 未配置 JumpServer 连接信息：请在 agent 主机 '
                                  'config.yaml 的 jumpserver 段填写 url / token。')
        self._client = JumpServerClient(self.jumpserver)
        await self._emit(on_lines, [f'[jumpserver] {self.jumpserver.base_url} '
                                    f'注册目标：{len(hosts)} 台'])

        failed = 0
        for host in hosts:
            self._check_interrupt(deadline)
            result = await asyncio.to_thread(self._register_one, host)
            self.host_results[host['key']] = {
                'name': host['name'], 'address': host['address'], **result}
            if not result['ok']:
                failed += 1
            mark = '✔' if result['ok'] else '✖'
            await self._emit(
                on_lines, [f'{mark} {host["name"]}({host["address"]})：{result["msg"]}'])

        await self._emit(on_lines, [f'[jumpserver] 完成：成功 {len(hosts) - failed}，'
                                    f'失败 {failed}'])
        return 0 if failed == 0 else 1

    def _parse_hosts(self):
        """校验并归一化下发的主机清单，非法内容抛 rc=126。"""
        hosts = self.payload.get('hosts')
        if not isinstance(hosts, list) or not hosts:
            raise JumpServerError(RC_PAYLOAD_ERROR, '下发内容未携带待注册主机清单（hosts）。')
        if len(hosts) > MAX_HOSTS:
            raise JumpServerError(
                RC_PAYLOAD_ERROR,
                f'单次下发主机数 {len(hosts)} 超过上限 {MAX_HOSTS}，请分批注册。')
        parsed = []
        for index, raw in enumerate(hosts):
            if not isinstance(raw, dict):
                raise JumpServerError(RC_PAYLOAD_ERROR, f'主机清单第 {index + 1} 项格式非法。')
            name = str(raw.get('name') or '').strip()
            address = str(raw.get('address') or '').strip()
            if not (name and address):
                raise JumpServerError(
                    RC_PAYLOAD_ERROR, f'主机清单第 {index + 1} 项缺少 name 或 address。')
            parsed.append({
                'key': str(raw.get('key') or name),
                'name': name,
                'address': address,
                'ssh_port': int(raw.get('ssh_port') or 22),
                'comment': str(raw.get('comment') or ''),
            })
        return parsed

    def _register_one(self, host):
        """注册单台（阻塞，经 to_thread 调用），返回 {'ok': bool, 'msg': str}。

        重名（创建返回 400 且报 name）按整批中止处理：与 manager 直连路径一致，
        禁止自动改名，交由使用者改名后重来。
        """
        try:
            resp = self._client.get('/api/v1/assets/hosts/', params={'name': host['name']})
            if resp.status == 401:
                return {'ok': False, 'msg': 'JumpServer 认证失败（token 无效或已过期）'}
            if resp.ok:
                data = resp.json_any()
                existing = data.get('results', data) if isinstance(data, dict) else data
                if existing:
                    return {'ok': True, 'msg': '已存在，跳过'}

            body = {
                'name': host['name'],
                'address': host['address'],
                'platform': {'pk': int(self.jumpserver.platform_id or 1)},
                'protocols': [{'name': 'ssh', 'port': host['ssh_port']}],
                'is_active': True,
                'comment': host['comment'],
            }
            if self.jumpserver.node_id:
                body['nodes'] = [{'pk': self.jumpserver.node_id}]
            resp = self._client.post('/api/v1/assets/hosts/', body)
            if resp.status == 400 and 'name' in (resp.text or ''):
                # 兜底：查重与创建之间的并发窗口，或查重接口异常时命中资产名唯一约束
                raise JumpServerError(
                    1, f'主机「{host["name"]}」（{host["address"]}）与 JumpServer 已有资产重名，'
                       '禁止同步，请先修改名称保证唯一。')
            if resp.status in (200, 201):
                return {'ok': True, 'msg': '注册成功'}
            return {'ok': False,
                    'msg': f'注册失败：HTTP {resp.status} {(resp.text or "")[:200]}'}
        except HttpError as exc:
            return {'ok': False, 'msg': f'请求异常：{exc}'}

    # ── 中断与输出 ────────────────────────────────────────

    def _check_interrupt(self, deadline):
        """每台之前检查取消与超时（已注册的主机保留，不回滚）。"""
        if self._cancelled:
            raise JumpServerError(RC_CANCELLED, '注册被 manager 取消，剩余主机未处理。')
        if time.monotonic() >= deadline:
            raise JumpServerError(RC_TIMEOUT, '注册等待超时，剩余主机未处理。')

    async def _emit(self, on_lines, lines):
        """tee 到 artifacts/stdout.log（全量）+ 回传。

        注册输出为每台一行，量级远小于 max_job_log_lines（主机数另有 MAX_HOSTS
        上限兜底），不做截断。
        """
        for line in lines:
            self._log_file.write(line + '\n')
        await on_lines(lines)
