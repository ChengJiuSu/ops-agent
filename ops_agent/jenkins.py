"""Jenkins 构建执行器：触发 job、跟踪构建、流式回传控制台日志。

manager 在公网而 Jenkins 在内网时，manager 侧 jenkins 方法把执行单元
（`step_type='jenkins'`）下发到本 agent，由 agent 走 Jenkins REST API：

    buildWithParameters 触发 → 轮询队列拿 build 号 → progressiveText 增量拉
    控制台日志（逐行经 on_lines 回传）→ api/json 取最终构建结果

Jenkins 连接信息只在 agent 本地 config.yaml 的 `jenkins:` 段维护（与 SSH
私钥同口径：密钥不出通道、不入 manager 库），payload 只带 job 名与构建参数。

与 executor.JobExecution 同接口（kind / run / cancel），故复用 connection 的
LogPump、spool 分片回传、ack 补传、artifacts 留存全套设施。

HTTP 走 httpclient.HttpClient（标准库 urllib 封装，与 JumpServer 注册共用），
阻塞调用一律经 asyncio.to_thread 出让事件循环，避免卡住心跳。
"""
import asyncio
import base64
import logging
import re
import time
import urllib.parse
from pathlib import Path

from .httpclient import HttpClient, HttpError

logger = logging.getLogger('ops_agent.jenkins')

HTTP_TIMEOUT = 30        # 单次 REST 请求超时（秒）
RC_TIMEOUT = 124         # 等待构建超时
RC_PAYLOAD_ERROR = 126   # payload / agent 配置错误
RC_CANCELLED = 143       # manager 取消

# Jenkins ConsoleNote 注解：日志流中形如 \x1b[8mha:<base64>\x1b[0m 的隐藏段
# （网页用它渲染超链接），progressiveText 原样透出，平台展示需剥除。
# ESC 部分按可选匹配：传输/存储中可能丢失。
CONSOLE_NOTE_RE = re.compile(r'(?:\x1b\[8m)?ha:////[A-Za-z0-9+/=]+(?:\x1b\[0m)?')
ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*m')


class JenkinsError(Exception):
    """流程失败：携带 rc 与中文原因，在 run 出口统一转为返回值。"""

    def __init__(self, rc, message):
        super().__init__(message)
        self.rc = rc
        self.message = message


class JenkinsClient:
    """Jenkins REST 客户端：共用 HttpClient，连接层失败转 JenkinsError。"""

    def __init__(self, config):
        token = f'{config.user}:{config.api_token}'.encode('utf-8')
        self._client = HttpClient(
            headers={'Authorization': 'Basic ' + base64.b64encode(token).decode('ascii')},
            verify_tls=config.verify_tls, ca_file=config.ca_file, timeout=HTTP_TIMEOUT)

    def set_header(self, key, value):
        self._client.set_header(key, value)

    def request(self, method, url, params=None):
        """发一次请求；连接异常抛 JenkinsError(rc=1)，HTTP 错误码正常返回。"""
        try:
            return self._client.request(method, url, params)
        except HttpError as exc:
            raise JenkinsError(1, f'Jenkins {exc}') from exc


class JenkinsExecution:
    """一次 Jenkins 构建单元：run() 流式产出日志行，cancel() 中止构建。"""

    kind = 'jenkins'

    def __init__(self, job_id: str, payload: dict, config):
        self.job_id = job_id
        self.payload = payload
        self.config = config
        self.jenkins = config.jenkins
        self.artifacts = Path(config.artifacts_dir) / job_id
        self._client = None
        self._cancelled = False
        self._build_url = ''
        self._log_file = None
        self._emitted = 0
        self._truncated = False

    async def run(self, on_lines):
        """触发并跟踪构建，返回 (rc, elapsed)。

        rc：0 = SUCCESS（或 UNSTABLE 且 allow_unstable）；1 = 构建失败/请求失败；
        124 等待超时；126 payload 或 agent 配置错误；143 manager 取消。
        """
        started = time.monotonic()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        timeout = min(int(self.payload.get('timeout') or 300), self.config.job_timeout_max)
        deadline = started + timeout
        self._log_file = (self.artifacts / 'stdout.log').open('w', encoding='utf-8')
        try:
            rc = await self._execute(on_lines, deadline)
        except JenkinsError as exc:
            await self._emit(on_lines, [f'✖ {exc.message}'])
            rc = exc.rc
        finally:
            self._log_file.close()
        elapsed = round(time.monotonic() - started, 2)
        (self.artifacts / 'rc').write_text(str(rc), encoding='utf-8')
        logger.info('job %s 结束：rc=%s elapsed=%ss build=%s',
                    self.job_id, rc, elapsed, self._build_url or '-')
        return rc, elapsed

    async def cancel(self):
        """manager 取消 / agent 停机：标记取消，由轮询节拍中止构建并收尾。"""
        self._cancelled = True

    # ── 主流程 ────────────────────────────────────────────

    async def _execute(self, on_lines, deadline):
        job_name = str(self.payload.get('job_name') or '').strip()
        if not job_name:
            raise JenkinsError(RC_PAYLOAD_ERROR, '下发内容未指定 Jenkins job_name。')
        if not self.jenkins.configured:
            raise JenkinsError(RC_PAYLOAD_ERROR,
                               'agent 未配置 Jenkins 连接信息：请在 agent 主机 '
                               'config.yaml 的 jenkins 段填写 url / user / api_token。')
        params = {str(k): str(v) for k, v in (self.payload.get('params') or {}).items()}
        self._client = JenkinsClient(self.jenkins)

        queue_url = await self._trigger(job_name, params)
        build_url, number = await self._wait_queue(queue_url, deadline)
        self._build_url = build_url
        await self._emit(on_lines, [
            f'[jenkins] job={job_name} build=#{number}',
            f'[jenkins] url={build_url}',
            '─' * 30 + ' Jenkins Console Output ' + '─' * 30,
        ])

        await self._stream_console(build_url, on_lines, deadline)
        result = await self._final_result(build_url, deadline)
        allow_unstable = bool(self.payload.get('allow_unstable'))
        rc = 0 if result == 'SUCCESS' or (result == 'UNSTABLE' and allow_unstable) else 1
        await self._emit(on_lines, [
            '─' * 84,
            f'[jenkins] 构建结果：{result or "未知（等待结果超时）"}',
        ])
        return rc

    async def _trigger(self, job_name, params):
        """触发构建，返回队列 URL。"""
        action = 'buildWithParameters' if params else 'build'
        endpoint = f'{self.jenkins.base_url}/job/{urllib.parse.quote(job_name)}/{action}'
        resp = await self._call('POST', endpoint, params)
        if resp.status == 403 and await self._load_crumb():
            # 旧版 Jenkins / 密码模式需要 CSRF crumb，取一次后重试
            resp = await self._call('POST', endpoint, params)
        if resp.status != 201:
            raise JenkinsError(1, f'触发 Jenkins 构建失败：HTTP {resp.status} '
                                  f'{resp.text[:500]}\n[jenkins] endpoint={endpoint}')
        queue_url = resp.headers.get('Location', '')
        if not queue_url:
            raise JenkinsError(1, '触发成功但 Jenkins 未返回队列地址（Location 头缺失），'
                                  '无法跟踪构建。')
        return queue_url.rstrip('/') + '/'

    async def _load_crumb(self):
        """取 CSRF crumb 挂到后续请求头上，成功返回 True。"""
        try:
            resp = await self._call('GET', f'{self.jenkins.base_url}/crumbIssuer/api/json')
        except JenkinsError:
            return False
        data = resp.json() if resp.status == 200 else {}
        field, crumb = data.get('crumbRequestField'), data.get('crumb')
        if not (field and crumb):
            return False
        self._client.set_header(field, crumb)
        return True

    async def _wait_queue(self, queue_url, deadline):
        """轮询队列项直至分配 build 号，返回 (build_url, number)。"""
        while True:
            resp = await self._call('GET', f'{queue_url}api/json')
            item = resp.json() if resp.status == 200 else {}
            if item.get('cancelled'):
                raise JenkinsError(1, '构建在 Jenkins 队列中被取消。')
            executable = item.get('executable') or {}
            if executable.get('url'):
                return executable['url'].rstrip('/') + '/', executable.get('number')
            await self._tick(deadline, f'等待 Jenkins 构建出队超时（队列：{queue_url}）。')

    async def _stream_console(self, build_url, on_lines, deadline):
        """增量拉控制台日志逐行回传；分片末行可能被截半，留到下轮再拼。"""
        start = 0
        partial = ''
        while True:
            resp = await self._call('GET', f'{build_url}logText/progressiveText',
                                    {'start': start})
            text = resp.text if resp.status == 200 else ''
            start = int(resp.headers.get('X-Text-Size') or start)
            more = resp.headers.get('X-More-Data') == 'true'
            if text:
                partial += ANSI_ESCAPE_RE.sub('', CONSOLE_NOTE_RE.sub('', text))
                lines = partial.split('\n')
                partial = lines.pop()
                if lines:
                    await self._emit(on_lines, lines)
            if not more:
                if partial:
                    await self._emit(on_lines, [partial])
                return
            await self._tick(
                deadline, f'等待 Jenkins 构建完成超时（构建可能仍在运行：{build_url}）。')

    async def _final_result(self, build_url, deadline):
        """取构建结果。日志结束时 result 可能尚未落库，短轮询兜底；超时返回 None。"""
        while True:
            resp = await self._call('GET', f'{build_url}api/json')
            result = resp.json().get('result') if resp.status == 200 else None
            if result:
                return result
            try:
                await self._tick(deadline, '')
            except JenkinsError as exc:
                if exc.rc == RC_TIMEOUT:
                    return None  # 构建已跑完，只是结果没等到：交由上层标未知
                raise

    # ── 轮询节拍与收尾 ────────────────────────────────────

    async def _call(self, method, url, params=None):
        """阻塞 HTTP 调用出让事件循环，避免卡住心跳与其他任务。"""
        return await asyncio.to_thread(self._client.request, method, url, params)

    async def _tick(self, deadline, timeout_message):
        """一次轮询间隔：期间响应取消，到点抛超时。两者都先按配置中止构建。"""
        if self._cancelled:
            await self._abort('manager 取消')
            raise JenkinsError(RC_CANCELLED, '构建被 manager 取消。')
        if time.monotonic() >= deadline:
            await self._abort('等待超时')
            raise JenkinsError(RC_TIMEOUT, timeout_message or '等待 Jenkins 构建结果超时。')
        await asyncio.sleep(self.jenkins.poll_interval)

    async def _abort(self, reason):
        """按 abort_on_cancel 中止 Jenkins 上仍在跑的构建。"""
        if not (self.jenkins.abort_on_cancel and self._build_url):
            return
        try:
            resp = await self._call('POST', f'{self._build_url}stop')
        except JenkinsError as exc:
            logger.warning('job %s 中止构建失败：%s', self.job_id, exc.message)
            return
        logger.info('job %s %s，已请求中止构建 %s（HTTP %s）',
                    self.job_id, reason, self._build_url, resp.status)

    async def _emit(self, on_lines, lines):
        """tee 到 artifacts/stdout.log（全量）+ 回传（受行数上限截断）。"""
        for line in lines:
            self._log_file.write(line + '\n')
        max_lines = self.config.max_job_log_lines
        allowed = lines[:max(0, max_lines - self._emitted)]
        self._emitted += len(allowed)
        if allowed:
            await on_lines(allowed)
        if self._emitted >= max_lines and not self._truncated:
            self._truncated = True
            await on_lines([f'…（回传日志超过 {max_lines} 行已截断，完整日志见 agent 主机 '
                            f'{self.artifacts}/stdout.log）'])
