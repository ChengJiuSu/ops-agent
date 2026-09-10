"""AgentApp：连接管理 + 消息路由 + 任务生命周期。

- 主动外连 manager（wss），指数退避重连（1s→2s→…封顶，±20% 抖动）；
- 断线不中断执行中任务：日志分片先落 spool，重连后按 ack 游标补传；
- 并发/排队/磁盘水位超限即 task.ack 拒收（ansible / jenkins / jumpserver 各一批槽位）；
- 崩溃恢复：启动扫描 spool，无终态的任务标记 rc=137 中断上报。
"""
import asyncio
import json
import logging
import os
import random
import shutil
import time

import websockets

from . import __version__
from .executor import CAPABILITIES, PayloadError, job_kind, make_execution, redact_payload
from .spool import JobSpool, pending_jobs

logger = logging.getLogger('ops_agent')

CLEANUP_INTERVAL = 3600  # 留存清理周期（秒）
RC_INTERRUPTED = 137     # agent 崩溃/重启导致任务中断
RC_PAYLOAD_ERROR = 126   # payload/环境错误
# 槽位中文名：拒收原因文案用
KIND_LABELS = {'ansible': 'ansible', 'jenkins': 'Jenkins', 'jumpserver': 'JumpServer'}


class LogPump:
    """单任务日志缓冲：行 → 定期/满额冲刷为 spool 分片并尝试回传。"""

    def __init__(self, app, spool, flush_interval, chunk_lines):
        self._app = app
        self._spool = spool
        self._interval = flush_interval
        self._chunk_lines = chunk_lines
        self._buffer = []
        self._task = asyncio.ensure_future(self._loop())

    async def on_lines(self, lines):
        self._buffer.extend(lines)
        if len(self._buffer) >= self._chunk_lines:
            await self._flush()

    async def close(self):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        await self._flush()

    async def _loop(self):
        while True:
            await asyncio.sleep(self._interval)
            await self._flush()

    async def _flush(self):
        if not self._buffer:
            return
        lines, self._buffer = self._buffer, []
        seq = self._spool.append_chunk(lines)
        await self._app.send('log.chunk', {
            'job_id': self._spool.job_id, 'chunk_seq': seq, 'lines': lines})


class AgentApp:
    def __init__(self, config):
        self.config = config
        self._ws = None
        self._seq = 0
        self._stopping = False
        self._send_lock = asyncio.Lock()
        # 按槽位归属分池：jenkins/jumpserver 是纯 IO 等待，不与 ansible 抢并发额度
        self._concurrency = {
            'ansible': config.max_concurrent_jobs,
            'jenkins': config.max_concurrent_jenkins_jobs,
            'jumpserver': config.max_concurrent_jumpserver_jobs,
        }
        self._slots = {k: asyncio.Semaphore(v) for k, v in self._concurrency.items()}
        self._running = dict.fromkeys(self._concurrency, 0)
        self._queued = dict.fromkeys(self._concurrency, 0)
        self.executions = {}   # job_id -> 执行句柄（运行中）
        self.spools = {}       # job_id -> JobSpool（运行中 + 待补传孤儿）
        self._job_tasks = set()

    # ── 生命周期 ──────────────────────────────────────────

    async def run(self):
        self._recover_startup()
        cleanup = asyncio.ensure_future(self._cleanup_loop())
        delay = 1
        while not self._stopping:
            try:
                async with self._connect() as ws:
                    self._ws = ws
                    delay = 1
                    logger.info('已连接 manager：%s', self.config.manager_url)
                    await self.send('register', {
                        'agent_name': self.config.agent_name,
                        'version': __version__,
                        'hostname': os.uname().nodename,
                        'capabilities': CAPABILITIES,
                    })
                    await self._resend_pending()
                    heartbeat = asyncio.ensure_future(self._heartbeat_loop())
                    try:
                        await self._recv_loop(ws)
                    finally:
                        heartbeat.cancel()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning('连接断开：%s', exc)
            finally:
                self._ws = None
            if self._stopping:
                break
            jitter = delay * random.uniform(0.8, 1.2)
            logger.info('%.1fs 后重连…', jitter)
            await asyncio.sleep(jitter)
            delay = min(delay * 2, self.config.reconnect_max_delay)
        cleanup.cancel()

    async def stop(self):
        """SIGTERM 停机：终止运行中任务（进程组 SIGTERM），中断连接。"""
        logger.info('收到停机信号，终止 %d 个运行中任务…', len(self.executions))
        self._stopping = True
        for execution in list(self.executions.values()):
            await execution.cancel()
        if self._ws is not None:
            await self._ws.close()

    def _connect(self):
        kwargs = {
            'ping_interval': 30,
            'ping_timeout': 30,
            'max_size': 4 * 1024 * 1024,
        }
        if self.config.tls_ca_file:
            import ssl

            kwargs['ssl'] = ssl.create_default_context(cafile=self.config.tls_ca_file)
        headers = {'Authorization': f'Token {self.config.token}'}
        try:
            return websockets.connect(
                self.config.manager_url, additional_headers=headers, **kwargs)
        except TypeError:  # websockets < 14 参数名为 extra_headers
            return websockets.connect(
                self.config.manager_url, extra_headers=headers, **kwargs)

    # ── 收发 ──────────────────────────────────────────────

    async def send(self, msg_type, data):
        """发送一帧；未连接/发送失败静默丢弃（数据已先落 spool，重连补传）。"""
        ws = self._ws
        if ws is None:
            return
        self._seq += 1
        frame = json.dumps({
            'type': msg_type, 'seq': self._seq, 'ts': int(time.time()), 'data': data,
        }, ensure_ascii=False)
        try:
            async with self._send_lock:
                await ws.send(frame)
        except Exception as exc:
            logger.debug('发送失败（%s），等待重连补传：%s', msg_type, exc)

    async def _recv_loop(self, ws):
        async for raw in ws:
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning('收到非法帧：%.200s', raw)
                continue
            msg_type, data = frame.get('type'), frame.get('data') or {}
            if msg_type == 'task.dispatch':
                await self._on_dispatch(data)
            elif msg_type == 'task.cancel':
                await self._on_cancel(data)
            elif msg_type == 'log.ack':
                self._on_log_ack(data)
            elif msg_type == 'result.ack':
                self._on_result_ack(data)
            elif msg_type == 'register.ack':
                logger.info('注册确认：%s', data)
            elif msg_type == 'error':
                logger.warning('manager 报错：%s', data)
            else:
                logger.warning('未知消息类型：%s', msg_type)

    async def _heartbeat_loop(self):
        while True:
            await self.send('heartbeat', {
                'version': __version__,
                'running_jobs': len(self.executions),
                'load1': round(os.getloadavg()[0], 2),
                'disk_free_pct': self._disk_free_pct(),
            })
            await asyncio.sleep(self.config.heartbeat_interval)

    # ── 任务处理 ──────────────────────────────────────────

    async def _on_dispatch(self, payload):
        job_id = str(payload.get('job_id') or '')
        if not job_id:
            return
        if job_id in self.spools:
            # 幂等：重复下发直接受理确认，不重复执行
            await self.send('task.ack', {'job_id': job_id, 'accepted': True})
            return
        kind = job_kind(payload)
        reason = (f'agent 不支持的执行类型 step_type={payload.get("step_type")}'
                  f'（agent 版本过低？本 agent 支持：{", ".join(CAPABILITIES)}）'
                  if kind is None else self._reject_reason(kind))
        if reason:
            logger.warning('拒收 job %s：%s', job_id, reason)
            await self.send('task.ack', {'job_id': job_id, 'accepted': False, 'reason': reason})
            return
        await self.send('task.ack', {'job_id': job_id, 'accepted': True})
        spool = JobSpool(self.config.spool_dir, job_id)
        # 敏感字段（证书内容）不落 spool：恢复流程只标中断、不重放执行
        spool.save_payload(redact_payload(payload))
        self.spools[job_id] = spool
        task = asyncio.ensure_future(self._run_job(job_id, payload, spool, kind))
        self._job_tasks.add(task)
        task.add_done_callback(self._job_tasks.discard)

    async def _run_job(self, job_id, payload, spool, kind):
        self._queued[kind] += 1
        try:
            async with self._slots[kind]:
                self._queued[kind] -= 1
                self._running[kind] += 1
                pump = LogPump(self, spool, self.config.log_flush_interval,
                               self.config.log_chunk_lines)
                try:
                    rc, elapsed, host_results = await self._execute(job_id, payload, pump)
                finally:
                    self._running[kind] -= 1
                    self.executions.pop(job_id, None)
                    await pump.close()
                result = {'rc': rc, 'elapsed': elapsed,
                          'host_results': host_results}
                spool.save_result(result)
                await self.send('task.result', {'job_id': job_id, **result})
        except Exception:
            self._queued[kind] = max(0, self._queued[kind] - 1)
            logger.exception('job %s 调度异常', job_id)

    async def _execute(self, job_id, payload, pump):
        """建执行句柄并跑完，返回 (rc, elapsed, host_results)；异常统一转 rc=126。

        host_results 为执行句柄可选提供的逐主机结构化结果（JumpServer 注册用，
        manager 侧据此还原每台的成功/跳过/失败），未提供则为空。
        """
        try:
            execution = make_execution(job_id, payload, self.config)
        except PayloadError as exc:
            await pump.on_lines([f'✖ {exc}'])
            return RC_PAYLOAD_ERROR, 0, {}
        self.executions[job_id] = execution
        try:
            rc, elapsed = await execution.run(pump.on_lines)
        except Exception as exc:  # noqa: BLE001 - 任何执行异常都要有终态回传
            logger.exception('job %s 执行异常', job_id)
            await pump.on_lines([f'✖ agent 内部错误：{exc}'])
            rc, elapsed = RC_PAYLOAD_ERROR, 0
        return rc, elapsed, getattr(execution, 'host_results', None) or {}

    async def _on_cancel(self, data):
        job_id = str(data.get('job_id') or '')
        execution = self.executions.get(job_id)
        if execution is not None:
            logger.info('取消 job %s', job_id)
            await execution.cancel()

    def _on_log_ack(self, data):
        spool = self.spools.get(str(data.get('job_id') or ''))
        if spool is not None:
            spool.mark_acked(int(data.get('chunk_seq') or 0))

    def _on_result_ack(self, data):
        job_id = str(data.get('job_id') or '')
        spool = self.spools.pop(job_id, None)
        if spool is not None:
            spool.mark_result_acked()
            spool.cleanup()
            logger.info('job %s 已确认并清理 spool（artifacts 留存至过期）', job_id)

    def _reject_reason(self, kind):
        concurrency = self._concurrency[kind]
        running, queued = self._running[kind], self._queued[kind]
        if running + queued >= concurrency + self.config.max_queued_jobs:
            return (f'{KIND_LABELS[kind]} 通道并发与排队均已满（运行 {running}，'
                    f'排队 {queued}），请稍后重试。')
        free_pct = self._disk_free_pct()
        if free_pct < self.config.disk_min_free_pct:
            return f'agent 磁盘空闲不足（{free_pct}% < {self.config.disk_min_free_pct}%），拒收新任务。'
        return None

    # ── 补传与恢复 ────────────────────────────────────────

    async def _resend_pending(self):
        """重连后：按 ack 游标补传日志分片，重发未确认终态。"""
        for job_id, spool in list(self.spools.items()):
            for chunk in spool.unacked_chunks():
                await self.send('log.chunk', {'job_id': job_id, **chunk})
            result = spool.result()
            if result is not None and not spool.result_acked():
                await self.send('task.result', {'job_id': job_id, **result})

    def _recover_startup(self):
        """启动扫描 spool：无终态的任务（进程已随崩溃消亡）标记中断。"""
        for spool in pending_jobs(self.config.spool_dir):
            if spool.result_acked():
                spool.cleanup()
                continue
            if spool.result() is None:
                spool.append_chunk(['✖ agent 重启，任务执行中断。'])
                spool.save_result({'rc': RC_INTERRUPTED, 'elapsed': 0, 'host_results': {}})
                logger.warning('恢复：job %s 标记为中断（rc=%d）', spool.job_id, RC_INTERRUPTED)
            self.spools[spool.job_id] = spool

    # ── 留存清理 ──────────────────────────────────────────

    async def _cleanup_loop(self):
        while True:
            try:
                self._purge_artifacts()
            except Exception:
                logger.exception('artifacts 清理失败')
            await asyncio.sleep(CLEANUP_INTERVAL)

    def _purge_artifacts(self):
        """按留存天数清理 artifacts；磁盘低于水位时从最旧开始加速清。"""
        root = self.config.artifacts_dir
        if not root.is_dir():
            return
        deadline = time.time() - self.config.log_retention_days * 86400
        entries = sorted(root.iterdir(), key=lambda p: p.stat().st_mtime)
        for path in entries:
            expired = path.stat().st_mtime < deadline
            low_disk = self._disk_free_pct() < self.config.disk_min_free_pct
            if not (expired or low_disk):
                break
            if path.name in self.executions:  # 运行中任务不清
                continue
            shutil.rmtree(path, ignore_errors=True)
            logger.info('清理 artifacts：%s（%s）', path.name, '过期' if expired else '磁盘水位')

    def _disk_free_pct(self):
        usage = shutil.disk_usage(self.config.data_dir)
        return round(usage.free / usage.total * 100, 1)
