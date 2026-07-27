"""任务执行器：payload → ansible 子进程，流式产出日志行。

- 子进程独立进程组（start_new_session），超时/取消先 SIGTERM 后 SIGKILL；
- stdout/stderr 合流逐行读取，tee 到 artifacts/<job_id>/stdout.log 与回调；
- 剧本路径限制在 ansible_project_dir 内（路径穿越防护）；
- forks / timeout 均与配置封顶取小。
"""
import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path

logger = logging.getLogger('ops_agent.executor')

TERM_GRACE = 5  # SIGTERM 后等待秒数，仍存活则 SIGKILL


class PayloadError(Exception):
    """payload 非法（缺字段/路径越界等），直接拒收。"""


def build_env() -> dict:
    """关闭交互/主机指纹校验的 ansible 运行环境（与 manager 同口径）。"""
    env = os.environ.copy()
    venv_bin = os.path.dirname(sys.executable)
    env['PATH'] = venv_bin + os.pathsep + env.get('PATH', '')
    env['ANSIBLE_HOST_KEY_CHECKING'] = 'False'
    env['ANSIBLE_RETRY_FILES_ENABLED'] = 'False'
    env['ANSIBLE_FORCE_COLOR'] = 'False'
    env['ANSIBLE_NOCOWS'] = '1'
    return env


def resolve_within(project_dir: str, rel_or_abs: str) -> str:
    """解析剧本路径并防止越出项目目录，越界抛 PayloadError。"""
    base = os.path.realpath(project_dir)
    target = rel_or_abs if os.path.isabs(rel_or_abs) else os.path.join(base, rel_or_abs)
    target = os.path.realpath(target)
    if not (target == base or target.startswith(base + os.sep)):
        raise PayloadError(f'剧本路径越出项目目录：{rel_or_abs}')
    return target


def build_argv(payload: dict, inventory_path: str, config) -> tuple:
    """构造 ansible 命令行，返回 (argv, cwd)。"""
    step_type = payload.get('step_type')
    forks = min(int(payload.get('forks') or 10), config.max_forks)
    extra_vars = payload.get('extra_vars') or []

    if step_type == 'command':
        command = (payload.get('command') or '').strip()
        if not command:
            raise PayloadError('command 为空')
        argv = ['ansible', 'all', '-i', inventory_path, '--forks', str(forks)]
        for value in extra_vars:
            argv += ['--extra-vars', value]
        argv += ['-m', 'shell', '-a', command]
        return argv, None

    if step_type == 'playbook':
        playbook = (payload.get('playbook') or '').strip()
        if not playbook:
            raise PayloadError('playbook 为空')
        project_dir = config.ansible_project_dir
        if not os.path.isdir(project_dir):
            raise PayloadError(f'Ansible 项目目录不存在：{project_dir}')
        playbook_path = resolve_within(project_dir, playbook)
        if not os.path.isfile(playbook_path):
            raise PayloadError(f'剧本文件不存在：{playbook}')
        argv = ['ansible-playbook', playbook_path, '-i', inventory_path,
                '--forks', str(forks)]
        if (payload.get('tags') or '').strip():
            argv += ['--tags', payload['tags'].strip()]
        for value in extra_vars:
            argv += ['--extra-vars', value]
        return argv, project_dir

    raise PayloadError(f'不支持的 step_type：{step_type}')


class JobExecution:
    """一次任务单元的执行句柄：run() 产出日志行，cancel() 终止进程组。"""

    def __init__(self, job_id: str, payload: dict, config):
        self.job_id = job_id
        self.payload = payload
        self.config = config
        self.artifacts = Path(config.artifacts_dir) / job_id
        self._proc = None
        self._cancelled = False

    async def run(self, on_lines):
        """执行任务，日志行经 on_lines(list[str]) 回调（含截断/系统提示行）。

        返回 (rc, elapsed)。rc 语义：ansible 原生 rc；124 超时；143 取消；
        126 payload 或环境错误。
        """
        started = time.monotonic()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        timeout = min(int(self.payload.get('timeout') or 300), self.config.job_timeout_max)

        try:
            if self.payload.get('project_sync') and self.payload.get('step_type') == 'playbook':
                rc = await self._project_sync(on_lines)
                if rc != 0:
                    return await self._finish(126, started, on_lines)
            inventory_path = self.artifacts / 'inventory.ini'
            inventory_path.write_text(self.payload.get('inventory') or '', encoding='utf-8')
            argv, cwd = build_argv(self.payload, str(inventory_path), self.config)
        except PayloadError as exc:
            await on_lines([f'✖ {exc}'])
            return await self._finish(126, started, on_lines)

        logger.info('job %s 启动：%s', self.job_id, ' '.join(argv))
        self._proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=build_env(),
            cwd=cwd,
            start_new_session=True,  # 独立进程组：连带 ssh 子进程一起终止
        )
        try:
            rc = await asyncio.wait_for(self._stream(on_lines), timeout=timeout)
        except asyncio.TimeoutError:
            await self._kill_group()
            await on_lines([f'✖ 任务执行超时（>{timeout}s），已终止。'])
            rc = 124
        if self._cancelled:
            rc = 143
        return await self._finish(rc, started, on_lines)

    async def cancel(self):
        """manager 取消 / agent 停机：终止进程组。"""
        self._cancelled = True
        await self._kill_group()

    # ── 私有 ──────────────────────────────────────────────

    async def _stream(self, on_lines) -> int:
        """逐行读子进程输出：tee 到 artifacts/stdout.log + 回调；返回 rc。"""
        max_lines = self.config.max_job_log_lines
        emitted = 0
        truncated = False
        with (self.artifacts / 'stdout.log').open('w', encoding='utf-8') as log_file:
            while True:
                raw = await self._proc.stdout.readline()
                if not raw:
                    break
                line = raw.decode('utf-8', 'replace').rstrip('\n')
                log_file.write(line + '\n')  # 本地全量留存，不受回传截断影响
                if emitted < max_lines:
                    emitted += 1
                    await on_lines([line])
                elif not truncated:
                    truncated = True
                    await on_lines([f'…（回传日志超过 {max_lines} 行已截断，'
                                    f'完整日志见 agent 主机 {self.artifacts}/stdout.log）'])
        return await self._proc.wait()

    async def _project_sync(self, on_lines) -> int:
        """执行剧本前同步 Ansible 项目目录（git pull）。"""
        cmd = self.config.project_sync_cmd
        if not cmd.strip():
            return 0
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=self.config.ansible_project_dir,
            start_new_session=True,
        )
        try:
            out, _ = await asyncio.wait_for(
                proc.communicate(), timeout=self.config.project_sync_timeout)
        except asyncio.TimeoutError:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            await on_lines([f'✖ 剧本同步超时（{cmd}），任务失败。'])
            return 1
        if proc.returncode != 0:
            text = out.decode('utf-8', 'replace').strip()
            await on_lines([f'✖ 剧本同步失败（{cmd}，rc={proc.returncode}）：{text}'])
        return proc.returncode

    async def _kill_group(self):
        if self._proc is None or self._proc.returncode is not None:
            return
        try:
            pgid = os.getpgid(self._proc.pid)
        except ProcessLookupError:
            return
        try:
            os.killpg(pgid, signal.SIGTERM)
            await asyncio.wait_for(self._proc.wait(), timeout=TERM_GRACE)
        except asyncio.TimeoutError:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        except (ProcessLookupError, PermissionError):
            pass

    async def _finish(self, rc, started, on_lines):
        elapsed = round(time.monotonic() - started, 2)
        (self.artifacts / 'rc').write_text(str(rc), encoding='utf-8')
        logger.info('job %s 结束：rc=%s elapsed=%ss', self.job_id, rc, elapsed)
        return rc, elapsed
