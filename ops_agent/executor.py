"""任务执行器：payload → ansible 子进程，流式产出日志行。

- 子进程独立进程组（start_new_session），超时/取消先 SIGTERM 后 SIGKILL；
- stdout/stderr 合流逐行读取，tee 到 artifacts/<job_id>/stdout.log 与回调；
- 剧本路径限制在 ansible_project_dir 内（路径穿越防护）；
- forks / timeout 均与配置封顶取小。

make_execution 为按 step_type 选实现的工厂：command/playbook/cert_deploy 走
本模块的 JobExecution（ansible 子进程），jenkins 走 jenkins.JenkinsExecution
（REST 轮询）。两者接口一致（kind / run(on_lines) / cancel()）。

cert_deploy（证书分发）：payload 带文件清单与目标目录，剧本由本模块**本地
生成**（不接受下发的剧本内容，执行面仍只有 copy/file/shell 三个动作）；证书
内容只写 0700 临时目录内的 0600 文件，执行完连目录一起删，不进 artifacts、
不进 spool（见 redact_payload）。
"""
import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from pathlib import Path

logger = logging.getLogger('ops_agent.executor')

TERM_GRACE = 5  # SIGTERM 后等待秒数，仍存活则 SIGKILL

# step_type → 并发槽位归属（ansible 子进程 / Jenkins REST 轮询各占一批槽）
STEP_KINDS = {'command': 'ansible', 'playbook': 'ansible',
              'cert_deploy': 'ansible', 'jenkins': 'jenkins'}
# register 上报的能力清单：manager 据此对老 agent fail fast
CAPABILITIES = sorted(STEP_KINDS)

# payload 中只用于执行、不落 spool 的字段（证书内容等）
SECRET_PAYLOAD_FIELDS = ('files',)
SECRET_PLACEHOLDER = '***（敏感内容仅在内存中执行，未落盘）***'
# 证书分发单个文件与总量上限（字节）：防下发内容撑爆磁盘
CERT_FILE_MAX_BYTES = 256 * 1024
CERT_TOTAL_MAX_BYTES = 1024 * 1024
# 目标文件名白名单：纯文件名，禁止路径分隔与 ..
CERT_NAME_RE = re.compile(r'^[\w.@*-]+$')
MODE_RE = re.compile(r'^0[0-7]{3}$')


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


def redact_payload(payload: dict) -> dict:
    """落 spool 用的 payload 副本：敏感字段换占位串。

    spool 是崩溃恢复依据，恢复流程只会把未完成的 job 标 rc=137 上报，不会
    重放执行，因此这里丢掉证书内容不影响可靠性。
    """
    if not any(k in payload for k in SECRET_PAYLOAD_FIELDS):
        return payload
    redacted = dict(payload)
    for key in SECRET_PAYLOAD_FIELDS:
        if key in redacted:
            redacted[key] = SECRET_PLACEHOLDER
    return redacted


def build_cert_playbook(payload: dict, workdir: Path) -> str:
    """证书分发：把文件内容落 workdir 并生成剧本，返回剧本路径。

    与 manager 侧 cloud_mgr.deploy._build_playbook 同口径（目录 0750、私钥
    0600、覆盖前 backup、可选 reload）。文件名只收纯文件名，目标目录必须为
    绝对路径——payload 由 manager 生成，但 agent 不做「上游一定对」的假设。
    """
    deploy_path = (payload.get('deploy_path') or '').strip()
    if not deploy_path:
        raise PayloadError('deploy_path 为空')
    if not os.path.isabs(deploy_path) or '..' in deploy_path.split('/'):
        raise PayloadError(f'deploy_path 必须为绝对路径且不含 ..：{deploy_path}')
    files = payload.get('files')
    if not isinstance(files, list) or not files:
        raise PayloadError('files 为空（证书内容未下发）')

    tasks = [{
        'name': '创建证书目录',
        'ansible.builtin.file': {
            'path': deploy_path, 'state': 'directory', 'mode': '0750',
        },
    }]
    total = 0
    for index, item in enumerate(files):
        if not isinstance(item, dict):
            raise PayloadError('files 元素必须为映射（name/mode/content）')
        name = str(item.get('name') or '').strip()
        mode = str(item.get('mode') or '0644').strip()
        content = item.get('content')
        if not CERT_NAME_RE.match(name):
            raise PayloadError(f'非法目标文件名：{name!r}')
        if not MODE_RE.match(mode):
            raise PayloadError(f'非法文件权限：{mode!r}')
        if not isinstance(content, str) or not content:
            raise PayloadError(f'文件 {name} 内容为空')
        size = len(content.encode('utf-8'))
        total += size
        if size > CERT_FILE_MAX_BYTES or total > CERT_TOTAL_MAX_BYTES:
            raise PayloadError(f'下发文件超限（{name}，单文件上限 '
                               f'{CERT_FILE_MAX_BYTES} 字节，合计上限 '
                               f'{CERT_TOTAL_MAX_BYTES} 字节）')
        local_path = workdir / f'src{index}.pem'
        local_path.write_text(content, encoding='utf-8')
        local_path.chmod(0o600)
        tasks.append({
            'name': f'分发 {name}',
            'ansible.builtin.copy': {
                'src': str(local_path),
                'dest': os.path.join(deploy_path, name),
                'mode': mode,
                'backup': True,  # 覆盖前留旧副本，便于回滚
            },
        })
    reload_cmd = (payload.get('reload_cmd') or '').strip()
    if reload_cmd:
        tasks.append({'name': '重载服务', 'ansible.builtin.shell': reload_cmd})

    playbook_path = workdir / 'deploy_cert.yml'
    # JSON 即合法 YAML，规避引号转义
    playbook_path.write_text(
        json.dumps([{'hosts': 'all', 'gather_facts': False, 'tasks': tasks}],
                   ensure_ascii=False),
        encoding='utf-8')
    return str(playbook_path)


def build_argv(payload: dict, inventory_path: str, config, workdir: Path = None) -> tuple:
    """构造 ansible 命令行，返回 (argv, cwd)。

    workdir 为 cert_deploy 用的私有临时目录（证书内容与生成的剧本落在此处）。
    """
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

    if step_type == 'cert_deploy':
        if workdir is None:
            raise PayloadError('cert_deploy 缺少临时工作目录')
        playbook_path = build_cert_playbook(payload, workdir)
        # 单主机分发，forks 固定 1；不注入任何 extra_vars（内容全在剧本里）
        return ['ansible-playbook', playbook_path, '-i', inventory_path,
                '--forks', '1'], None

    raise PayloadError(f'不支持的 step_type：{step_type}')


class JobExecution:
    """一次任务单元的执行句柄：run() 产出日志行，cancel() 终止进程组。"""

    kind = 'ansible'  # 并发槽位归属（见 connection._reject_reason）

    def __init__(self, job_id: str, payload: dict, config):
        self.job_id = job_id
        self.payload = payload
        self.config = config
        self.artifacts = Path(config.artifacts_dir) / job_id
        self._proc = None
        self._cancelled = False
        self._secure_dir = None  # cert_deploy 的私有临时目录，执行完即删

    async def run(self, on_lines):
        """执行任务，日志行经 on_lines(list[str]) 回调（含截断/系统提示行）。

        返回 (rc, elapsed)。rc 语义：ansible 原生 rc；124 超时；143 取消；
        126 payload 或环境错误。
        """
        try:
            return await self._run(started=time.monotonic(), on_lines=on_lines)
        finally:
            self._wipe_secure_dir()

    async def _run(self, started, on_lines):
        self.artifacts.mkdir(parents=True, exist_ok=True)
        timeout = min(int(self.payload.get('timeout') or 300), self.config.job_timeout_max)

        try:
            if self.payload.get('project_sync') and self.payload.get('step_type') == 'playbook':
                rc = await self._project_sync(on_lines)
                if rc != 0:
                    return await self._finish(126, started, on_lines)
            if self.payload.get('step_type') == 'cert_deploy':
                # 证书内容不进 artifacts（有留存期），inventory 一并放私有目录
                self._secure_dir = Path(tempfile.mkdtemp(prefix='ops-cert-'))
                inventory_path = self._secure_dir / 'inventory.ini'
            else:
                inventory_path = self.artifacts / 'inventory.ini'
            inventory_path.write_text(self.payload.get('inventory') or '', encoding='utf-8')
            argv, cwd = build_argv(self.payload, str(inventory_path), self.config,
                                   workdir=self._secure_dir)
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

    def _wipe_secure_dir(self):
        """删除 cert_deploy 的私有临时目录（内含证书明文）。"""
        if self._secure_dir is None:
            return
        shutil.rmtree(self._secure_dir, ignore_errors=True)
        if self._secure_dir.exists():
            logger.error('job %s 临时目录未能删除：%s', self.job_id, self._secure_dir)
        self._secure_dir = None

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


def job_kind(payload: dict):
    """payload 的槽位归属；None = 本 agent 不支持该 step_type。"""
    return STEP_KINDS.get(payload.get('step_type'))


def make_execution(job_id: str, payload: dict, config):
    """按 step_type 选执行实现，未知类型抛 PayloadError（下发方拒收）。"""
    step_type = payload.get('step_type')
    if step_type in ('command', 'playbook', 'cert_deploy'):
        return JobExecution(job_id, payload, config)
    if step_type == 'jenkins':
        from .jenkins import JenkinsExecution  # 延迟导入：未用 jenkins 时不依赖 aiohttp

        return JenkinsExecution(job_id, payload, config)
    raise PayloadError(f'不支持的 step_type：{step_type}')
