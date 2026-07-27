"""配置加载与校验（config.yaml）。"""
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = '/etc/ops-agent/config.yaml'


@dataclass
class Config:
    # ── 连接 ──
    manager_url: str = ''            # wss://<manager>/ws/agent/
    token: str = ''
    agent_name: str = ''
    heartbeat_interval: int = 30     # 秒
    reconnect_max_delay: int = 60    # 重连退避封顶（秒）
    tls_ca_file: str = ''            # 自签证书时指定 CA；留空走系统信任链

    # ── 执行 ──
    max_concurrent_jobs: int = 2     # 并发执行上限，超出排队
    max_queued_jobs: int = 10        # 排队上限，再满即拒收
    max_forks: int = 10              # ansible forks 封顶（与 payload 取小）
    job_timeout_max: int = 3600      # 单任务超时封顶（秒）
    ansible_project_dir: str = '/opt/ansible-project'
    project_sync_cmd: str = 'git pull --ff-only'
    project_sync_timeout: int = 120  # 剧本同步超时（秒）

    # ── 留存与资源 ──
    data_dir: str = '/var/lib/ops-agent'      # spool/ 与 artifacts/ 根目录
    log_dir: str = '/var/log/ops-agent'
    log_retention_days: int = 30               # artifacts 留存天数
    disk_min_free_pct: int = 15                # 磁盘低水位：拒收新任务
    max_job_log_lines: int = 100000            # 单任务回传行数上限，超限截断
    log_chunk_lines: int = 50                  # 日志分片行数
    log_flush_interval: float = 1.0            # 分片冲刷间隔（秒）

    _path: str = field(default='', repr=False)

    @property
    def spool_dir(self) -> Path:
        return Path(self.data_dir) / 'spool'

    @property
    def artifacts_dir(self) -> Path:
        return Path(self.data_dir) / 'artifacts'

    def validate(self):
        missing = [k for k in ('manager_url', 'token', 'agent_name') if not getattr(self, k)]
        if missing:
            raise ValueError(f'配置缺少必填项：{", ".join(missing)}（{self._path or "config.yaml"}）')
        if not self.manager_url.startswith(('ws://', 'wss://')):
            raise ValueError('manager_url 必须为 ws:// 或 wss:// 地址')


def load_config(path: str) -> Config:
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8')) or {}
    known = {f for f in Config.__dataclass_fields__ if not f.startswith('_')}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f'未知配置项：{", ".join(sorted(unknown))}')
    cfg = Config(**raw, _path=path)
    cfg.validate()
    return cfg
