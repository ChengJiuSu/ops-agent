"""配置加载与校验（config.yaml）。"""
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = '/etc/ops-agent/config.yaml'


@dataclass
class JenkinsConfig:
    """Jenkins 连接信息：只在 agent 本地维护，不经通道下发（同 SSH 私钥口径）。

    未配置（url/user/api_token 任一为空）时仍可正常启动，只是收到 jenkins
    执行单元会以 rc=126 失败并回传提示。
    """

    url: str = ''                    # http(s)://jenkins.intra:8080
    user: str = ''
    api_token: str = ''
    verify_tls: bool = True          # 自签且不便配 CA 时置 false
    ca_file: str = ''                # 自签证书的 CA 路径（verify_tls=true 时生效）
    poll_interval: float = 2.0       # 队列/日志/结果轮询间隔（秒）
    abort_on_cancel: bool = True     # manager 取消或等待超时时中止 Jenkins 构建

    @property
    def configured(self) -> bool:
        return bool(self.url and self.user and self.api_token)

    @property
    def base_url(self) -> str:
        return self.url.rstrip('/')


@dataclass
class JumpServerConfig:
    """JumpServer 连接信息：只在 agent 本地维护，不经通道下发（同 Jenkins 口径）。

    未配置（url/token 任一为空）时仍可正常启动，只是收到 jumpserver
    执行单元会以 rc=126 失败并回传提示。
    """

    url: str = ''                    # http(s)://jumpserver.intra
    token: str = ''                  # 个人 API Token（Authorization: Token <token>）
    org_id: str = ''                 # 组织 id（多组织时指定，写入 X-JMS-ORG 头）
    platform_id: str = '1'           # 平台 id（默认 1 = Linux）
    node_id: str = ''                # 资产挂载节点 id
    verify_tls: bool = True          # 自签且不便配 CA 时置 false
    ca_file: str = ''                # 自签证书的 CA 路径（verify_tls=true 时生效）
    request_timeout: int = 15        # 单次 API 请求超时（秒）

    @property
    def configured(self) -> bool:
        return bool(self.url and self.token)

    @property
    def base_url(self) -> str:
        return self.url.rstrip('/')


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
    max_concurrent_jobs: int = 2     # ansible 并发执行上限，超出排队
    max_queued_jobs: int = 10        # 排队上限，再满即拒收（ansible 与 jenkins 各自计）
    # jenkins 单元是纯 IO 等待（轮询 Jenkins API），不该与 ansible 抢同一批槽位
    max_concurrent_jenkins_jobs: int = 4
    # jumpserver 注册同为纯 IO（逐台 REST），单独一批槽位：拒收文案不会张冠李戴
    max_concurrent_jumpserver_jobs: int = 2
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

    # ── Jenkins（step_type='jenkins' 执行单元）──
    jenkins: JenkinsConfig = field(default_factory=JenkinsConfig)

    # ── JumpServer（step_type='jumpserver' 执行单元）──
    jumpserver: JumpServerConfig = field(default_factory=JumpServerConfig)

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


def _load_section(raw, key, cls, hint):
    """加载 config.yaml 的一个子段（jenkins / jumpserver）为 dataclass。"""
    if raw is None:
        return cls()
    if not isinstance(raw, dict):
        raise ValueError(f'配置项 {key} 必须为映射（{hint}）')
    unknown = set(raw) - set(cls.__dataclass_fields__)
    if unknown:
        raise ValueError(f'未知配置项：{key}.{f", {key}.".join(sorted(unknown))}')
    return cls(**raw)


def _load_jenkins(raw) -> JenkinsConfig:
    return _load_section(raw, 'jenkins', JenkinsConfig, 'url / user / api_token …')


def _load_jumpserver(raw) -> JumpServerConfig:
    return _load_section(raw, 'jumpserver', JumpServerConfig, 'url / token / org_id …')


def load_config(path: str) -> Config:
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8')) or {}
    known = {f for f in Config.__dataclass_fields__ if not f.startswith('_')}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f'未知配置项：{", ".join(sorted(unknown))}')
    raw = dict(raw)
    raw['jenkins'] = _load_jenkins(raw.get('jenkins'))
    raw['jumpserver'] = _load_jumpserver(raw.get('jumpserver'))
    cfg = Config(**raw, _path=path)
    cfg.validate()
    return cfg
