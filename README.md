# ops-agent

NAT 出口执行代理：与 ops-manager 经 WebSocket 长连接通信，接收命令/剧本
执行单元，本地调用 ansible 对内网主机执行，流式回传日志；亦可代 manager
触发内网 Jenkins 构建（manager 在公网、Jenkins 在内网的场景）。

设计文档：manager 仓库 `docs/ops-agent-design.md`（架构、消息协议、状态机）。
manager 侧模块：`apps/agent_mgr`（`docs/backend/modules/agent.md`）。

## 特性

- **只出不进**：agent 主动外连 manager，不监听任何端口；断线指数退避重连。
- **四类执行单元**：`command` / `playbook` / `cert_deploy`（ansible 子进程）
  + `jenkins`（REST 触发内网 job，控制台日志实时回传）。ansible 与 Jenkins
  各占一批并发槽位——后者是纯 IO 等待，不该抢 ansible 的额度。
- **日志不丢**：分片先落本地 spool，manager ack 后清理；断线/重启后按
  ack 游标补传；终态结果同样 ack 确认。
- **双层留存**：agent 自身日志（`log_dir`，50MB × 5 轮转）+ 每任务
  ansible 完整输出（`data_dir/artifacts/<job_id>/stdout.log`，按天数清理，
  回传截断不影响本地全量）。
- **资源受控**：systemd MemoryMax/CPUQuota 硬限 + 并发/排队上限、forks
  封顶、超时进程组强杀、回传行数上限、磁盘水位拒收。
- **崩溃恢复**：重启扫描 spool，中断任务标记 rc=137 上报，未确认数据补传。

## 部署

前置：出口主机可达 manager（wss）、到内网主机 SSH 免密、已装 ansible。

```bash
# 1. 安装
sudo mkdir -p /opt/ops-agent && cd /opt/ops-agent
git clone <this-repo> . && python3 -m venv venv
./venv/bin/pip install -r requirements.txt ansible-core

# 2. 配置（token 在 manager「Agent 管理」创建 agent 时获得，仅展示一次）
sudo mkdir -p /etc/ops-agent
sudo cp config.example.yaml /etc/ops-agent/config.yaml && sudo vim /etc/ops-agent/config.yaml

# 3. 运行用户与目录
sudo useradd -r -s /usr/sbin/nologin ops-agent
sudo mkdir -p /var/lib/ops-agent /var/log/ops-agent
sudo chown -R ops-agent:ops-agent /var/lib/ops-agent /var/log/ops-agent

# 4. 剧本仓库（与 manager ANSIBLE_PROJECT_DIR 同源）
sudo git clone <ansible-project-repo> /opt/ansible-project
sudo chown -R ops-agent:ops-agent /opt/ansible-project

# 5. systemd 托管
sudo cp deploy/ops-agent.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now ops-agent
journalctl -u ops-agent -f   # 观察连接与心跳
```

manager 侧：给 NAT 内主机在「服务器管理」把执行通道切到该 agent，任务流
执行时命令/剧本模块自动经 agent 下发。

### Jenkins 代触发（可选）

manager 在公网而 Jenkins 在内网时，在 `config.yaml` 加 `jenkins:` 段：

```yaml
jenkins:
  url: http://jenkins.intra:8080
  user: ci-bot
  api_token: "<Jenkins 用户 API Token>"
  abort_on_cancel: true   # manager 取消/等待超时时，连带中止 Jenkins 上的构建
```

凭据只驻留本机、不经通道下发（与 SSH 私钥同口径）；manager 侧只需在
「系统配置」把 `jenkins.agent` 指向本 agent 名（或在方法/模块参数里指定），
jenkins 方法模块即改走本 agent，构建日志实时回传任务日志。未配置该段时
收到 jenkins 单元会以 rc=126 失败并回传提示。

环境里的 `http_proxy/https_proxy` 一律忽略：内网 Jenkins 走代理只会拿 502。

### 证书分发（`cert_deploy`，无需配置）

manager 的云控证书分发到 agent 通道主机时下发 `cert_deploy` 单元：payload 带
目标目录、重载命令与文件清单（`name` / `mode` / `content`），**剧本由 agent
本地生成**（建目录 0750 → copy 各文件、覆盖前 backup → 可选 reload），不接受
下发的剧本内容。

证书明文的落盘面：只写 `tempfile.mkdtemp()`（0700）内的 0600 文件，inventory
与生成的剧本一并放在那里，**不进 artifacts**（有留存期），执行结束（含异常/
超时/取消）连目录一起删；spool 的 `payload.json` 里 `files` 已换成占位串
（崩溃恢复只标 rc=137 上报，不重放执行）。校验不过（目标目录非绝对路径、
文件名带路径、mode 非法、单文件 > 256 KiB 或合计 > 1 MiB）→ rc=126。

## 目录

```
ops_agent/
├── main.py        # 入口：配置加载、日志初始化、信号处理
├── config.py      # config.yaml 解析与校验
├── connection.py  # AgentApp：连接/重连、心跳、消息路由、任务调度、留存清理
├── executor.py    # ansible 子进程：流式输出、进程组超时强杀、路径防穿越；按 step_type 选实现；cert_deploy 剧本本地生成 + 私有临时目录
├── jenkins.py     # Jenkins 构建：触发/队列/控制台增量/结果，取消时中止构建
└── spool.py       # 可靠回传缓存：分片 + ack 游标 + 崩溃恢复

/var/lib/ops-agent/
├── spool/<job_id>/      # 未确认的分片与终态（ack 后删除）
└── artifacts/<job_id>/  # inventory.ini / stdout.log / rc（留存 30 天）
```

## 测试

```bash
./venv/bin/python -m unittest discover -s tests
```

## rc 约定

| rc | 含义 |
|----|------|
| 0/非零 | ansible 原生返回码；jenkins 单元 0=SUCCESS（或 UNSTABLE 且 allow_unstable），1=其余构建结果/请求失败 |
| 124 | 任务超时（ansible 进程组强杀；jenkins 等待构建超时） |
| 126 | payload/环境错误（剧本不存在、同步失败、未配 Jenkins 连接信息、cert_deploy 文件清单非法、agent 不支持的 step_type 等） |
| 137 | agent 重启导致任务中断（jenkins 单元：Jenkins 上的构建不随 agent 消亡，仍会跑完） |
| 143 | manager 取消 |
