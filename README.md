# ops-agent

NAT 出口执行代理：与 ops-manager 经 WebSocket 长连接通信，接收命令/剧本
执行单元，本地调用 ansible 对内网主机执行，流式回传日志。

设计文档：manager 仓库 `docs/ops-agent-design.md`（架构、消息协议、状态机）。
manager 侧模块：`apps/agent_mgr`（`docs/backend/modules/agent.md`）。

## 特性

- **只出不进**：agent 主动外连 manager，不监听任何端口；断线指数退避重连。
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

## 目录

```
ops_agent/
├── main.py        # 入口：配置加载、日志初始化、信号处理
├── config.py      # config.yaml 解析与校验
├── connection.py  # AgentApp：连接/重连、心跳、消息路由、任务调度、留存清理
├── executor.py    # ansible 子进程：流式输出、进程组超时强杀、路径防穿越
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
| 0/非零 | ansible 原生返回码 |
| 124 | 任务超时（agent 强杀） |
| 126 | payload/环境错误（剧本不存在、同步失败等） |
| 137 | agent 重启导致任务中断 |
| 143 | manager 取消 |
