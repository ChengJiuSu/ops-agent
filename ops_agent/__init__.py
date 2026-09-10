"""ops-agent：NAT 出口执行代理。

与 ops-manager 经 WebSocket 长连接通信，接收命令/剧本执行单元，
本地调用 ansible 执行并流式回传日志；亦可代 manager 触发内网 Jenkins 构建
（step_type='jenkins'）、代下发证书到内网主机（step_type='cert_deploy'）、
代注册主机到内网 JumpServer（step_type='jumpserver'）。
协议见 manager 仓库 docs/ops-agent-design.md。
"""
__version__ = '1.3.0'
