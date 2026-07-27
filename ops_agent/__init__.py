"""ops-agent：NAT 出口执行代理。

与 ops-manager 经 WebSocket 长连接通信，接收命令/剧本执行单元，
本地调用 ansible 执行并流式回传日志。协议见 manager 仓库
docs/ops-agent-design.md。
"""
__version__ = '1.0.0'
