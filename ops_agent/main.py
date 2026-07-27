"""入口：加载配置、初始化日志、启动 AgentApp（python -m ops_agent.main）。"""
import argparse
import asyncio
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

from .config import DEFAULT_CONFIG_PATH, load_config
from .connection import AgentApp


def setup_logging(config):
    """agent 自身日志：控制台 + 轮转文件（50MB × 5）。"""
    Path(config.log_dir).mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = logging.handlers.RotatingFileHandler(
        Path(config.log_dir) / 'agent.log',
        maxBytes=50 * 1024 * 1024, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(console)


async def _run(config):
    app = AgentApp(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(app.stop()))
    await app.run()


def main():
    parser = argparse.ArgumentParser(description='ops-agent：NAT 出口执行代理')
    parser.add_argument('-c', '--config', default=DEFAULT_CONFIG_PATH,
                        help=f'配置文件路径（默认 {DEFAULT_CONFIG_PATH}）')
    args = parser.parse_args()
    try:
        config = load_config(args.config)
    except (OSError, ValueError) as exc:
        print(f'配置加载失败：{exc}', file=sys.stderr)
        sys.exit(2)
    config.spool_dir.mkdir(parents=True, exist_ok=True)
    config.artifacts_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(config)
    logging.getLogger('ops_agent').info('ops-agent 启动，manager=%s', config.manager_url)
    asyncio.run(_run(config))


if __name__ == '__main__':
    main()
