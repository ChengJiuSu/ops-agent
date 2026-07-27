"""可靠回传缓存（spool）：日志分片与终态结果先落盘、ack 后清理。

目录结构（<data_dir>/spool/<job_id>/）：
    payload.json   下发内容（崩溃恢复依据）
    chunks.jsonl   日志分片，每行 {"chunk_seq": n, "lines": [...]}
    ack            已被 manager 确认的最大 chunk_seq
    result.json    终态汇总（发送前落盘）
    result.ack     manager 确认标记（存在即可清理整个目录）

约束：分片 seq 从 1 起严格递增；manager 侧按 seq 幂等去重，agent 断线
重连后从 ack 游标补传。所有写操作在 asyncio 事件循环线程内完成（文件小、
本地盘，阻塞可忽略；执行输出的高频写走内存缓冲按分片批量落盘）。
"""
import json
import shutil
from pathlib import Path


class JobSpool:
    def __init__(self, root: Path, job_id: str):
        self.dir = Path(root) / job_id
        self.job_id = job_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self._next_seq = self._load_last_seq() + 1

    # ── 写入 ──────────────────────────────────────────────

    def save_payload(self, payload: dict):
        (self.dir / 'payload.json').write_text(
            json.dumps(payload, ensure_ascii=False), encoding='utf-8')

    def append_chunk(self, lines: list) -> int:
        """追加一个日志分片，返回分配的 chunk_seq。"""
        seq = self._next_seq
        self._next_seq += 1
        with (self.dir / 'chunks.jsonl').open('a', encoding='utf-8') as f:
            f.write(json.dumps({'chunk_seq': seq, 'lines': lines}, ensure_ascii=False) + '\n')
        return seq

    def mark_acked(self, seq: int):
        current = self.acked_seq()
        if seq > current:
            (self.dir / 'ack').write_text(str(seq), encoding='utf-8')

    def save_result(self, result: dict):
        (self.dir / 'result.json').write_text(
            json.dumps(result, ensure_ascii=False), encoding='utf-8')

    def mark_result_acked(self):
        (self.dir / 'result.ack').write_text('1', encoding='utf-8')

    # ── 读取 ──────────────────────────────────────────────

    def payload(self) -> dict | None:
        path = self.dir / 'payload.json'
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else None

    def acked_seq(self) -> int:
        path = self.dir / 'ack'
        try:
            return int(path.read_text(encoding='utf-8').strip())
        except (FileNotFoundError, ValueError):
            return 0

    def unacked_chunks(self) -> list:
        """返回未确认分片列表（按 seq 升序），断线补传用。"""
        path = self.dir / 'chunks.jsonl'
        if not path.exists():
            return []
        acked = self.acked_seq()
        chunks = []
        with path.open(encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                chunk = json.loads(line)
                if chunk['chunk_seq'] > acked:
                    chunks.append(chunk)
        return chunks

    def result(self) -> dict | None:
        path = self.dir / 'result.json'
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else None

    def result_acked(self) -> bool:
        return (self.dir / 'result.ack').exists()

    def is_finished(self) -> bool:
        """终态且 manager 已确认：可清理。"""
        return self.result_acked()

    # ── 清理 ──────────────────────────────────────────────

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _load_last_seq(self) -> int:
        """崩溃恢复：从 chunks.jsonl 尾部恢复分片序号。"""
        path = self.dir / 'chunks.jsonl'
        if not path.exists():
            return 0
        last = 0
        with path.open(encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    last = json.loads(line)['chunk_seq']
        return last


def pending_jobs(root: Path) -> list:
    """启动扫描：返回 spool 下全部未清理的 JobSpool（含待补传与需标失败的）。"""
    root = Path(root)
    if not root.is_dir():
        return []
    return [JobSpool(root, p.name) for p in sorted(root.iterdir()) if p.is_dir()]
