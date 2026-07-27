"""spool 可靠回传缓存测试：分片序号、ack 游标、崩溃恢复。"""
import tempfile
import unittest
from pathlib import Path

from ops_agent.spool import JobSpool, pending_jobs


class SpoolTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_chunk_seq_increments_and_ack_cursor(self):
        spool = JobSpool(self.root, 'job-1')
        self.assertEqual(spool.append_chunk(['a']), 1)
        self.assertEqual(spool.append_chunk(['b', 'c']), 2)
        self.assertEqual(spool.acked_seq(), 0)
        self.assertEqual(len(spool.unacked_chunks()), 2)
        spool.mark_acked(1)
        chunks = spool.unacked_chunks()
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]['chunk_seq'], 2)
        self.assertEqual(chunks[0]['lines'], ['b', 'c'])
        # ack 游标只前进不后退
        spool.mark_acked(1)
        self.assertEqual(spool.acked_seq(), 1)

    def test_seq_recovered_after_restart(self):
        spool = JobSpool(self.root, 'job-2')
        spool.append_chunk(['x'])
        spool.append_chunk(['y'])
        reopened = JobSpool(self.root, 'job-2')  # 模拟进程重启
        self.assertEqual(reopened.append_chunk(['z']), 3)

    def test_result_lifecycle(self):
        spool = JobSpool(self.root, 'job-3')
        self.assertIsNone(spool.result())
        spool.save_result({'rc': 0, 'elapsed': 1.5, 'host_results': {}})
        self.assertEqual(spool.result()['rc'], 0)
        self.assertFalse(spool.result_acked())
        spool.mark_result_acked()
        self.assertTrue(spool.result_acked())
        spool.cleanup()
        self.assertFalse(spool.dir.exists())

    def test_payload_roundtrip_and_pending_scan(self):
        spool = JobSpool(self.root, 'job-4')
        spool.save_payload({'step_type': 'command', 'command': 'echo 中文'})
        found = pending_jobs(self.root)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].payload()['command'], 'echo 中文')

    def test_pending_jobs_empty_root(self):
        self.assertEqual(pending_jobs(self.root / 'nonexistent'), [])


if __name__ == '__main__':
    unittest.main()
