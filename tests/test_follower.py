import json
import tempfile
import unittest
from pathlib import Path

from follower import (
    KIND_CORRUPT, KIND_GAP, KIND_LIFECYCLE, KIND_RECORD, Event,
    event_key, Follower,
)
from source import append, compress_archive, inventory, rotate, seed, truncate

FIXTURE = Path(__file__).parents[1] / "fixtures/source.json"


def kinds(events):
    return [e.kind for e in events]


def records(events):
    return [e.record for e in events if e.kind == KIND_RECORD]


class FollowerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "src"
        self.state = Path(self.tmp.name) / "state"
        seed(self.root, self.fixture)

    def follower(self, **kw):
        return Follower(self.root, self.state, **kw)

    def test_half_line_and_ack_coordination(self):
        fw = self.follower()
        events = fw.poll()
        # 初始只有一条闭合记录；末行无换行，不代表记录结束
        self.assertEqual(records(events), [{"device": "A", "value": 18}])
        snap = fw.snapshot()
        self.assertEqual(snap["streams"][fw.current]["offset"], 0)
        self.assertEqual(snap["streams"][fw.current]["pending_bytes"],
                         len(self.fixture["files"]["gateway.log"].encode()))
        fw.ack()  # 只确认到行边界
        first_len = self.fixture["files"]["gateway.log"].index("\n") + 1
        snap = fw.snapshot()["streams"][fw.current]
        self.assertEqual(snap["offset"], first_len)
        self.assertGreater(snap["pending_bytes"], 0)

        # 补齐半行后，范围从半行的原始起点开始，不产生替代字符
        append(self.root, self.fixture["append"])
        events = fw.poll()
        ev = next(e for e in events if e.kind == KIND_RECORD)
        self.assertEqual(ev.record, {"device": "B", "value": "在线", "seq": 2})
        total = len((self.root / "gateway.log").read_bytes())
        self.assertEqual((ev.start, ev.end), (first_len, total))

    def test_multibyte_split_across_restart(self):
        # 写入被从多字节 UTF-8 字符中间切开的半行
        line = '{"v":"中"}\n'.encode("utf-8")
        cut = line.index("中".encode("utf-8")) + 1  # 切在“中”的第二个字节后
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(line[:cut])
        fw = self.follower()
        self.assertEqual(records(fw.poll()), [])  # 半行不产生替代字符
        # 用新实例模拟进程重启
        fw2 = Follower(self.root, self.state)
        with (self.root / "gateway.log").open("ab") as fh:
            fh.write(line[cut:])
        events = fw2.poll()
        self.assertEqual(records(events), [{"v": "中"}])
        self.assertNotIn("�", json.dumps(records(events), ensure_ascii=False))

    def test_buffered_bytes_replayed_after_restart(self):
        fw = self.follower()
        fw.poll()  # 首条已下发但未 ack
        fw2 = Follower(self.root, self.state)  # 重启：未确认字节必须重放
        events = fw2.poll()
        self.assertEqual(records(events), [{"device": "A", "value": 18}])
        fw2.ack()
        fw3 = Follower(self.root, self.state)
        self.assertEqual(records(fw3.poll()), [])  # ack 后不再重复

    def test_rotation_tail_then_new_file(self):
        fw = self.follower()
        fw.poll()
        fw.ack()
        old_identity = tuple(inventory(self.root)[0]["identity"])
        # 网关在轮转前把半行补全（末行获得换行符），随后才 rename + 新建
        append(self.root, self.fixture["append"])
        rotate(self.root, self.fixture["new_file"])
        entries = {e["path"]: e for e in inventory(self.root)}
        self.assertEqual(tuple(entries["gateway.log.1"]["identity"]),
                         old_identity)

        events = fw.poll()  # 旧文件未读尾部 + 新文件接入
        tail = next(e for e in events if e.kind == KIND_RECORD
                    and tuple(e.origin_identity) == old_identity)
        self.assertEqual(tail.record, {"device": "B", "value": "在线",
                                       "seq": 2})
        new_ev = next(e for e in events if e.kind == KIND_RECORD
                      and tuple(e.origin_identity) != old_identity)
        self.assertEqual(new_ev.record, {"device": "A", "value": 19})
        self.assertIn("stream_started",
                      [e.reason for e in events if e.kind == KIND_LIFECYCLE])
        # 新流从字节 0 开始，两条记录身份不同，范围相同也不算重复
        first_len = self.fixture["files"]["gateway.log"].index("\n") + 1
        self.assertEqual((tail.start, new_ev.start), (first_len, 0))
        self.assertNotEqual(event_key(tail), event_key(new_ev))

    def test_in_place_truncate_same_text_not_duplicate(self):
        # 截短场景使用全部闭合的内容，避免与“未闭合尾行”语义混淆
        closed = self.fixture["new_file"]  # 单行 + 换行
        (self.root / "gateway.log").write_text(closed, encoding="utf-8")
        fw = self.follower()
        first = [e for e in fw.poll() if e.kind == KIND_RECORD][0]
        fw.ack()
        truncate(self.root, closed)
        events = fw.poll()
        second = next(e for e in events if e.kind == KIND_RECORD)
        self.assertEqual(second.record, first.record)
        self.assertEqual((second.start, second.end),
                         (first.start, first.end))  # 范围相同
        self.assertEqual(second.incarnation, first.incarnation + 1)
        self.assertNotEqual(event_key(first), event_key(second))
        self.assertEqual(
            next(e for e in events if e.kind == KIND_LIFECYCLE).reason,
            "truncated")

    def test_gzip_archive_continuity_and_verification(self):
        fw = self.follower()
        fw.poll()
        fw.ack()
        append(self.root, self.fixture["append"])  # 补全半行后再轮转
        rotate(self.root, self.fixture["new_file"])
        fw.poll()          # 追尾旧明文并接入新文件
        fw.ack()
        compress_archive(self.root)
        events = fw.poll()  # 归档出现：验证前缀摘要后接续
        self.assertFalse(any(e.kind == KIND_GAP for e in events))
        gz_events = [e for e in events if e.compressed]
        self.assertTrue(gz_events)
        self.assertEqual(gz_events[0].record,
                         {"device": "B", "value": "在线", "seq": 2})
        snap = fw.snapshot()["streams"]
        self.assertTrue(all(s["status"] == "archived"
                            for s in snap.values() if s["path"].endswith(".gz")))

    def test_corrupt_archive_reports_gap_and_stops(self):
        fw = self.follower()
        fw.poll()
        fw.ack()
        append(self.root, self.fixture["append"])
        rotate(self.root, self.fixture["new_file"])
        fw.poll()
        fw.ack()
        compress_archive(self.root)
        bad = self.root / "gateway.log.1.gz"
        good = bad.read_bytes()
        bad.write_bytes(good[:-8] + b"\x00" * 8)  # 破坏尾部 CRC/长度
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "archive_corrupt")
        self.assertTrue(gap.detail["recovered_bytes"] >= 0)
        # 再次轮询不重复报缺口
        self.assertEqual(fw.poll(), [])

    def test_identity_lost_stops_without_guessing(self):
        fw = self.follower()
        fw.poll()
        fw.ack()
        # 旧文件被删且没有任何归档证据
        (self.root / "gateway.log").unlink()
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "identity_lost")
        self.assertEqual(fw.snapshot()["streams"][fw.current]["status"],
                         "stopped")
        # 之后新建活动文件：下一次同步即作为独立新流接入，不声明连续
        (self.root / "gateway.log").write_text(
            self.fixture["new_file"], encoding="utf-8")
        events = fw.poll()
        self.assertTrue(any(e.kind == KIND_LIFECYCLE
                            and e.reason == "stream_started"
                            for e in events))
        self.assertIn("stream_started",
                      [e.reason for e in events if e.kind == KIND_LIFECYCLE])
        self.assertEqual(records(events), [{"device": "A", "value": 19}])

    def test_corrupt_lines_keep_bytes_and_continue(self):
        body = (b'{"ok":1}\n'
                b'\xff\xfe\n'
                b'{"bad"\n'
                b'{"ok":2}\n')
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(body)
        fw = self.follower()
        events = fw.poll()
        corrupt = [e for e in events if e.kind == KIND_CORRUPT]
        self.assertEqual(len(corrupt), 2)
        # 原始字节与位置保留，且两条损坏不挡住后续合法行
        self.assertEqual(bytes.fromhex(corrupt[0].raw_hex), b'\xff\xfe')
        self.assertEqual(corrupt[0].reason, "invalid_utf8")
        self.assertEqual(corrupt[1].reason, "invalid_json")
        self.assertEqual(records(events), [{"ok": 1}, {"ok": 2}])
        fw.ack()
        self.assertEqual(Follower(self.root, self.state).poll(), [])

    def test_oversize_line_limited_and_gap_reported(self):
        # 先放一条已确认的闭合行，再让超长行在未确认缓冲中堆积
        (self.root / "gateway.log").write_text(
            self.fixture["new_file"], encoding="utf-8")
        fw = self.follower(max_line_bytes=32)
        fw.poll()
        fw.ack()
        huge = b'{"x":"' + b"a" * 200 + b'"}\n{"tail":true}\n'
        with (self.root / "gateway.log").open("ab") as fh:
            fh.write(huge)
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "line_too_long")
        self.assertGreaterEqual(gap.end - gap.start, 200)
        self.assertIn("evidence_hex", gap.detail)
        # 超长行之后的合法行照常交付
        self.assertEqual(records(events), [{"tail": True}])

    def test_pause_resume_persists_state(self):
        fw = self.follower()
        fw.poll()
        fw.pause()
        self.assertTrue(fw.is_paused())
        self.assertEqual(fw.poll(), [])
        fw2 = Follower(self.root, self.state)  # 重启后仍处于暂停
        self.assertTrue(fw2.is_paused())
        fw2.resume()
        events = fw2.poll()  # 缓冲仍在，记录不丢
        self.assertEqual(records(events), [{"device": "A", "value": 18}])

    def test_pause_marker_file(self):
        fw = self.follower()
        fw.pause_path.write_text("manual", encoding="utf-8")
        self.assertTrue(fw.is_paused())
        self.assertEqual(fw.poll(), [])
        fw.pause_path.unlink()
        self.assertFalse(fw.is_paused())

    def test_event_identity_and_byte_ranges(self):
        fw = self.follower()
        events = [e for e in fw.poll() if e.kind == KIND_RECORD]
        ev = events[0]
        raw = (self.root / "gateway.log").read_bytes()
        self.assertEqual(raw[ev.start:ev.end],
                         b'{"device":"A","value":18}\n')
        self.assertEqual(tuple(ev.identity),
                         tuple(inventory(self.root)[0]["identity"]))
        self.assertIsInstance(ev.as_dict(), dict)
        self.assertIsInstance(Event(**{k: v for k, v in ev.as_dict().items()}),
                              Event)


    def test_archive_unverified_refuses_continuity(self):
        # 归档内容与已确认前缀不一致：不猜测，报缺口并停止该流
        fw = self.follower()
        fw.poll()
        fw.ack()
        append(self.root, self.fixture["append"])
        rotate(self.root, self.fixture["new_file"])
        fw.poll()
        fw.ack()
        import gzip as _gzip
        (self.root / "gateway.log.1").unlink()  # 明文删除，只剩归档
        wrong = self.root / "gateway.log.1.gz"
        wrong.write_bytes(_gzip.compress(b'{"device":"Z","value":0}\n',
                                         mtime=0))
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "archive_unverified")
        stopped = [s for s in fw.snapshot()["streams"].values()
                   if s["stop_reason"] == "archive_unverified"]
        self.assertEqual(len(stopped), 1)
        self.assertEqual(stopped[0]["status"], "stopped")

    def test_archive_oversize_reports_gap_with_limit(self):
        fw = self.follower(max_archive_bytes=40)
        fw.poll()  # 先建立旧流身份与已确认前缀
        fw.ack()
        append(self.root, self.fixture["append"])
        rotate(self.root, self.fixture["new_file"])
        fw.poll()
        fw.ack()
        compress_archive(self.root)  # 压缩同时删除明文
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "archive_oversize")
        self.assertEqual(gap.detail["limit_bytes"], 40)

    def test_restart_inside_compressed_archive(self):
        fw = self.follower()
        fw.poll()
        fw.ack()
        append(self.root, self.fixture["append"])
        rotate(self.root, self.fixture["new_file"])
        fw.poll()
        fw.ack()
        compress_archive(self.root)
        fw.poll()  # 归档落地，状态持久化
        fw2 = Follower(self.root, self.state)  # 重启
        self.assertEqual(fw2.poll(), [])       # 已归档流不重复下发
        snap = fw2.snapshot()
        self.assertTrue(all(s["status"] == "archived"
                            for s in snap["streams"].values()
                            if s["compressed"]))

    def test_oversize_open_line_drains_across_polls(self):
        # 未闭合的超长行：第一次轮询进入排空且内存不堆积，换行到达后报缺口
        fw = self.follower(max_line_bytes=32)
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(b'{"ok":1}\n' + b"a" * 100)  # 尚无换行
        events = fw.poll()
        self.assertEqual(records(events), [{"ok": 1}])
        s = fw.snapshot()["streams"][fw.current]
        self.assertIsNotNone(fw.streams[fw.current]["drain"])
        # 缓冲里只保留正常行，超长行不进 pending
        self.assertLessEqual(s["pending_bytes"], len(b'{"ok":1}\n'))
        with (self.root / "gateway.log").open("ab") as fh:
            fh.write(b"b" * 50 + b"\n{\"tail\":1}\n")
        events = fw.poll()
        gap = next(e for e in events if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "line_too_long")
        self.assertEqual(records(events), [{"tail": 1}])

    def test_oversize_tail_after_unacked_prefix(self):
        # 未确认缓冲里已有闭合行，随后出现未闭合超长行，两者不能互相稀释
        fw = self.follower(max_line_bytes=32)
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(b'{"ok":1}\n' + b"x" * 80)  # 尾行无换行且超长
        events = fw.poll()
        self.assertEqual(records(events), [{"ok": 1}])
        self.assertTrue(fw.streams[fw.current]["drain"] is not None
                        or any(e.kind == KIND_GAP for e in events))

    def test_oversize_gap_not_reemitted_on_later_polls(self):
        # 超长行缺口只报一次；后续轮询不得把超长行重新读入或重复报缺口
        fw = self.follower(max_line_bytes=32)
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(b'{"ok":1}\n' + b"a" * 100)
        fw.poll()
        fw.ack()
        with (self.root / "gateway.log").open("ab") as fh:
            fh.write(b"b" * 50 + b'\n{"tail":1}\n')
        second = fw.poll()
        self.assertEqual(len([e for e in second if e.kind == KIND_GAP]), 1)
        gap = next(e for e in second if e.kind == KIND_GAP)
        self.assertEqual(gap.reason, "line_too_long")
        # 洞之后的合法行范围紧跟 gap 终点，绝对位置正确
        tail = next(e for e in second if e.kind == KIND_RECORD)
        self.assertEqual(tail.record, {"tail": 1})
        self.assertEqual(tail.start, gap.end)
        fw.ack()
        third = fw.poll()
        # 无新增：既不重复缺口，也不重复记录
        self.assertFalse(any(e.kind == KIND_GAP for e in third))
        self.assertFalse(any(e.kind == KIND_RECORD for e in third))

    def test_oversize_after_hole_uses_correct_absolute_base(self):
        # 洞之后的新段里再出现超长行，绝对范围必须基于 pending_base
        fw = self.follower(max_line_bytes=32)
        # 第一条超长行先不闭合，走 drain + hole 路径
        with (self.root / "gateway.log").open("wb") as fh:
            fh.write(b'{"ok":1}\n' + b"a" * 100)
        fw.poll()
        fw.ack()
        # 补换行闭合第一个洞，并写入正常行与第二条闭合超长行
        with (self.root / "gateway.log").open("ab") as fh:
            fh.write(b"\n" + b'{"mid":2}\n' + b"c" * 120 + b"\n")
        events = fw.poll()
        gaps = [e for e in events if e.kind == KIND_GAP]
        # 第一个洞（a 行补换行）+ 第二个洞（c 行，解析器内闭合超长）
        self.assertEqual(len(gaps), 2)
        mid = next(e for e in events if e.kind == KIND_RECORD
                   and e.record == {"mid": 2})
        # mid 记录夹在两个洞之间，范围相邻且不重叠、顺序正确
        self.assertLessEqual(gaps[0].end, mid.start)
        self.assertLessEqual(mid.end, gaps[1].start)
        # 第二个洞的绝对起点 = mid 终点，证明 pending_base 锚定正确
        self.assertEqual(gaps[1].start, mid.end)
        fw.ack()
        self.assertEqual(fw.poll(), [])

    def test_event_key_distinguishes_same_text_after_truncate(self):
        fw = self.follower()
        (self.root / "gateway.log").write_text(
            self.fixture["new_file"], encoding="utf-8")
        first = next(e for e in fw.poll() if e.kind == KIND_RECORD)
        fw.ack()
        truncate(self.root, self.fixture["new_file"])
        second = next(e for e in fw.poll() if e.kind == KIND_RECORD)
        self.assertEqual(first.record, second.record)
        self.assertNotEqual(event_key(first), event_key(second))


if __name__ == "__main__":
    unittest.main()
