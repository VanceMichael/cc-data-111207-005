"""字节级持久化跟读器。

设计约定（与 docs/数据约定.md 对应）：

* 文件身份是 ``(st_dev, st_ino)``，路径只作展示；同路径 rename 后新建、
  原地截短都靠身份区分，绝不按行文本判重。
* 所有偏移量均为未压缩原始字节的绝对偏移；gzip 归档的偏移定义在解压后的
  字节流上，并通过已确认前缀摘要验证关联，无法验证即停止而非猜测。
* 检查点先落盘再下发事件；业务侧采纳后调用 :meth:`Follower.ack` 才推进
  已确认偏移。未确认字节（含半行、半截多字节字符）原样持久化，重启后
  以 at-least-once 方式重放，事件身份与字节范围保持不变。
* 损坏行（非法 UTF-8 / 非法 JSON）保留原始字节与位置，单独成事件，不
  阻挡后续合法行；超长行与损坏归档有资源上限并显式报告缺口。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path

from source import inventory

STATE_NAME = "checkpoint.json"
PAUSE_NAME = "paused"
DEFAULT_CHUNK = 64 * 1024
DEFAULT_MAX_LINE = 1 << 20          # 单行 1 MiB 上限
DEFAULT_MAX_ARCHIVE = 256 << 20     # 解压后 256 MiB 上限
EVIDENCE_BYTES = 256                # 超长行保留的前缀证据长度

# 事件类型：record 业务记录 / corrupt 损坏行 / gap 缺口 / lifecycle 生命周期
KIND_RECORD = "record"
KIND_CORRUPT = "corrupt"
KIND_GAP = "gap"
KIND_LIFECYCLE = "lifecycle"


@dataclass
class Event:
    """一条下发事件。

    ``identity`` 是实际读出字节的物理文件身份；``origin_identity`` 是逻辑
    日志流的起始身份（gzip 接续后两者不同）。``start``/``end`` 为未压缩
    字节流上的半开区间 ``[start, end)``，区间包含行尾换行符。
    """

    kind: str
    path: str
    identity: list
    start: int
    end: int
    incarnation: int
    compressed: bool = False
    origin_identity: list | None = None
    record: object = None
    raw_hex: str | None = None
    reason: str | None = None
    detail: dict | None = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "kind": self.kind,
            "path": self.path,
            "identity": list(self.identity),
            "origin_identity": list(self.origin_identity or self.identity),
            "compressed": self.compressed,
            "incarnation": self.incarnation,
            "start": self.start,
            "end": self.end,
            "record": self.record,
            "raw_hex": self.raw_hex,
            "reason": self.reason,
            "detail": self.detail,
        }


def event_key(event: Event) -> tuple:
    """下游去重应使用的稳定键：流身份 + 截短代数 + 字节范围。

    截短后同一 inode、同一范围会出现相同文本，但 incarnation 不同，因此
    不能只凭行文本判重。
    """
    return (tuple(event.origin_identity or event.identity),
            event.incarnation, event.start, event.end)


def _digest(data: bytes) -> str:
    return hashlib.blake2b(data).hexdigest()


class Follower:
    """持久化跟读器；一个实例对应一个共享目录与一个状态目录。"""

    def __init__(self, root, state_dir, active_name: str = "gateway.log",
                 max_line_bytes: int = DEFAULT_MAX_LINE,
                 max_archive_bytes: int = DEFAULT_MAX_ARCHIVE):
        self.root = Path(root)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.state_dir / STATE_NAME
        self.pause_path = self.state_dir / PAUSE_NAME
        self.active_name = active_name
        self.max_line_bytes = max_line_bytes
        self.max_archive_bytes = max_archive_bytes
        self.active_name_gz = active_name + ".gz"
        self.streams: dict[str, dict] = {}
        self.current: str | None = None
        self.paused = False
        self._hashers: dict[str, "hashlib._Hash"] = {}
        if self.state_path.exists():
            self._load()

    # ------------------------------------------------------------------ 持久化
    def _load(self) -> None:
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.active_name = payload["active_name"]
        self.active_name_gz = self.active_name + ".gz"
        self.current = payload["current"]
        self.paused = payload.get("paused", False)
        self.max_line_bytes = payload.get("max_line_bytes", self.max_line_bytes)
        self.max_archive_bytes = payload.get("max_archive_bytes",
                                             self.max_archive_bytes)
        self.streams = {}
        for key, raw in payload["streams"].items():
            self.streams[key] = {
                "identity": tuple(raw["identity"]),
                "physical_identity": tuple(raw["physical_identity"]),
                "path": raw["path"],
                "compressed": raw["compressed"],
                "incarnation": raw["incarnation"],
                "offset": raw["offset"],
                "pending": bytearray(bytes.fromhex(raw["pending_hex"])),
                "parsed": raw["parsed"],
                "digest_hex": raw["digest_hex"],
                "status": raw["status"],
                "stop_reason": raw.get("stop_reason"),
                "drain": raw.get("drain"),
                "last_mtime_ns": raw.get("last_mtime_ns"),
                "holes": [tuple(h) for h in raw.get("holes", [])],
                "pending_base": raw.get("pending_base", raw["offset"]),
            }
            # at-least-once：重启后未确认的缓冲字节从头重新解析下发，
            # 事件身份与字节范围不变；offset 本身保持在已确认位置。
            self.streams[key]["parsed"] = 0

    def _persist(self) -> None:
        streams = {}
        for key, s in self.streams.items():
            streams[key] = {
                "identity": list(s["identity"]),
                "physical_identity": list(s["physical_identity"]),
                "path": s["path"],
                "compressed": s["compressed"],
                "incarnation": s["incarnation"],
                "offset": s["offset"],
                "pending_hex": bytes(s["pending"]).hex(),
                "parsed": s["parsed"],
                "digest_hex": s["digest_hex"],
                "status": s["status"],
                "stop_reason": s.get("stop_reason"),
                "drain": s.get("drain"),
                "last_mtime_ns": s.get("last_mtime_ns"),
                "holes": [list(h) for h in s.get("holes", [])],
                "pending_base": s.get("pending_base", s["offset"]),
            }
        payload = {
            "version": 1,
            "active_name": self.active_name,
            "current": self.current,
            "paused": self.paused,
            "max_line_bytes": self.max_line_bytes,
            "max_archive_bytes": self.max_archive_bytes,
            "streams": streams,
        }
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.state_path)
        try:
            os.fsync(os.open(self.state_dir, os.O_RDONLY))
        except OSError:
            pass

    def snapshot(self) -> dict:
        """返回检查点摘要，供演示与运维核对。"""
        return {
            "current": self.current,
            "paused": self.paused,
            "streams": {
                key: {
                    "identity": list(s["identity"]),
                    "physical_identity": list(s["physical_identity"]),
                    "path": s["path"],
                    "compressed": s["compressed"],
                    "incarnation": s["incarnation"],
                    "offset": s["offset"],
                    "pending_bytes": len(s["pending"]),
                    "parsed": s["parsed"],
                    "status": s["status"],
                    "stop_reason": s.get("stop_reason"),
                }
                for key, s in self.streams.items()
            },
        }

    # ---------------------------------------------------------------- 暂停恢复
    def pause(self) -> None:
        """暂停跟读：状态完整落盘，恢复前不产生新读取。"""
        self.paused = True
        self._persist()

    def resume(self) -> None:
        self.paused = False
        self._persist()

    def is_paused(self) -> bool:
        return self.paused or self.pause_path.exists()

    def serve(self, on_event, interval: float = 0.2, polls: int | None = None):
        """简单跟读循环；``on_event(events)`` 返回 True 时自动确认本批。

        状态目录下存在 ``paused`` 文件或调用 :meth:`pause` 时只空转不读取，
        移除文件并 :meth:`resume` 后继续，检查点不丢失。
        """
        rounds = 0
        while polls is None or rounds < polls:
            if not self.is_paused():
                events = self.poll()
                if events and on_event(events):
                    self.ack()
            time.sleep(interval)
            rounds += 1

    # -------------------------------------------------------------------- 确认
    def ack(self, end: int | None = None) -> int:
        """业务采纳后确认字节；返回新的已确认偏移。

        默认确认当前流所有已解析字节。只允许确认到行边界，未闭合尾行保持
        缓冲。确认的字节进入前缀摘要，供后续 gzip 归档关联验证。
        """
        s = self._require_current()
        if end is None:
            # 已解析字节的绝对终点 = 缓冲基准 + 解析游标
            end = s.get("pending_base", s["offset"]) + s["parsed"]
        base = s.get("pending_base", s["offset"])
        delta_buf = end - base
        if delta_buf < 0 or delta_buf > s["parsed"]:
            raise ValueError(f"非法确认位置 {end}，已解析边界 {base + s['parsed']}")
        if delta_buf:
            hasher = self._hasher_for(s)
            hasher.update(bytes(s["pending"][:delta_buf]))
            del s["pending"][:delta_buf]
            s["parsed"] -= delta_buf
            s["offset"] = end
            s["pending_base"] = end
            s["digest_hex"] = hasher.copy().hexdigest()
        self._persist()
        return s["offset"]

    def _hasher_for(self, s: dict):
        key = self._key(s["identity"])
        hasher = self._hashers.get(key)
        if hasher is not None:
            return hasher
        # 进程重启后增量摘要对象丢失：若同 inode 明文文件仍在，重新读前缀
        # 重建；文件已不在则沿用落盘摘要，归档验证只依赖落盘摘要。
        hasher = hashlib.blake2b()
        if not s["compressed"]:
            for entry in inventory(self.root):
                if tuple(entry["identity"]) == s["identity"]:
                    with (self.root / entry["path"]).open("rb") as fh:
                        remaining = s["offset"]
                        while remaining:
                            chunk = fh.read(min(DEFAULT_CHUNK, remaining))
                            if not chunk:
                                break
                            hasher.update(chunk)
                            remaining -= len(chunk)
                    break
        self._hashers[key] = hasher
        return hasher

    # -------------------------------------------------------------------- 轮询
    def poll(self) -> list[Event]:
        """执行一次目录同步，返回新事件；读取到的字节先落盘再返回。"""
        if self.is_paused():
            return []
        entries = {e["path"]: e for e in inventory(self.root)}
        events: list[Event] = []
        s = self.streams.get(self.current) if self.current else None

        active = entries.get(self.active_name)
        if s is None:
            # 全新跟读。若活动路径存在，从其当前身份从 0 开始；目录里同时
            # 存在的历史明文/归档属于更早的未知来源，身份无法与新流关联，
            # 不猜测连续性，只接入活动文件。只有 gz 时从 0 接入该归档。
            if active is not None and not active["compressed"]:
                self._start_stream(active, events)
            elif self.active_name_gz in entries:
                self._start_stream(entries[self.active_name_gz], events,
                                   compressed=True)
            self._persist()
            return events

        # 活动路径身份变化（rename+新建、活动文件消失）时，当前流其实已是
        # 待收敛的旧流：先让它追尾/归档/停止，再接入新活动文件。
        identity_changed = not (
            active is not None and not active["compressed"]
            and tuple(active["identity"]) == s["identity"])
        if identity_changed and s["status"] == "active":
            self._retire_stream(s, entries, events)

        # 每次同步都收敛非当前历史流（归档可能在任何时刻出现）
        for key, other in list(self.streams.items()):
            if key != self.current and other["status"] == "active" \
                    and not other["compressed"]:
                self._retire_stream(other, entries, events)

        # 重新取当前流：收敛可能已经改变它的状态，但 current 键不变
        s = self.streams[self.current]
        if active is not None and not active["compressed"] \
                and tuple(active["identity"]) == s["identity"] \
                and s["status"] == "active":
            events.extend(self._follow_plain(s, active))
        elif active is not None and not active["compressed"]:
            # 活动路径出现新身份（或旧流已停止/归档）：作为独立新流接入，
            # 不声明与历史流连续
            self._start_stream(active, events)
        self._persist()
        return events

    def _retire_stream(self, s: dict, entries: dict,
                       events: list[Event]) -> None:
        """收敛一个不再位于活动路径的明文流：追尾 / 归档 / 停止。"""
        # 1) 旧 inode 仍以明文存在（典型 rename 轮转）：追读到当前 EOF。
        for entry in entries.values():
            if not entry["compressed"] \
                    and tuple(entry["identity"]) == s["identity"]:
                if entry["path"] != s["path"]:
                    s["path"] = entry["path"]
                events.extend(self._follow_plain(s, entry))
                return
        # 2) 明文消失但出现同名 .gz：用前缀摘要验证归档关联后接续。
        gz_entry = entries.get(s["path"] + ".gz") \
            or entries.get(self.active_name_gz)
        if gz_entry is not None:
            events.extend(self._drain_archive(s, gz_entry))
            return
        # 3) 身份无法确认：停止该流并报告缺口，绝不猜测关联。
        s["status"] = "stopped"
        s["stop_reason"] = "identity_lost"
        events.append(self._gap(
            s, s["offset"], s["offset"], "identity_lost",
            {"last_path": s["path"],
             "hint": "文件身份与归档证据均缺失，停止跟读"}))

    def _start_stream(self, entry: dict, events: list[Event],
                      compressed: bool = False) -> None:
        identity = tuple(entry["identity"])
        key = self._key(identity)
        if key in self.streams and self.streams[key]["status"] == "active":
            raise RuntimeError(f"流 {key} 已存在且仍在活动，拒绝覆盖")
        if key in self.streams:
            # inode 被复用：历史流保留可追溯，新身份使用独立键。
            key = key + f"#gen{len(self.streams)}"
        s = {
            "identity": identity,
            "physical_identity": identity,
            "path": entry["path"],
            "compressed": compressed,
            "incarnation": 0,
            "offset": 0,
            "pending": bytearray(),
            "parsed": 0,
            "digest_hex": hashlib.blake2b(b"").hexdigest(),
            "status": "active",
            "stop_reason": None,
            "drain": None,
            "last_mtime_ns": None,
            "holes": [],
            "pending_base": 0,
        }
        self.streams[key] = s
        self.current = key
        self._hashers[key] = hashlib.blake2b()
        events.append(self._lifecycle(s, "stream_started",
                                      {"path": entry["path"]}))
        if compressed:
            events.extend(self._drain_archive(s, entry))
        else:
            events.extend(self._follow_plain(s, entry))

    # ------------------------------------------------------------- 明文跟读逻辑
    def _follow_plain(self, s: dict, entry: dict) -> list[Event]:
        events: list[Event] = []
        readable = s["offset"] + len(s["pending"])
        size = entry["size"]
        mtime = (self.root / entry["path"]).stat().st_mtime_ns
        # 截短判定（同 inode 原地重写时仅靠 size 无法区分“截短到原长”与
        # “无新增”，因此同时使用 mtime 回退/变化 + 大小证据）：
        #   * 文件短于已确认偏移或已读取长度；
        #   * 或 mtime 变化且文件不高于已确认偏移（内容已被重写）。
        mtime_changed = s.get("last_mtime_ns") is not None \
            and mtime != s["last_mtime_ns"]
        # mtime 变化且文件不超过已确认偏移、又没有未确认缓冲可以解释时，
        # 说明同 inode 被截短重写（追加会让 size 超过已确认位置）。
        rewritten = mtime_changed and size <= s["offset"] \
            and not s["pending"]
        truncated = size < s["offset"] or size < readable or rewritten
        if truncated:
            # 同 inode 原地截短：代数加一后从 0 重读；相同文本不再被视作
            # 同一事件（incarnation 已变）。
            events.append(self._lifecycle(
                s, "truncated",
                {"previous_readable": readable, "new_size": size,
                 "previous_mtime_ns": s.get("last_mtime_ns"),
                 "mtime_ns": mtime}))
            s["incarnation"] += 1
            s["offset"] = 0
            s["pending"] = bytearray()
            s["parsed"] = 0
            s["drain"] = None
            s["holes"] = []
            s["pending_base"] = 0
            s["digest_hex"] = hashlib.blake2b(b"").hexdigest()
            self._hashers[self._key(s["identity"])] = hashlib.blake2b()
            readable = 0
        s["last_mtime_ns"] = mtime

        # 阶段 1：上一次轮询已识别但尚未读到行尾的超长行，先继续排空
        drained_now = False
        if s["drain"] is not None:
            drained_now = self._drain_remaining(s, entry, events)
            if s["drain"] is not None:
                return events  # 行尾仍未到达，本轮不再做正常读取/解析

        # 阶段 2：把文件新增字节读入缓冲。阶段 1 排空覆盖的区间已作为
        # “洞”移出 pending，读取水位取 已确认位置+缓冲长度 与 所有洞终点
        # 的最大值，否则会把超长行重新读入缓冲造成重复。
        watermark = s["offset"] + len(s["pending"])
        for hole in s.get("holes", []):
            watermark = max(watermark, hole[1])
        if s.get("drained_end") is not None:
            watermark = max(watermark, s.pop("drained_end"))
        readable = watermark
        if size > readable:
            # 若读取起点不与当前 pending 尾部连续（中间隔着洞），把
            # pending_base 锚定到新读取起点，事件绝对范围据此计算。
            if readable != s["offset"] + len(s["pending"]):
                s["pending_base"] = readable
                s["pending"] = bytearray()
                s["parsed"] = 0
            with (self.root / entry["path"]).open("rb") as fh:
                fh.seek(readable)
                while readable < size:
                    chunk = fh.read(min(DEFAULT_CHUNK, size - readable))
                    if not chunk:
                        break
                    readable += len(chunk)
                    s["pending"].extend(chunk)

        # 阶段 3：解析已闭合的行（重启重放、截短重置、新增字节都走这里）。
        # 闭合超长行在解析器内部直接转缺口，不会被当成记录。
        events.extend(self._emit_lines(s))

        # 阶段 4：检测最后一个未闭合尾行是否超长（换行尚未到达的情形）。
        # 阶段 1 的跨轮询排空已经在阶段 3 解析过新字节，这里不再重复。
        if not s["compressed"] and not drained_now:
            self._detect_oversize(s, entry, events)
        return events

    def _detect_oversize(self, s: dict, entry: dict,
                         events: list[Event]) -> bool:
        """识别缓冲中最后一个未闭合行是否超长；超长则进入排空模式。

        缓冲布局为 ``[0, parsed)`` 已解析待 ack、``[parsed, ...)`` 未解析。
        只检查未解析部分里最后一个换行之后的尾行；超长时把该尾行整体移出
        pending（已解析字节绝不回删），后续字节直接从文件排空到行尾。
        返回 True 表示本轮排空已读到行尾并带回了新字节。
        """
        unparsed = bytes(s["pending"][s["parsed"]:])
        last_nl = unparsed.rfind(b"\n")
        tail = unparsed if last_nl < 0 else unparsed[last_nl + 1:]
        if not tail or b"\n" in tail or len(tail) <= self.max_line_bytes:
            return False
        tail_start_in_buf = s["parsed"] + (0 if last_nl < 0
                                           else last_nl + 1)
        base = s.get("pending_base", s["offset"])
        s["drain"] = {
            "start": base + tail_start_in_buf,
            "count": len(tail),
            "evidence_hex": tail[:EVIDENCE_BYTES].hex(),
        }
        # 移除未解析的超长尾行，保留 [0, tail_start_in_buf)
        del s["pending"][tail_start_in_buf:]
        return self._drain_remaining(s, entry, events)

    def _drain_remaining(self, s: dict, entry: dict,
                         events: list[Event]) -> bool:
        """超长行已移出缓冲，从文件中的绝对位置继续读到行尾并报告缺口。

        行尾仍未到达时保留 ``drain`` 状态，下次轮询继续，不把无界数据
        压进内存；到达行尾后重建缓冲（跳过超长行区间）并返回 True。
        """
        position = s["drain"]["start"] + s["drain"]["count"]
        size = entry["size"]
        gap_end: int | None = None
        with (self.root / entry["path"]).open("rb") as fh:
            fh.seek(position)
            while position < size:
                chunk = fh.read(min(DEFAULT_CHUNK, size - position))
                if not chunk:
                    break
                position += len(chunk)
                nl = chunk.find(b"\n")
                if nl < 0:
                    s["drain"]["count"] += len(chunk)
                    continue
                s["drain"]["count"] += nl + 1
                drain = s["drain"]
                s["drain"] = None
                gap_end = drain["start"] + drain["count"]
                events.append(self._gap(
                    s, drain["start"], gap_end,
                    "line_too_long",
                    {"limit_bytes": self.max_line_bytes,
                     "evidence_hex": drain["evidence_hex"]}))
                break
        if gap_end is None:
            return False
        # 重建缓冲：只保留超长行之前、已确认偏移起的前缀；行尾后的正常
        # 内容不在这里读入，交给阶段 2 从 gap_end 水位正常读取，避免重复。
        path = self.root / entry["path"]
        with path.open("rb") as fh:
            fh.seek(s["offset"])
            prefix = fh.read(drain["start"] - s["offset"])
        s["pending"] = bytearray(prefix)
        # pending[0] 仍对应已确认偏移；洞之后读取的新段会重设 pending_base
        s["pending_base"] = s["offset"]
        # 已解析游标按保留的前缀长度收敛，绝不越过它
        s["parsed"] = min(s["parsed"], len(prefix))
        # 记录被跳过的洞（超长行字节区间）：这些字节不在 pending 里，但
        # 读取水位必须越过洞终点，否则下一轮会把超长行重新读入。
        s.setdefault("holes", []).append([drain["start"], gap_end])
        s["drained_end"] = gap_end
        return True

    def _emit_lines(self, s: dict) -> list[Event]:
        events: list[Event] = []
        data = s["pending"]
        base = s.get("pending_base", s["offset"])
        p = s["parsed"]
        if p and not data:
            # 截短重置后缓冲已清空，游标必须归零
            p = 0
            s["parsed"] = 0
        while True:
            nl = data.find(b"\n", p)
            if nl < 0:
                break
            raw = bytes(data[p:nl])
            start, end = base + p, base + nl + 1
            p = nl + 1
            if not raw:
                continue
            if len(raw) > self.max_line_bytes:
                # 闭合行本身超长：不解析内容，保留前缀证据并报告缺口，
                # 行位置照常推进，后续合法行不受影响。
                events.append(self._gap(
                    s, start, end, "line_too_long",
                    {"limit_bytes": self.max_line_bytes,
                     "evidence_hex": raw[:EVIDENCE_BYTES].hex()}))
                continue
            try:
                text = raw.decode("utf-8")  # strict：绝不产生替代字符
            except UnicodeDecodeError:
                events.append(self._corrupt(s, start, end, raw,
                                            "invalid_utf8"))
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                events.append(self._corrupt(s, start, end, raw,
                                            "invalid_json",
                                            {"error": str(exc)}))
                continue
            events.append(Event(
                kind=KIND_RECORD, path=s["path"],
                identity=list(s["physical_identity"]),
                origin_identity=list(s["identity"]),
                compressed=s["compressed"], incarnation=s["incarnation"],
                start=start, end=end, record=record))
        s["parsed"] = p
        return events

    # ------------------------------------------------------------- gzip 归档逻辑
    def _read_gzip_limited(self, path: Path):
        data = bytearray()
        error = None
        try:
            with gzip.open(path, "rb") as fh:
                while len(data) < self.max_archive_bytes:
                    chunk = fh.read(DEFAULT_CHUNK)
                    if not chunk:
                        break
                    room = self.max_archive_bytes - len(data)
                    data.extend(chunk[:room])
                    if len(chunk) > room:
                        error = "archive_oversize"
                        break
        except (OSError, EOFError, zlib.error) as exc:
            error = "archive_corrupt"
            return bytes(data), error, str(exc)
        return bytes(data), error, None

    def _drain_archive(self, s: dict, entry: dict) -> list[Event]:
        data, error, detail_text = self._read_gzip_limited(
            self.root / entry["path"])
        if error == "archive_corrupt":
            s["status"] = "stopped"
            s["stop_reason"] = error
            return [self._gap(s, s["offset"], s["offset"] + len(data),
                              "archive_corrupt",
                              {"path": entry["path"], "error": detail_text,
                               "recovered_bytes": len(data)})]
        if error == "archive_oversize":
            # 超限是根因：截断后的前缀可能无法通过摘要验证，必须先报告
            # 资源缺口并停止，不能误判成归档内容不一致。
            cap = self.max_archive_bytes
            s["status"] = "stopped"
            s["stop_reason"] = error
            return [self._gap(s, cap, cap, "archive_oversize",
                              {"limit_bytes": cap,
                               "recovered_bytes": len(data),
                               "hint": "超出解压上限，后续字节未读"})]
        # 关联证据：解压前缀必须与已确认字节摘要一致，未确认缓冲也要逐字节
        # 吻合；任一不满足即不承诺连续。
        if len(data) < s["offset"] \
                or _digest(data[:s["offset"]]) != s["digest_hex"] \
                or data[s["offset"]:s["offset"] + len(s["pending"])] \
                != bytes(s["pending"]):
            s["status"] = "stopped"
            s["stop_reason"] = "archive_unverified"
            return [self._gap(s, s["offset"], s["offset"],
                              "archive_unverified",
                              {"path": entry["path"],
                               "hint": "归档内容与已确认前缀不一致，停止"})]
        s["physical_identity"] = tuple(entry["identity"])
        s["compressed"] = True
        s["path"] = entry["path"]
        s["pending"] = bytearray(data[s["offset"]:])
        # 解压流连续且从 offset 开始，缓冲基准锚回已确认偏移
        s["pending_base"] = s["offset"]
        # 解压流整体替换缓冲：游标归零重新解析，已确认部分已被 offset 排除，
        # 未确认部分以相同身份与范围 at-least-once 重放。
        s["parsed"] = 0
        events = self._emit_lines(s)
        s["status"] = "archived"
        return events

    # ------------------------------------------------------------------ 构造事件
    def _corrupt(self, s: dict, start: int, end: int, raw: bytes,
                 reason: str, extra: dict | None = None) -> Event:
        detail = {"raw_length": len(raw)}
        if extra:
            detail.update(extra)
        return Event(
            kind=KIND_CORRUPT, path=s["path"],
            identity=list(s["physical_identity"]),
            origin_identity=list(s["identity"]),
            compressed=s["compressed"], incarnation=s["incarnation"],
            start=start, end=end, raw_hex=raw.hex(), reason=reason,
            detail=detail)

    def _gap(self, s: dict, start: int, end: int, reason: str,
             detail: dict | None = None) -> Event:
        return Event(
            kind=KIND_GAP, path=s["path"],
            identity=list(s["physical_identity"]),
            origin_identity=list(s["identity"]),
            compressed=s["compressed"], incarnation=s["incarnation"],
            start=start, end=end, reason=reason, detail=detail or {})

    def _lifecycle(self, s: dict, reason: str, detail: dict) -> Event:
        return Event(
            kind=KIND_LIFECYCLE, path=s["path"],
            identity=list(s["physical_identity"]),
            origin_identity=list(s["identity"]),
            compressed=s["compressed"], incarnation=s["incarnation"],
            start=s["offset"], end=s["offset"], reason=reason, detail=detail)

    @staticmethod
    def _key(identity: tuple) -> str:
        return f"{identity[0]}:{identity[1]}"

    def _require_current(self) -> dict:
        if not self.current or self.current not in self.streams:
            raise RuntimeError("没有活动的跟读流")
        return self.streams[self.current]
