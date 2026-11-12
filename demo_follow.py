"""端到端演示：在真实临时目录里对真实文件执行全部目录动作。

与单元测试不同，本脚本：

* 使用 :class:`tempfile.TemporaryDirectory` 创建真实目录，所有动作
  （append / rename / 新建 / truncate / gzip）都落到真实文件系统；
* 每个阶段都重新构造 :class:`follower.Follower` 实例，模拟进程重启后
  仅凭检查点恢复跟读；
* 打印每条事件的来源身份、压缩标记、截短代数与未压缩字节范围，并打印
  检查点摘要，证明缓冲字节与偏移如何跨重启协调。

用 ``python3 -I demo_follow.py`` 运行，不依赖第三方库。
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from follower import Follower, event_key  # noqa: E402
from source import (  # noqa: E402
    append, compress_archive, inventory, rotate, seed, truncate,
)

FIXTURE = Path(__file__).parent / "fixtures" / "source.json"


def show(title: str, events, fw: Follower) -> None:
    print(f"\n=== {title} ===")
    for ev in events:
        body = ev.as_dict()
        line = (f"[{ev.kind:9}] {ev.path:16} "
                f"id={tuple(ev.identity)} origin={tuple(ev.origin_identity or ev.identity)} "
                f"inc={ev.incarnation} bytes=[{ev.start},{ev.end}) "
                f"gz={ev.compressed}")
        if ev.kind == "record":
            line += f" record={json.dumps(ev.record, ensure_ascii=False)}"
        elif ev.kind == "corrupt":
            line += f" reason={ev.reason} raw_hex={ev.raw_hex[:32]}..."
        else:
            line += f" reason={ev.reason} detail={json.dumps(ev.detail, ensure_ascii=False)}"
        print(line)
    print("检查点:", json.dumps(fw.snapshot(), ensure_ascii=False))


def main() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    work = Path(tempfile.mkdtemp(prefix="follow-demo-"))
    root, state = work / "share", work / "state"
    print(f"临时工作目录（真实文件系统）: {work}")
    seed(root, fixture)
    print("初始目录:", [(e["path"], tuple(e["identity"]), e["size"])
                       for e in inventory(root)])

    # 1) 首次跟读：闭合记录下发，无换行的半行留在缓冲
    fw = Follower(root, state, max_line_bytes=1024)
    show("首次 poll：半行不下发", fw.poll(), fw)
    fw.ack()  # 业务只采纳已闭合的第一条

    # 2) 追加补全半行（含中文多字节字符），再模拟进程重启
    append(root, fixture["append"])
    fw2 = Follower(root, state, max_line_bytes=1024)  # 新实例 = 重启，仅靠 checkpoint.json 恢复
    show("重启后追加补全记录", fw2.poll(), fw2)
    fw2.ack()

    # 3) 半条多字节字符切到一半后重启：不得出现替代字符
    multibyte = '{"device":"C","value":"温度℃"}\n'.encode("utf-8")
    cut = multibyte_cut(multibyte)
    with (root / "gateway.log").open("ab") as fh:
        fh.write(multibyte[:cut])
    Follower(root, state, max_line_bytes=1024).poll()  # 读到半个字符即落盘
    with (root / "gateway.log").open("ab") as fh:
        fh.write(multibyte[cut:])
    fw3 = Follower(root, state, max_line_bytes=1024)
    show("半截多字节字符跨重启拼回", fw3.poll(), fw3)
    fw3.ack()

    # 4) rename 轮转 + 同路径新建：旧 inode 追尾，新 inode 从 0 接入
    old_id = tuple(inventory(root)[0]["identity"])
    rotate(root, fixture["new_file"])
    fw4 = Follower(root, state, max_line_bytes=1024)
    show("rename 轮转：旧流追尾 + 新流接入", fw4.poll(), fw4)
    fw4.ack()
    new_id = next(
        tuple(e["identity"]) for e in inventory(root)
        if e["path"] == "gateway.log")
    assert old_id != new_id, "轮转后活动文件身份应当改变"

    # 5) 旧明文压缩为 gzip：前缀摘要验证后接续，偏移按解压字节计算
    compress_archive(root)
    fw5 = Follower(root, state, max_line_bytes=1024)
    show("gzip 归档：验证关联并接续", fw5.poll(), fw5)

    # 6) 原地截短后写入相同文本：incarnation 变化，不判成重复
    same = fixture["new_file"]
    truncate(root, same)
    fw6 = Follower(root, state, max_line_bytes=1024)
    evs = fw6.poll()
    show("原地截短：相同文本不是重复事件", evs, fw6)
    keys = {event_key(e) for e in evs if e.kind == "record"}
    print("截短后事件键（含 incarnation）:", keys)

    # 7) 损坏行 + 超长行：保留原字节、限制资源、不挡后续合法行
    fw6.ack()  # 先采纳截短后的记录，使长度检测基于干净的未确认缓冲
    messy = (b'{"device":"D","value":1}\n'
             b'\xc3\x28not-utf8\n'
             b'{"not-json"\n'
             + b'{"x":"' + b"z" * 5000 + b'"}\n'
             + b'{"device":"E","value":2}\n')
    with (root / "gateway.log").open("ab") as fh:
        fh.write(messy)
    fw7 = Follower(root, state, max_line_bytes=1024)
    show("损坏行隔离 + 超长行缺口 + 合法行继续", fw7.poll(), fw7)

    # 8) 暂停 / 恢复：状态落盘，暂停期间不读取
    fw7.pause()
    append(root, '{"device":"F","value":3}\n')
    paused_events = fw7.poll()
    assert paused_events == [], "暂停期间不得产生事件"
    print("\n=== 暂停期间 poll 返回", paused_events, "（状态已持久化）===")
    fw7.resume()
    show("恢复后继续跟读", fw7.poll(), fw7)

    print("\n演示结束，检查点文件:", state / "checkpoint.json")


def multibyte_cut(data: bytes) -> int:
    # 切在多字节字符“温”的第二个字节前，制造半截 UTF-8 字符
    marker = "温".encode("utf-8")
    idx = data.index(marker)
    return idx + 1


if __name__ == "__main__":
    main()
