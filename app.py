import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from follower import Follower  # noqa: E402
from reader import inspect  # noqa: E402
from source import seed  # noqa: E402


def run(fixture):
    with tempfile.TemporaryDirectory() as directory:
        seed(directory, fixture)
        return inspect(directory, fixture)


def cmd_inspect(arguments) -> None:
    """原有目录查看入口：只读、不改来源数据。"""
    fixture = json.loads(arguments.fixture.read_text(encoding="utf-8"))
    print(json.dumps(run(fixture), ensure_ascii=False, indent=2))


def cmd_follow(arguments) -> None:
    """持久化跟读入口：检查点落在 --state 目录，可暂停、可恢复。"""
    fw = Follower(arguments.root, arguments.state,
                  max_line_bytes=arguments.max_line_bytes,
                  max_archive_bytes=arguments.max_archive_bytes)
    if arguments.pause:
        fw.pause()
        print(f"已暂停，状态目录: {arguments.state}")
        return
    if arguments.resume:
        fw.resume()
        print("已恢复跟读")
    if arguments.snapshot:
        print(json.dumps(fw.snapshot(), ensure_ascii=False, indent=2))
        return

    def emit(events):
        for event in events:
            print(json.dumps(event.as_dict(), ensure_ascii=False))
        return True  # 成功打印即视为采纳，自动 ack 推进检查点

    fw.serve(emit, interval=arguments.interval, polls=arguments.polls)


def main():
    parser = argparse.ArgumentParser(description="本地来源查看与持久化跟读")
    sub = parser.add_subparsers(dest="command")

    p_inspect = sub.add_parser("inspect", help="目录查看（默认入口，只读）")
    p_inspect.add_argument("--fixture", type=Path,
                           default=Path(__file__).parent / "fixtures/source.json")
    p_inspect.set_defaults(func=cmd_inspect)

    p_follow = sub.add_parser("follow", help="持久化跟读共享目录")
    p_follow.add_argument("--root", type=Path, required=True,
                          help="网关共享目录（真实文件系统路径）")
    p_follow.add_argument("--state", type=Path, required=True,
                          help="检查点状态目录（不写入来源目录）")
    p_follow.add_argument("--polls", type=int, default=None,
                          help="轮询次数；缺省持续跟读")
    p_follow.add_argument("--interval", type=float, default=0.2)
    p_follow.add_argument("--max-line-bytes", type=int, default=1 << 20)
    p_follow.add_argument("--max-archive-bytes", type=int, default=256 << 20)
    ctrl = p_follow.add_mutually_exclusive_group()
    ctrl.add_argument("--pause", action="store_true", help="暂停并落盘")
    ctrl.add_argument("--resume", action="store_true", help="恢复跟读")
    p_follow.add_argument("--snapshot", action="store_true",
                          help="只打印检查点摘要，不读取来源")
    p_follow.set_defaults(func=cmd_follow)

    arguments = parser.parse_args()
    if arguments.command is None:
        # 不带子命令时保持原有预览行为
        arguments.fixture = Path(__file__).parent / "fixtures/source.json"
        return cmd_inspect(arguments)
    arguments.func(arguments)


if __name__ == "__main__":
    main()
