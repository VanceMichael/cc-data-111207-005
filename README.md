# 轮转日志的连续采集与字节级恢复

本工具用于查看授权来源的数据及其原始边界，样例中的机构、设备、人员和标识均为虚构。现有入口提供来源预览，不改变来源数据。

## 目录

`source.py` 提供可重复的本地来源与目录动作（追加、rename 轮转、原地截短、gzip 压缩）；`reader.py` 负责现有只读查看；`follower.py` 是字节级持久化跟读器；`app.py` 提供 `inspect` 与 `follow` 两个入口；`demo_follow.py` 在真实临时文件上演示全部动作；`fixtures/source.json` 可替换为同结构样例；`docs/数据约定.md` 描述接口与数据含义。

## 运行

使用 Python 3.11 及标准库，无第三方依赖。隔离模式（`-I`）下用包方式跑测试：

```bash
python3 -I app.py
python3 -I app.py inspect
python3 -I -m unittest discover -s tests -t . -v
python3 -I demo_follow.py
PYTHONPYCACHEPREFIX=/tmp/collector-pycache python3 -I -m compileall -q app.py reader.py follower.py source.py tests
```

## 持久化跟读

`Follower(root, state_dir)` 对共享目录做持续跟读，检查点原子写入状态目录的 `checkpoint.json`，不改动来源文件：

```python
from follower import Follower, event_key

fw = Follower("/share/gateway", "/var/lib/follow-state")
events = fw.poll()          # 读到的字节先落盘再返回
handle(events)              # 业务处理（能耗分析等）
fw.ack()                   # 采纳后才推进已确认偏移
```

每条事件携带 `identity`/`origin_identity`（物理与逻辑流身份）、`incarnation`（截短代数）、`compressed`、未压缩字节范围 `[start, end)`；`record`/`corrupt`/`gap`/`lifecycle` 四类事件分别承载业务记录、损坏行（含 `raw_hex`）、缺口与生命周期。暂停与恢复：

```bash
python3 -I app.py follow --root /share/gateway --state /var/lib/follow-state --polls 0
python3 -I app.py follow --root /share/gateway --state /var/lib/follow-state --pause
python3 -I app.py follow --root /share/gateway --state /var/lib/follow-state --resume
python3 -I app.py follow --root /share/gateway --state /var/lib/follow-state --snapshot
```

状态目录下出现 `paused` 文件同样会暂停跟读。语义细节（轮转追尾、归档摘要验证、身份丢失停止、半行与多字节恢复、超长行与损坏归档上限）见 `docs/数据约定.md`。

## 容器

基础镜像固定摘要，不在启动阶段安装软件或获取远程数据。

```bash
docker build --pull=false -t rotating-tail:local .
docker run --rm --network none rotating-tail:local
docker run --rm --network none rotating-tail:local python3 -I -m unittest discover -s tests -t . -v
```

## 本机演示

`python3 -I demo_follow.py` 在 `tempfile.mkdtemp()` 创建的真实目录里依次执行追加、半截多字节写入、rename 轮转、同路径新建、gzip 压缩、原地截短、损坏行与超长行写入，并多次重新构造 `Follower` 模拟进程重启；每步打印事件身份与字节范围以及检查点摘要。`python3 -I app.py` 保留原有只读预览。持久化数据应放在 `state/` 或调用方明确传入的目录，不写入样例文件。
