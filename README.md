# 轮转日志的连续采集与字节级恢复

本工具用于查看授权来源的数据及其原始边界，样例中的机构、设备、人员和标识均为虚构。现有入口提供来源预览，不改变来源数据。

## 目录

`source.py` 提供可重复的本地来源；`reader.py` 负责现有读取；`app.py` 是预览入口；`fixtures/source.json` 可替换为同结构样例；`docs/数据约定.md` 描述接口与数据含义。

## 运行

使用 Python 3.11 及标准库，无第三方依赖。

```bash
python3 app.py
python3 -m unittest discover -s tests -v
PYTHONPYCACHEPREFIX=/tmp/collector-pycache python3 -m compileall -q app.py reader.py source.py tests
```

## 容器

基础镜像固定摘要，不在启动阶段安装软件或获取远程数据。

```bash
docker build --pull=false -t rotating-tail:local .
docker run --rm --network none rotating-tail:local
docker run --rm --network none rotating-tail:local python3 -m unittest discover -s tests -v
```

## 本机演示

在全新临时目录执行 `python3 app.py` 可查看实际读取结果，随后运行测试以观察来源边界。这两步使用本地小尺寸材料，通常数秒结束，不需要账户、设备或外部站点。持久化数据应放在 `state/` 或调用方明确传入的目录，不写入样例文件。
