import json
from pathlib import Path
from source import inventory

def read_closed_lines(path):
    content=Path(path).read_bytes()
    lines=content.split(b'\n')
    output=[]
    for line in lines[:-1]:
        if line:
            output.append(json.loads(line.decode('utf-8')))
    return output

def inspect(root, fixture):
    return {'文件':inventory(root), '已闭合记录':read_closed_lines(Path(root)/'gateway.log')}
