import gzip
import os
from pathlib import Path

def seed(root, fixture):
    root=Path(root)
    root.mkdir(parents=True,exist_ok=True)
    for name,content in fixture['files'].items():
        (root/name).write_bytes(content.encode('utf-8'))

def inventory(root):
    entries=[]
    for path in sorted(Path(root).iterdir()):
        if path.is_file():
            stat=path.stat()
            entries.append({'path':path.name,'identity':[stat.st_dev,stat.st_ino],
                            'size':stat.st_size,'compressed':path.suffix=='.gz'})
    return entries

def append(root, content):
    with (Path(root)/'gateway.log').open('ab') as stream:
        stream.write(content.encode())
        stream.flush()
        os.fsync(stream.fileno())

def rotate(root, replacement):
    root=Path(root)
    (root/'gateway.log').rename(root/'gateway.log.1')
    (root/'gateway.log').write_bytes(replacement.encode())

def truncate(root, replacement):
    with (Path(root)/'gateway.log').open('wb') as stream:
        stream.write(replacement.encode())

def compress_archive(root):
    root=Path(root)
    path=root/'gateway.log.1'
    (root/'gateway.log.1.gz').write_bytes(gzip.compress(path.read_bytes(),mtime=0))
    path.unlink()
