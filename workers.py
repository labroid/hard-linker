import zlib
from hashlib import md5
import stat
from pathlib import Path
from dataclasses import dataclass
from collections import namedtuple


def get_md5(path: Path) -> str:
    """
    Compute MD5 hash
    :param path: pathlib Path object
    :return: hash: MD5 hash as hexadecimal string
    """
    result = md5()
    with path.open(mode="rb") as f:
        while True:
            data = f.read(
                512 * 1024
            )  # 1k * 512 byte disk sectors - benchmarks pretty well
            if not data:
                break
            result.update(data)
    return result.hexdigest()


def hash_path(path):
    try:
        md5sum = get_md5(path)
    except:  # Too broad; let's see what failures we get
        md5sum = ''
    return {'path': path, 'md5': md5sum}


@dataclass()
class Record:
    inode: int
    size: int
    mtime: int


def filestats(dirpath):
    a = {}
    for p in dirpath.rglob("*"):
        s = p.stat()
        if stat.S_ISREG(s[stat.ST_MODE]):
            a |= {
                p: Record(
                    inode=s[stat.ST_INO], size=s[stat.ST_SIZE], mtime=s[stat.ST_MTIME]
                ),
            }
    return a
