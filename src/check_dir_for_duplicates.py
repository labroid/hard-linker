from pathlib import Path
from hard_linker import hash_and_link
import pandas as pd


def compare_dirs(new: Path, archive: Path, clean: bool = False):
    new = Path(new)
    archive = Path(archive)
    hash_and_link([new], hard_link=False, compute_all_hashes=True)
    hash_and_link([archive], hard_link=False, compute_all_hashes=True)
    new_hashes = pd.read_pickle(new / "filesystem_scan_hash_savepoint.zip").reset_index()
    archive_hashes = pd.read_pickle(archive / "filesystem_scan_hash_savepoint.zip").reset_index()
    candidates = new_hashes.merge(archive_hashes, on=["md5"], how="inner", suffixes=("_new", "_archive"))
    print(f"Found {len(candidates)} duplicates totalling {candidates.size.sum()} bytes")
    if clean:
        for p in candidates.path_new:
            try:
                p.unlink()
                print(f"Deleted {p}")
            except FileNotFoundError:
                print(f"File not found: {p}")


if __name__ == "__main__":
    compare_dirs(Path(r"C:\Users\scott\Desktop\MOV00B-108.TOD"), Path(r"C:\Users\scott\Pictures\Takeout"))
