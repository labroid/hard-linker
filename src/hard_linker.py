import hashlib
import os
import shutil
import stat
from pathlib import Path

import dask.dataframe as dd
import humanize
import pandas as pd
from loguru import logger

DO_NOT_HARD_LINK = True
HASH_FILE_NAME = "filesystem_scan_hash_savepoint.zip"


def hash_and_link(
    target_dirs: list[str] | list[Path],
    compute_all_hashes: bool = True,
    hard_link: bool = False,
    verbose: bool = False,
):
    roots = [Path(p) for p in target_dirs]  # Path() is idempotent; Path(Path) => Path
    logger.level("INFO") if verbose else logger.level("WARNING")
    check_roots_on_same_filesystem(roots)
    # TODO:  check_if_roots_related(roots) -> related roots reduced to parent Path.is_relative_to(Path)
    disk_free_before = shutil.disk_usage(roots[0]).free
    logger.info(f"Free disk space: {humanize.naturalsize(disk_free_before)}")
    dir_hashes = []
    for r in roots:
        f = scan_filesystem(r)
        f = merge_previous_scan_hashes(f, get_cached_scan(r / HASH_FILE_NAME))
        f = assign_known_hashes_to_hard_linked_files(f)
        f = compute_hashes(f, compute_all_hashes=compute_all_hashes)
        save_hashes(f, r / HASH_FILE_NAME)
        dir_hashes.append(f)
    if hard_link:
        all_hashes = pd.concat(dir_hashes, axis=0)
        inodes_before = all_hashes.inode.nunique()
        all_hashes = assign_known_hashes_to_hard_linked_files(all_hashes)
        hard_link_hash_groups(all_hashes)
        disk_free_after = shutil.disk_usage(roots[0]).free
        logger.info(
            f"Free disk space: {humanize.naturalsize(disk_free_after)}.\n"
            f"Freed {humanize.naturalsize(disk_free_after - disk_free_before)}\n"
            f"{all_hashes.inode.nunique() - inodes_before} inodes freed"
        )


def save_hashes(f, location):
    f[pd.notna(f.md5)].to_pickle(location, compression="zip")


def compute_hashes(f, compute_all_hashes: bool = False):
    if pd.notna(f.md5).all():
        logger.info("All hashes already computed")
        return f
    if not compute_all_hashes:
        f = f.loc[f.duplicated(subset="size", keep=False)]  # Ignore files with unique length
    need_hash = f.loc[pd.isna(f.md5)]
    need_hash = need_hash[~need_hash.duplicated(subset="inode", keep="first")]
    need_hash = need_hash.reset_index()  # Preserve Path objects to column; dask mangles objects used in index
    need_hash_dd = dd.from_pandas(need_hash.loc[:, ["path", "md5"]], npartitions=os.cpu_count())
    logger.info(f"Computing hashes for {humanize.intcomma(len(need_hash))} files...")
    need_hash_dd["md5"] = need_hash_dd.apply(hasher, axis=1, meta=("md5", str)).compute()
    hashed = need_hash_dd.compute().set_index("path")
    f = f.merge(hashed, how="left", left_index=True, right_index=True, suffixes=("old", ""))
    f.loc[pd.isna(f.md5), "md5"] = f.loc[pd.isna(f.md5), "md5old"]
    f = f.drop(columns=["md5old"])
    f = assign_known_hashes_to_hard_linked_files(f)
    logger.info("Done computing hashes.")
    return f


def get_free_space(roots: list[Path]) -> int:
    disk_free_before = shutil.disk_usage(roots[0]).free
    logger.info(f"Free disk space: {humanize.naturalsize(disk_free_before)}")
    return disk_free_before


def check_roots_on_same_filesystem(roots: list[Path]):
    if len({p.stat().st_dev for p in roots}) > 1:
        message = "Roots must be in same filesystem."
        logger.error(message)
        raise OSError(message)


def scan_filesystem(root: Path):  # Perhaps scan lists of files or lists of dirs?
    """
    Scans filesystem and creates dataframe of files with inode, size, mtime, links, md5 indexed by path
    """
    logger.info(f"Scanning {root}...")
    tree = (
        [{"path": root, "stats": root.stat()}]
        if root.is_file()
        else [{"path": path, "stats": path.stat()} for path in root.rglob("*")]
    )
    files = pd.DataFrame(
        [
            {
                "path": t["path"],
                "inode": t["stats"].st_ino,
                "size": t["stats"].st_size,
                "mtime": t["stats"].st_mtime,
                "links": t["stats"].st_nlink,
            }
            for t in tree
            if stat.S_ISREG(t["stats"].st_mode)
        ]
    )
    logger.info(
        f"Total files: {humanize.intcomma(len(files))} files, " f"consuming {humanize.naturalsize(files['size'].sum())}"
    )
    files = files.set_index(keys="path")
    return files


def merge_previous_scan_hashes(f: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    if old.empty:
        return f.assign(md5=pd.NA)
    logger.info("Merging previous scan hashes...")
    merged = f.merge(old, how="left", left_index=True, right_index=True, suffixes=("", "_old"))
    identical = (merged["size"] == merged.size_old) & (merged.mtime == merged.mtime_old)
    merged.loc[~identical, "md5"] = pd.NA
    merged = merged.drop(columns=["inode_old", "size_old", "mtime_old", "links_old"])
    return merged


def get_cached_scan(path: Path) -> pd.DataFrame:
    logger.info("Reading in previous scan cache...")
    try:
        old = pd.read_pickle(path)
    except (EOFError, FileNotFoundError):
        logger.info(f"{path}: No previous scan available")
        return pd.DataFrame()
    return old


def assign_known_hashes_to_hard_linked_files(f: pd.DataFrame):
    def reconcile_inode_groups(df: pd.DataFrame):
        if all(df.need_hash):
            return df
        hash_set = df.md5.dropna().unique()
        if len(hash_set) > 1:  # Multiple hashes assigned to same inode; this is a problem
            logger.warning(
                f"Paths with same inode have different hashes. Ignoring previous scan. {df.path, df.md5.unique()}"
            )
            df.loc[:, "md5"] = pd.NA
            return df
        if all(~df.need_hash):
            return df
        df.loc[df.need_hash, "md5"] = list(hash_set)[0]
        return df

    logger.info("Assigning known hashes to hard linked files...")
    affected_inodes = f.loc[(f.links > 1) & (pd.isna(f.md5))].inode.unique()
    hardlinked = f.loc[f.inode.isin(affected_inodes), ["inode", "md5"]]
    hardlinked["need_hash"] = hardlinked.md5.isna()
    if hardlinked.empty:
        return f
    if hardlinked.md5.notna().all():
        return f
    hardlinked = hardlinked.groupby("inode", group_keys=False).apply(reconcile_inode_groups)
    hardlinked = hardlinked.loc[hardlinked.need_hash, ["md5"]]
    merged = f.merge(hardlinked.md5, how="left", left_index=True, right_index=True, suffixes=("", "_new"))
    merged.loc[pd.isna(f.md5), "md5"] = merged.loc[pd.isna(f.md5), "md5_new"]
    f = merged.drop(columns=["md5_new"])
    return f


def hard_link_hash_groups(df: pd.DataFrame):
    logger.info(f"Hard linking files with same hash...")
    # Ignore unique hashes
    df = df.loc[df.duplicated(subset="md5", keep=False)]
    # Group by hash and link process
    df.groupby("md5").apply(hard_link_paths)


def hard_link_paths(df: pd.DataFrame):
    # Might be faster to return None if there is only one inode in the group (and skip if statement in loop)
    # Sort, so we can link to most-hard-linked of group or arbitrarily to top of group
    df = df.sort_values(by="links", ascending=False)
    for p in df.index[1:]:
        # Skip if already hard linked to top of group
        if df.loc[p].inode == df.iloc[0].inode:
            continue
        logger.info(f"Hard linking {p} to {df.index[0]}")
        p.unlink()
        p.hardlink_to(df.index[0])
    return None


def hasher(s: pd.Series):
    result = hashlib.md5()
    with s.path.open(mode="rb") as f:
        while True:
            if data := f.read(512 * 1024):  # 1k * 512 byte disk sectors - benchmarks pretty well
                result.update(data)
            else:
                break
    return result.hexdigest()


if __name__ == "__main__":
    TARGET_DIRS = [
        Path(r"C:\Users\scott\Pictures\Domain Backsup not in Local Takeout\Photos"),
        Path(r"C:\Users\scott\Pictures\Takeout"),
    ]
    hash_and_link(TARGET_DIRS, verbose=True, compute_all_hashes=True, hard_link=True)
