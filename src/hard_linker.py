import itertools
import shutil
import stat
import hashlib
from pathlib import Path
import dask.dataframe as dd

import humanize
import pandas as pd
from loguru import logger

# TODO: This should be in config file
ROOTS = [Path(r"C:\Users\scott\Pictures\Takeout\Google Photos\Juli")]
PREVIOUS_SCAN_CACHE = Path(r".\old_filesystem_scan.pickle")
FILESYSTEM_SCAN_SAVEPOINT = Path(r".\filesystem_scan_savepoint.pickle")
HASHES_CACHE = Path(r".\filesystem_hashes.pickle")
DEBUG_MODE = False


def main():
    check_roots_on_same_filesystem(ROOTS)
    disk_free_before = shutil.disk_usage(ROOTS[0]).free
    logger.info(f"Free disk space: {humanize.naturalsize(disk_free_before)}")
    logger.info(f"Scanning filesystem stats...")
    f = scan_filesystem(ROOTS)
    logger.info(f"Total files: {humanize.intcomma(len(f))} files, consuming {humanize.naturalsize(f['size'].sum())}")
    f = f.loc[f.duplicated(subset="size", keep=False)]  # Ignore files with unique length
    f = merge_previous_scan_hashes(f, get_cached_scan(PREVIOUS_SCAN_CACHE))
    f = reconcile_duplicate_inodes(f)
    need_hash = dd.from_pandas(f[pd.isna(f.md5)].reset_index().path, npartitions=8)
    # TODO: Perhaps this should groupby size, hash and link by group - should make free space faster
    hashes = pd.DataFrame(need_hash.apply(hasher, convert_dtype=False, meta=pd.Series(data=None, name="md5")).compute())
    # TODO:  Save hashes for future runs # hashes.to_pickle(HASHES_CACHE)
    # hard_link_md5_groups(f)
    disk_free_after = shutil.disk_usage(ROOTS[0]).free
    print(
        f"Free disk space: {humanize.naturalsize(disk_free_after)}. "
        f"Freed {humanize.naturalsize(disk_free_before - disk_free_after)}"
    )
    logger.info("Done")


def check_roots_on_same_filesystem(roots: list[Path]):
    if len({p.stat().st_dev for p in roots}) > 1:
        message = "Roots must be in same filesystem."
        logger.error(message)
        raise OSError(message)


def scan_filesystem(roots: list[Path]):  # Perhaps scan lists of files or lists of dirs?
    """
    Scans filesystem and creates dataframe of results
    """

    if DEBUG_MODE:
        logger.info("Debug mode on - loading scan values")
        try:
            return pd.read_pickle(FILESYSTEM_SCAN_SAVEPOINT)
        except (EOFError, FileNotFoundError):
            logger.info("Debug mode on - no previous scan available")
    tree = [
        {"path": path, "stats": path.stat()} for path in itertools.chain.from_iterable([p.rglob("*") for p in roots])
    ]
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
    files["md5"] = pd.NA

    files = files.set_index(keys="path")
    if DEBUG_MODE:
        files.to_pickle(FILESYSTEM_SCAN_SAVEPOINT)
    return files


def merge_previous_scan_hashes(f: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    if old.empty:
        return f
    merged = f.merge(old, how="left", left_index=True, right_index=True, suffixes=("", "_old"))
    identical = (merged["size"] == merged.size_old) & (merged.mtime == merged.mtime_old)
    merged.loc[identical, "md5"] = merged.loc[identical, "md5_old"]
    merged = merged.drop(columns=["inode_old", "size_old", "mtime_old", "links_old", "md5_old"])
    if check_dup_inode_hash(merged):  # If conflicting hashes found, abandon merge
        return f
    return merged


def check_dup_inode_hash(df: pd.DataFrame):
    d = df.loc[df.duplicated(subset="inode", keep=False)]
    d = d.loc[pd.notna(d.md5)]
    if not all(d.groupby('inode')['md5'].unique().apply(lambda a: a.size == 1)):
        return True
    return False


def get_cached_scan(path: Path) -> pd.DataFrame:
    logger.info("Reading in previous scan info...")
    try:
        old = pd.read_pickle(path)
    except (EOFError, FileNotFoundError):
        logger.info("No previous scan available")
        return pd.DataFrame()
    return old


def reconcile_inode_groups(df: pd.DataFrame):
    """
    Returns only one path of paths sharing same inode, one with a hash if available
    :param df:
    :return:
    """
    if len(df.index) == 1:
        return df
    have_hashes = pd.notna(df.md5)
    if all(~have_hashes):
        return df.iloc[0]
    if len(set(df[have_hashes].md5)) > 1:
        raise ValueError(  # TODO: maybe ignore old file in this case?
            f"Files with same inode have different hashes. You may want to rerun with no historic file.{df}"
        )
    return df.iloc[have_hashes[0]]


def reconcile_duplicate_inodes(df: pd.DataFrame):
    return df.loc[~df.sort_values(by="md5").duplicated(subset="inode", keep="first")]


def hard_link_md5_groups(df: pd.DataFrame):  # TODO: Link to path with the most links
    if df.shape[0] == 1:
        return df
    common_inode = df.inode.mode()[0]
    target_path = df.loc[df.inode == common_inode].index.values[0]
    hard_link_list = df.loc[df.inode != common_inode].index.to_list()
    hard_link_paths(hard_link_list, target_path)
    # TODO:  Rescan here?


def hard_link_paths(path_list: list[Path], target_path: Path):
    for p in path_list:
        p.unlink()
        p.hardlink_to(target_path)


def hasher(path: Path):  # TODO:  Add hashlist, hard link capabilities, and return an indexed list
    result = hashlib.md5()
    with path.open(mode="rb") as f:
        while True:
            data = f.read(512 * 1024)  # 1k * 512 byte disk sectors - benchmarks pretty well
            if not data:
                break
            result.update(data)
    return {path: result.hexdigest()}


######################################
#  OK above here
######################################

# Assure all paths sharing inodes have hashes by (a) copying existing hash from an inode in the group or
# (b) computing one hash for group and assigning to others
# Link all files not in an inode group that have MD5 that match an inode
# This should be walking through hashes of inode groups looking for members, not the other way around
# path_by_md5 = (
#     f.loc[f.inode.duplicated(), "md5"]
#     .reset_index()
#     .set_index("md5")
#     .to_dict()
#     .get("path")
# )
# hash_match = f.md5.isin(path_by_md5.keys())
# for r in f[~f.inode.duplicated(keep=False)].sort_values(by='size', ascending=False).itertuples():
#     if r.md5 in path_by_md5:
#         r.index.unlink()
#         r.path.hardlink_to(path_by_md5[r.md5])
#
# # Hash same-size files in hopes of a match; largest first to save space fastest
# hasher.get_hashes(f[f['size'].duplicated(keep=False)].loc[f.md5.isna()].sort_values(by='size').index)
# print("Waiting for hashes to complete...", flush=True, end='')
# hasher.wait()
# print("Done")
#
# fltr = f.md5.isna()
#
# merged = f.loc[fltr].merge(hashes, on="path", how="left", suffixes=("", "_hash"))
#
# merged = f.merge(hashes, on="path", how="left", suffixes=("", "_hash"))
# merged.md5.fillna(merged.md5_hash)
# # if any(pd.isna(f.md5)):
# #     f.loc[pd.isna(f.md5), ['md5', 'crc23']] = hash_list(f.loc[pd.isna(f.md5), 'path'], update_rate=1000).loc[:, ['md5', 'crc32']]
# # dog.loc[:, ['md5', 'crc32']] = hash_list(dog.loc[:, 'path']).loc[:, ['md5', 'crc32']]
# # print(dog.head())
#
# print("Linking matched MD5")
# for hg in f[f.md5.duplicated(keep=False)].groupby("md5"):
#     h = hg[1]
#     if len(set(h.inode)) > 1:
#         master_path = h.iloc[0]["path"]
#         h.iloc[1:]["path"].apply(os.unlink)
#         h.iloc[1:]["path"].apply(lambda p: os.link(master_path, p))
# print("Done")


if __name__ == "__main__":
    main()
