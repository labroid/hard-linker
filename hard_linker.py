import itertools
import multiprocessing as mp
import stat
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED, as_completed
import hashlib
from pathlib import Path
from collections import deque
from concurrent.futures._base import TimeoutError
from queue import Empty

import dask.dataframe as dd
from dask.diagnostics import ProgressBar

import humanize
import pandas as pd
from loguru import logger

# TODO: This should be in config file
ROOTS = [r"C:\Users\scott\Pictures\Takeout\Google Photos\Juli"]
PREVIOUS_SCAN_CACHE = r".\old_filesystem_scan.pickle"
FILESYSTEM_SCAN_SAVEPOINT = r".\filesystem_scan_savepoint.pickle"
HASHES_CACHE = r".\filesystem_hashes.pickle"
DEBUG_MODE = True


def main():
    # TODO:  Build a test dir for this!!!
    # TODO: Check paths are on same filesystem so they can link
    # TODO: Show disk free space
    f = scan_filesystem(ROOTS)
    f = merge_previous_scan_hashes(f)
    # f = reconcile_inode_hashes(f)  # if same inode, assure hashes are same
    # f = link_matching_hashes(f)  # Group and match, or do hashdict and link
    f = f[f.duplicated(subset="size", keep=False)]  # Keep only non-unique file sizes
    # if hashes missing...
    f_single_linked = f.drop_duplicates(subset="inode", keep="first")
    # need_hash = dd.from_pandas(f_single_linked[pd.isna(f_single_linked.md5)].reset_index().path, npartitions=8)
    # hashes = pd.DataFrame(
    #     need_hash.apply(dask_hasher, convert_dtype=False, meta=pd.Series(data=None, name="md5")).compute()
    # )
    # hashes.to_pickle(HASHES_CACHE)

    need_hash = f_single_linked[pd.isna(f_single_linked.md5)].index
    proc_count = 8
    with mp.Manager() as manager:
        qpath = mp.JoinableQueue()
        qhash = mp.JoinableQueue()
        logger.info(f"Filling job queue with {len(need_hash)} paths")
        [qpath.put(p) for p in need_hash]
        logger.info("Kicking off processes")
        processes = []
        for i in range(proc_count):
            proc = mp.Process(target=worker_hasher, args=(qpath, qhash))
            processes.append(proc)
            proc.start()
            print(proc)

        hashdict = {}
        hashdict_hit = 0
        processed = 0
        while True:
            r = qhash.get()
            if r is None:
                proc_count -= 1
                if not proc_count:
                    break
                continue
            if r["hash"] in hashdict:
                hashdict_hit += 1
            else:
                hashdict[r["hash"]] = r["path"]
            qhash.task_done()

            processed += 1
            if not processed % 1000:
                print(
                    f"Queue: {qpath.qsize()}, Hashed: {qhash.qsize()}, Processed: {processed}, Hash hits: {hashdict_hit}"
                )

        print(
            f"Finally: Queue: {qpath.qsize()}, Hashed: {qhash.qsize()}, Processed: {processed}, Hash hits: {hashdict_hit}"
        )

    # queue_target = 1000
    # hashdict = {}
    # futures = {}
    # jobs_done = 0
    # hash_dict_hits = 0
    # need_hash_paths = iter(need_hash.index)
    # with ProcessPoolExecutor() as executor:
    #     print(f"Total jobs needed: {len(need_hash)}")
    #     while True:
    #         batch = list(itertools.islice(need_hash_paths, queue_target - len(futures)))
    #         logger.info(f"New batch length: {len(batch)}")
    #         futures |= {executor.submit(hasher, p): p for p in batch}
    #         if not len(futures):
    #             break
    #         try:
    #             for future in as_completed(fs=futures, timeout=30):
    #                 jobs_done += 1
    #                 path = futures[future]
    #                 # print(path)
    #                 try:
    #                     hash_value = future.result()
    #                 except Exception as e:
    #                     logger.info(f"{path} generated exception {e}")
    #                     hash_value = ""
    #                 del futures[future]
    #                 f.loc[path, "md5"] = hash_value
    #                 if hash_value in hashdict:
    #                     # print(f"Link {path} to {hashdict[hash_value]}")
    #                     hash_dict_hits += 1
    #                     # path.unlink()
    #                     # path.hardlink_to(hashdict[hash_value])
    #                 else:
    #                     hashdict[hash_value] = path
    #         except TimeoutError as e:
    #             logger.info(f"{len(futures)} jobs in progress. Jobs done: {jobs_done}, hashdict hits: {hash_dict_hits}")

    # linkhit = 0
    # for rcount, r in enumerate(executor.map(hasher, f_single_linked.index, chunksize=100)): # TODO: Catch exceptions
    #     path = r[0]
    #     hashval = r[1]
    #     f.loc[path, 'md5'] = hashval
    #     if hashval in hashdict:
    #         print(f"link {path} to {hashdict[hashval]}")
    #         linkhit += 1
    #         # h.path.unlink()
    #         # h.path.hardlink_to(hashdict[h.md5])
    #     else:
    #         hashdict[hashval] = path
    #     if not rcount %100
    #         logger.info(f"Processed: {rcount}, Linked: {linkhit}")
    # logger.info(f"Jobs submitted")

    # TODO:  Assign hashes to linked files that were skipped earlier
    # TODO: Show disk free space and delta
    logger.info("Done")


def scan_filesystem(roots):
    """
    Scans filesystem and creates dataframe of results
    """
    logger.info(f"Scanning filesystem stats...")
    if DEBUG_MODE:
        logger.info("Debug mode on - loading scan values")
        try:
            return pd.read_pickle(FILESYSTEM_SCAN_SAVEPOINT)
        except (EOFError, FileNotFoundError):
            logger.info("Debug mode on - no previous scan available")
    tree = [
        {"path": path, "stats": path.stat()}
        for path in itertools.chain.from_iterable([Path(p).rglob("*") for p in roots])
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
    # files = tree[tree.stats.apply(lambda p: stat.S_ISREG(p[stat.ST_MODE]))]
    logger.info(
        f"Total files: {humanize.intcomma(len(files))} files, consuming {humanize.naturalsize(files['size'].sum())}"
    )
    files = files.set_index(keys="path")
    if DEBUG_MODE:
        files.to_pickle(FILESYSTEM_SCAN_SAVEPOINT)
    return files


def merge_previous_scan_hashes(f):
    logger.info("Reading in previous scan info...")
    try:
        old = pd.read_pickle(PREVIOUS_SCAN_CACHE)
    except (EOFError, FileNotFoundError):
        logger.info("No previous scan available")
        f["md5"] = pd.NA
        return f
    logger.info("Merging new and old scans...")
    f = f.merge(old, how="left", left_index=True, right_index=True, suffixes=("", "_old"))
    f.loc[(f["size"] != f.size_old) | (f.mtime != f.mtime_old), "md5"] = pd.NA
    f = f.drop(columns=["inode_old", "size_old", "mtime_old"])
    return f


def hasher(path: Path):
    result = hashlib.md5()
    with path.open(mode="rb") as f:
        while True:
            data = f.read(512 * 1024)  # 1k * 512 byte disk sectors - benchmarks pretty well
            if not data:
                break
            result.update(data)
    return result.hexdigest()


def dask_hasher(path):  # TODO:  Add hashlist, hard link capabilities, and return an indexed list
    result = hashlib.md5()
    with path.open(mode="rb") as f:
        while True:
            data = f.read(512 * 1024)  # 1k * 512 byte disk sectors - benchmarks pretty well
            if not data:
                break
            result.update(data)
    return {path: result.hexdigest()}


def worker_hasher(qpath, qhash):
    while True:
        try:
            path = qpath.get(block=True, timeout=5)
        except Empty:
            qhash.put_nowait(None)  # Add sentry
            logger.info("Worker suicide...")
            break
        result = hashlib.md5()
        with path.open(mode="rb") as f:
            while True:
                data = f.read(512 * 1024)  # 1k * 512 byte disk sectors - benchmarks pretty well
                if not data:
                    break
                result.update(data)
        qhash.put_nowait({"path": path, "hash": result.hexdigest()})
        qpath.task_done()


######################################
#  OK above here
######################################

# Assure all paths sharing inodes have hashes by (a) copying existing hash from an inode in the group or
# (b) computing one hash for group and assigning to others
# def hasher_inode_callback(hashlist):
#     for h in hashlist:
#         f.loc[h["path"], ["md5", "crc32"]] = h["md5"], h["crc32"]
#
#     start = dt.now()
#     print("Processing inode groups...")
#     for group in (
#             f[f.inode.duplicated(keep=False)]
#                     .sort_values(by="size", ascending=False)
#                     .groupby(by="inode", sort=False)
#     ):
#         g = group[1]
#         assert (
#                 len(set(g.md5[g.md5.notna()])) <= 1
#         ), f"Whoa! Shared inode with non-unique hashes {g}"
#         if any(g.md5.isna()):
#             hashes = g.loc[g.md5.notna(), ["md5", "crc32"]]
#             if len(hashes):
#                 f.loc[g.index, ["md5", "crc32"]] = hashes.loc[:, ["md5", "crc32"]].iloc[0]
#                 continue
#             hasher.get_inode_hashes(g.index)
#     print("Waiting unfinished hashes...")
#     hasher.wait()
#     print(dt.now() - start)


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
