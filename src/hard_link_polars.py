import hashlib
import itertools
import multiprocessing
import os
import shutil
import stat
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Tuple
from collections import Counter
import traceback
from more_itertools import ichunked

import humanize
import polars as pl
from loguru import logger
from mpire import WorkerPool


# TODO: This should be in config file or cmd line args
ROOTS = [Path(r"F:\TakeoutFeb2025")]
HASH_SAVE_PATH = ROOTS[0] / "filesystem_scan_hash_savepoint.parquet"
COMPUTE_ALL_HASHES = True  # True means to compute hash of even unique-length files


def main():
    check_roots_on_same_filesystem(ROOTS)
    log_disk_stats = DiskStats()
    log_disk_stats.begin()
    f = scan_filesystem(ROOTS)
    f = merge_previous_scan_hashes(f, get_cached_scan(HASH_SAVE_PATH))
    f = synchronize_md5_within_inodes(f)
    f = synchronize_inodes_by_md5(f)
    compute_hashes_and_link(f, HASH_SAVE_PATH, compute_all_hashes=COMPUTE_ALL_HASHES)
    log_disk_stats.end()

    logger.info("Done")


class DiskStats:
    def __init__(self):
        self.disk_free_before = None

    def begin(self):
        self.disk_free_before = shutil.disk_usage(ROOTS[0]).free
        logger.info(f"Free disk space: {humanize.naturalsize(self.disk_free_before)}")
        return

    def end(self):
        disk_free_after = shutil.disk_usage(ROOTS[0]).free
        print(
            f"Free disk space: {humanize.naturalsize(disk_free_after)}. "
            f"Freed {humanize.naturalsize(disk_free_after - self.disk_free_before)}"
        )


def check_roots_on_same_filesystem(roots: list[Path]):
    if len({p.stat().st_dev for p in roots}) > 1:
        message = "Roots must be in same filesystem."
        logger.error(message)
        raise OSError(message)


def groom_scan(f: pl.DataFrame) -> pl.DataFrame:
    """
    Filter dataframe to include only regular files.

    Args:
        f: Polars DataFrame with 'path' and 'stat' columns

    Returns:
        Filtered DataFrame containing only regular files and columns inode, size, mtime, md5 in columns
    """
    logger.info("Filtering out non-regular files...")

    # Create a mask for regular files using stat.S_ISREG
    is_regular_file = f.with_columns(
        pl.col("stat").map_elements(
            lambda s: stat.S_ISREG(s.st_mode),
            return_dtype=pl.Boolean
        ).alias("is_regular")
    )

    # Filter to keep only regular files
    regular_files = is_regular_file.filter(pl.col("is_regular"))

    # Add columns needed for further processing
    regular_files = regular_files.with_columns([
        pl.col("stat").map_elements(lambda s: s.st_ino, return_dtype=pl.UInt64).alias("inode"),
        pl.col("stat").map_elements(lambda s: s.st_size, return_dtype=pl.Int64).alias("size"),
        pl.col("stat").map_elements(lambda s: s.st_mtime, return_dtype=pl.Float64).alias("mtime"),
    ])

    regular_files = regular_files.with_columns(pl.lit(None).cast(pl.Utf8).alias('md5'))

    # Drop the temporary columns
    regular_files = regular_files.drop(["is_regular", 'stat'])

    logger.info(
        f"Total files: {humanize.intcomma(regular_files.height)} files, "
        f"consuming {humanize.naturalsize(regular_files.select(pl.sum('size')).item())}"
    )

    return regular_files


def scan_filesystem(roots: list[Path]):  # Perhaps scan lists of files or lists of dirs?
    """
    Scans filesystem and creates dataframe of files with inode, size, mtime, md5 indexed by path
    """
    logger.info("Walking roots to get Path info...")
    files = pl.from_records(
        [str(path) for path in itertools.chain.from_iterable([p.rglob("*") for p in roots])],
        schema=['path']
    )
    logger.info("Getting stat for each path...")
    files = files.with_columns(
        pl.col("path").map_elements(lambda p: os.stat(p), return_dtype=pl.Object).alias('stat')
    )
    logger.info("Stats loaded into dataframe")
    files = groom_scan(files)
    return files


def merge_previous_scan_hashes(f: pl.DataFrame, old: pl.DataFrame) -> pl.DataFrame:
    if old.is_empty():
        return f

    logger.info("Merging previous scan hashes...")
    old_subset = old.select(["path", "mtime", "size", "md5"])
    merged = f.join(old_subset, on=["path", "mtime", "size"], how="left", suffix="_old")

    # Check for mismatched MD5 values and issue warning
    mismatched = merged.filter(
        (pl.col("md5").is_not_null()) &
        (pl.col("md5_old").is_not_null()) &
        (pl.col("md5") != pl.col("md5_old"))
    )
    if not mismatched.is_empty():
        paths = mismatched.select("path").to_series().to_list()
        logger.warning(f"Found {len(paths)} files with mismatched MD5 hashes from previous scan")
        for path in paths[:5]:  # Log first 5 examples
            logger.warning(f"MD5 mismatch for {path}")
        if len(paths) > 5:
            logger.warning(f"... and {len(paths) - 5} more files with mismatched hashes")

    # Use old MD5 when current is null
    merged = merged.with_columns(
        pl.when(pl.col("md5").is_null() & pl.col("md5_old").is_not_null())
        .then(pl.col("md5_old"))
        .otherwise(pl.col("md5"))
        .alias("md5")
    )

    # Drop unnecessary columns
    merged = merged.drop([col for col in merged.columns if col.endswith("_old")])

    return merged


def get_cached_scan(path: Path) -> pl.DataFrame:
    logger.info("Reading in previous scan info...")
    try:
        old = pl.read_parquet(path)
    except FileNotFoundError as err:
        logger.info(f"No previous scan available: {err}")
        return pl.DataFrame()
    return old

def synchronize_md5_within_inodes(f: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure MD5 hashes are consistent within each inode group.

    Args:
        f: Polars DataFrame with file information

    Returns:
        DataFrame with synchronized MD5 hashes within inode groups
    """
    # Group by inode and ensure MD5 hashes are consistent within each group
    inode_groups = f.group_by("inode").agg([
        pl.col("path").alias("paths"),
        pl.col("md5").alias("md5s")
    ])

    # Process each inode group
    result_df = f.clone()

    for group in inode_groups.filter(pl.col('paths').count().over('inode') > 1).iter_rows(named=True):
        inode = group["inode"]
        paths = group["paths"]
        md5s = group["md5s"]

        # Filter out None values
        valid_md5s = [md5 for md5 in md5s if md5 is not None]

        if not valid_md5s:
            # No valid MD5s in this group, nothing to do
            continue

        if len(set(valid_md5s)) > 1:
            # Inconsistent MD5s within the same inode group - this shouldn't happen
            logger.warning(f"Inconsistent MD5 hashes for inode {inode}: {set(valid_md5s)}")
            logger.warning(f"Affected paths: {paths}")

            # Reset all MD5s in this group to None
            result_df = result_df.with_columns(
                pl.when(pl.col("inode") == inode)
                .then(pl.lit(None).cast(pl.Utf8))
                .otherwise(pl.col("md5"))
                .alias("md5")
            )
        elif None in md5s:
            # Propagate the consistent MD5 to all files in the group
            common_md5 = valid_md5s[0]
            result_df = result_df.with_columns(
                pl.when(pl.col("inode") == inode)
                .then(pl.lit(common_md5))
                .otherwise(pl.col("md5"))
                .alias("md5")
            )
            logger.debug(f"Propagated MD5 {common_md5} to all files with inode {inode}")

    return result_df


def synchronize_inodes_by_md5(result_df: pl.DataFrame) -> pl.DataFrame:
    """
    Ensure all files with the same MD5 share the same inode.

    Args:
        result_df: Polars DataFrame with synchronized MD5 hashes

    Returns:
        DataFrame with synchronized inodes
    """
    # Filter for files with known hashes
    known_hashes = result_df.filter(pl.col("md5").is_not_null())

    if known_hashes.is_empty():
        logger.info("No files with known hashes found")
        return result_df

    # Group by MD5 hash
    md5_groups = known_hashes.group_by("md5").agg([
        pl.col("path").alias("paths"),
        pl.col("inode").alias("inodes"),
        pl.col("size").first().alias("size")
    ])

    # Filter for groups with more than one file and more than one inode
    duplicate_groups = md5_groups.filter(
        (pl.col("paths").list.len() > 1) &
        (pl.col("inodes").list.unique().list.len() > 1)
    )

    if duplicate_groups.is_empty():
        logger.info("No MD5 groups with different inodes found")
        return result_df

    logger.info(f"Found {duplicate_groups.height} MD5 groups with different inodes")

    # Process each MD5 group
    for group in duplicate_groups.iter_rows(named=True):
        paths = group["paths"]
        inodes = group["inodes"]

        most_common_inode = Counter(inodes).most_common(1)[0][0]

        # Find a source path with the most common inode
        source_path = None
        for path, inode in zip(paths, inodes):
            if inode == most_common_inode:
                source_path = path
                break

        if not source_path:
            logger.error(f"No source path found for inode {most_common_inode}")
            continue

        # Hard link all other files to the source
        for target_path in paths:
            if target_path == source_path:
                continue

            target_inode = next(i for p, i in zip(paths, inodes) if p == target_path)

            if target_inode == most_common_inode:
                continue

            # Skip if source and target are already the same file
            try:
                if os.path.samefile(source_path, target_path):
                    continue
            except OSError:
                pass

            # Create hard link
            try:
                os.unlink(target_path)
                os.link(source_path, target_path)

                # Update the inode in the DataFrame
                result_df = result_df.with_columns(
                    pl.when(pl.col("path") == target_path)
                    .then(pl.lit(most_common_inode))
                    .otherwise(pl.col("inode"))
                    .alias("inode")
                )
                # logger.debug(f"Created hard link: {source_path} -> {target_path}")
            except OSError as e:
                logger.error(f"Failed to create hard link from {source_path} to {target_path}: {e}")

    logger.info(f"Done synchronizing inodes by md5")
    return result_df

def compute_file_hash(file_path: str) -> Tuple[str, str | None]:
    """
    Compute MD5 hash for a single file.

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (path, md5_hash)
    """
    try:
        with open(file_path, 'rb') as f:
            md5_hash = hashlib.md5()
            for chunk in iter(lambda: f.read(512 * 1024), b''):  # 1k * 512 byte disk sectors - benchmarks pretty well
                md5_hash.update(chunk)
        return file_path, md5_hash.hexdigest()
    except (IOError, OSError) as e:
        logger.error(f"Error hashing {file_path}: {e}")
        return file_path, None


def compute_hashes_and_link(f: pl.DataFrame, hash_save_path: Path, compute_all_hashes=True):
    """
    Compute MD5 hashes for files without hashes using mpire for parallelization.
    Create hard links for identical files as they're discovered.
    Periodically save the updated DataFrame.
    Windows-compatible version without shared_objects.

    Args:
        f: Polars DataFrame with file information
        hash_save_path: Path to save the hash information
        compute_all_hashes: Whether to compute hashes for all files or only those with sizes that match other files
    """
    # Initialize hash_to_path dict
    hash_to_path_map = f.filter(pl.col("md5").is_not_null()).group_by("md5").agg(
        pl.col("path").first().alias("first_path")
    )
    hash_to_path = dict(zip(hash_to_path_map['md5'].to_list(), hash_to_path_map['first_path'].to_list()))

    # Identify files that need hashing
    if compute_all_hashes:
        to_hash = f.filter(pl.col("md5").is_null()).sort("size").filter(pl.col('md5').is_null())['path'].to_list()
    else:
        # Group by size, filter for groups with more than 1 item
        size_groups = (f.group_by("size")
                      .agg([
                           pl.col("path"),
                           pl.col('md5'),
                           pl.len().alias('count')
                       ])
                      .filter(pl.col("count") > 1)  # Only keep groups with more than 1 item
                      .sort("size", descending=True)
                      )
        to_hash = size_groups.explode(['path', 'md5']).filter(pl.col('md5').is_null())['path'].to_list()

    if not to_hash:
        logger.info("No files need hashing")
        return f

    logger.info(f"Computing hashes for {humanize.intcomma(len(to_hash))} files...")

    # Shared state for worker processes
    result_df = f.clone()
    last_save_time = time.time()

    # Define a callback function to process results
    def process_result(result):
        nonlocal result_df, last_save_time, hash_to_path

        path, md5 = result

        if md5 is not None:
            # Update the dataframe with the new hash
            result_df = result_df.with_columns(
                pl.when(pl.col("path") == path)
                .then(pl.lit(md5))
                .otherwise(pl.col("md5"))
                .alias("md5")
            )

            # Check if we've seen this hash before
            if md5 in hash_to_path:
                source_path = hash_to_path[md5]

                # Skip if source and target are already the same file
                try:
                    if os.path.samefile(source_path, path):
                        return
                except OSError:
                    pass

                # Create hard link
                try:
                    # Remove target and create hard link
                    os.unlink(path)
                    os.link(source_path, path)

                    # Get the inode of the source file
                    source_inode = result_df.filter(pl.col("path") == source_path).select("inode").item()

                    # Update the inode in the DataFrame for the target file
                    result_df = result_df.with_columns(
                        pl.when(pl.col("path") == path)
                        .then(pl.lit(source_inode))
                        .otherwise(pl.col("inode"))
                        .alias("inode")
                    )

                    # logger.debug(f"Created hard link: {source_path} -> {path}")
                except OSError as err:
                    logger.error(f"Failed to create hard link from {source_path} to {path}: {err}")
            else:
                # First time seeing this hash
                hash_to_path[md5] = path

        # Check if we should save the dataframe
        current_time = time.time()
        if current_time - last_save_time > 300:  # 5 minutes
            save_dataframe(result_df, hash_save_path)
            last_save_time = current_time

    # Process files with mpire WorkerPool
    num_cores = multiprocessing.cpu_count()

    try:
        # Use mpire without shared_objects for Windows compatibility
        with WorkerPool(n_jobs=num_cores) as pool:
            results = pool.imap_unordered(compute_file_hash, to_hash, progress_bar=True)

            # Process each result
            for result in results:
                process_result(result)

    except Exception as e:
        logger.error(f"Error in hash computation: {e}")
        traceback.print_exc()
        # Save what we have so far
        save_dataframe(result_df, hash_save_path)
        raise

    # Final save
    save_dataframe(result_df, hash_save_path)
    return result_df

def save_dataframe(df: pl.DataFrame, save_path: Path) -> None:
    """
    Save the DataFrame to the specified path.

    Args:
        df: Polars DataFrame to save
        save_path: Path to save the DataFrame
    """
    logger.info(f"Saving hash information to {save_path}")

    # Create parent directory if it doesn't exist
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Save only rows with MD5 hashes
    df.filter(pl.col("md5").is_not_null()).write_parquet(save_path)

    logger.info(f"Saved {df.filter(pl.col('md5').is_not_null()).height} entries to {save_path}")


if __name__ == "__main__":
    main()
