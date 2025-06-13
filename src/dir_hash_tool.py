import hashlib
import itertools
import multiprocessing
import os
import shutil
import stat
import sys
import time
from pathlib import Path
from typing import Annotated, Optional, Tuple, Optional, List
from collections import Counter, deque
import traceback
from enum import Enum, auto
from unittest import case

import humanize
import polars as pl
from loguru import logger
from mpire import WorkerPool
import typer

# TODO: Review whole file for logging levels

app = typer.Typer(help="File system hashing and deduplication tool")

from enum import StrEnum


class Command(StrEnum):
    HASH = 'hash'
    DEDUPE = 'dedupe'
    PURGE = 'purge'


# Common options for commands
class CommonOptions:
    def __init__(
        self,
        verbose: bool = False,
        quiet: bool = False,
        dry_run: bool = False,
        refresh_cache: bool = False,
        skip_unique_length_files: bool = False,
    ):
        self.verbose = verbose
        self.quiet = quiet
        self.dry_run = dry_run
        self.refresh_cache = refresh_cache
        self.skip_unique_length_files = skip_unique_length_files

# Define common options using Annotated
RootArg = Annotated[str, typer.Argument(..., help="Root path to scan")]
VerboseOption = Annotated[bool, typer.Option(False, "--verbose", "-v", help="Verbose output")]
QuietOption = Annotated[bool, typer.Option(False, "--quiet", "-q", help="Quiet output")]
DryRunOption = Annotated[bool, typer.Option(False, "--dry-run", "-n", help="Dry run output")]
RefreshCacheOption = Annotated[bool, typer.Option(False, "--refresh-cache", "-r", help="Clear cache and rehash all files in root")]
SkipUniqueOption = Annotated[bool, typer.Option(False, "--skip-unique-length-file", "-s", help="Skip files with unique length")]
OutputOption = Annotated[Optional[str], typer.Option(None, "--output", "-o", help="Specify output hash file name")]
InputOption = Annotated[str, typer.Option(..., "--input", "-i", help="Path to file with data used to target root files for purge")]


@app.command(Command.HASH.value)
def hash_files(
    root: RootArg,
    verbose: VerboseOption = False,
    quiet: QuietOption = False ,
    dry_run: DryRunOption = False,
    refresh_cache: RefreshCacheOption = False,
    skip_unique_length_files: SkipUniqueOption = False,
):
    command_name = Command.HASH.value
    process_files(
        root=root,
        verbose=verbose,
        quiet=quiet,
        dry_run=dry_run,
        refresh_cache=refresh_cache,
        skip_unique_length_files=skip_unique_length_files,
        hash_only=True
    )

@app.command(Command.DEDUPE.value)
def dedupe_files(
    root: RootArg,
    verbose: VerboseOption = False,
    quiet: QuietOption = False,
    dry_run: DryRunOption = False,
    refresh_cache: RefreshCacheOption = False,
    skip_unique_length_files: SkipUniqueOption = False,
):
    """Hash files and create hard links for duplicates as you go"""
    output_path = Path(output) if output else Path(root) / generate_fs_info_filename(root)

    # Pass options directly to process_files
    process_files(
        root=root,
        verbose=verbose,
        quiet=quiet,
        dry_run=dry_run,
        refresh_cache=refresh_cache,
        skip_unique_length_files=skip_unique_length_files,
        hash_only=False
    )
    process_files(root, Path(output_path), options, hash_only=False)


@app.command(Command.PURGE.value)
def purge_files(
    root: RootArg,
    purge_reference_data: InputOption,
    verbose: VerboseOption = False,
    quiet: QuietOption = False,
    dry_run: DryRunOption = False,
):
    """Delete files matching hashes in a specified fs-info file."""
    configure_logging(verbose, quiet)

    input_path = Path(purge_reference_data)
    if not input_path.exists():
        logger.error(f"Input file {input_path} does not exist")
        raise typer.Exit(1)

    output_path = Path(root) / generate_fs_info_filename(root)

    logger.info(f"Purging files in {root} matching hashes in {input_path}")

    if dry_run:
        logger.info("Dry run mode - no files will be deleted")
        return

    # Implementation for purge command would go here
    logger.error("Purge command not yet implemented")
    raise typer.Exit(1)

def process_files(
    root: str,
    verbose: bool = False,
    quiet: bool = False,
    dry_run: bool = False,
    refresh_cache: bool = False,
    skip_unique_length_files: bool = False,
    hash_only: bool = True
):
    """
    Common processing logic for both hash and dedupe commands.

    Args:
        root: Root path to scan
        verbose: Enable verbose output
        quiet: Suppress all non-error output
        dry_run: Show what would be done without making changes
        refresh_cache: Clear cache and rehash all files in root
        skip_unique_length_files: Skip files with unique length
        hash_only: Whether to only hash files (True) or also create hard links (False)
    """
    configure_logging(verbose, quiet)

    output_path = Path(root) / generate_fs_info_filename(root)

    logger.info(f"Scanning files in {root}, saving to {output_path}")
    fs_data = scan_directory_with_mpire(root_path=root)

    if not refresh_cache:
        fs_data = merge_cached_scan(fs_data, root)

    needing_hash = fs_data.filter(pl.col('md5').is_null())

    if dry_run:
        logger.info(
            f"Dry run. Files needing hashing: {len(needing_hash)} "
            f"a total of {humanize.naturalsize(needing_hash['size'].sum())} bytes."
        )
        return

    action = HashAction.HASH if hash_only else HashAction.HARDLINK

    compute_hashes(
        to_hash=needing_hash.select('path').to_series().to_list(),
        f=fs_data,
        hash_save_path=output_path,
        action=action,
        skip_unique_lengths=skip_unique_length_files
    )

    logger.info("Processing complete")


    logger.info("Processing complete")


    logger.info("Processing complete")

def configure_logging(verbose, quiet):
    """Configure logging based on verbosity settings."""
    log_levels = {
        (True, False): "DEBUG",  # verbose=True, quiet=False
        (False, True): "WARNING",  # verbose=False, quiet=True
    }
    log_level = log_levels.get((verbose, quiet), "INFO")
    logger.remove()
    logger.add(sys.stderr, level=log_level)


def generate_fs_info_filename(directory_path):
    return f"fs_info-{str(Path(directory_path).absolute()).replace(':', '').replace('\\', '-').replace('/', '-')}.parquet"


# class DiskStats:
#     def __init__(self):
#         self.disk_free_before = None
#
#     def begin(self):
#         self.disk_free_before = shutil.disk_usage(ROOTS[0]).free
#         logger.info(f"Free disk space: {humanize.naturalsize(self.disk_free_before)}")
#         return
#
#     def end(self):
#         disk_free_after = shutil.disk_usage(ROOTS[0]).free
#         print(
#             f"Free disk space: {humanize.naturalsize(disk_free_after)}. "
#             f"Freed {humanize.naturalsize(disk_free_after - self.disk_free_before)}"
#         )


# def main():
#     log_disk_stats = DiskStats()
#     log_disk_stats.begin()
#     f = scan_filesystem(ROOTS)
#     f = merge_previous_scan_hashes(f, get_cached_scan(HASH_SAVE_PATH))
#     f = synchronize_md5_within_inodes(f)
#     f = synchronize_inodes_by_md5(f)
#     compute_hashes_and_link(f, HASH_SAVE_PATH, compute_all_hashes=COMPUTE_ALL_HASHES)
#     log_disk_stats.end()
#
#     logger.info("Done")


# log_disk_stats = DiskStats()


# def groom_scan(f: pl.DataFrame) -> pl.DataFrame:
#     """
#     Filter dataframe to include only regular files.
#
#     Args:
#         f: Polars DataFrame with 'path' and 'stat' columns
#
#     Returns:
#         Filtered DataFrame containing only regular files and columns inode, size, mtime, md5 in columns
#     """
#     logger.info("Filtering out non-regular files...")
#
#     # Create a mask for regular files using stat.S_ISREG
#     is_regular_file = f.with_columns(
#         pl.col("stat").map_elements(
#             lambda s: stat.S_ISREG(s.st_mode),
#             return_dtype=pl.Boolean
#         ).alias("is_regular")
#     )
#
#     # Filter to keep only regular files
#     regular_files = is_regular_file.filter(pl.col("is_regular"))
#
#     # Add columns needed for further processing
#     regular_files = regular_files.with_columns([
#         pl.col("stat").map_elements(lambda s: s.st_ino, return_dtype=pl.UInt64).alias("inode"),
#         pl.col("stat").map_elements(lambda s: s.st_size, return_dtype=pl.Int64).alias("size"),
#         pl.col("stat").map_elements(lambda s: s.st_mtime, return_dtype=pl.Float64).alias("mtime"),
#     ])
#
#     regular_files = regular_files.with_columns(pl.lit(None).cast(pl.Utf8).alias('md5'))
#
#     # Drop the temporary columns
#     regular_files = regular_files.drop(["is_regular", 'stat'])
#
#     logger.info(
#         f"Total files: {humanize.intcomma(regular_files.height)} files, "
#         f"consuming {humanize.naturalsize(regular_files.select(pl.sum('size')).item())}"
#     )
#
#     return regular_files
#
# def scan_filesystem(path):
#     """
#     Scans filesystem and creates dataframe of files with inode, size, mtime, md5 indexed by path
#     """
#     logger.info("Walking roots to get Path info...")
#     files = pl.from_records(
#         [str(path) for path in itertools.chain.from_iterable([p.rglob("*") for p in roots])],
#         schema=['path']
#     )
#     logger.info("Getting stat for each path...")
#     files = files.with_columns(
#         pl.col("path").map_elements(lambda p: os.stat(p), return_dtype=pl.Object).alias('stat')
#     )
#     logger.info("Stats loaded into dataframe")
#     files = groom_scan(files)
#     return files


def scan_single_directory(directory_path):
    """
    Scan a single directory (non-recursive) and return file information.
    This function is designed to be called by worker processes.

    Returns a dict of lists instead of a list of dicts for better performance.
    """
    # Initialize column data structures
    paths = []
    sizes = []
    mtimes = []
    inodes = []

    try:
        with os.scandir(directory_path) as entries:
            for entry in entries:
                try:
                    if entry.is_file(follow_symlinks=False):
                        entry_stat = entry.stat()
                        paths.append(os.path.abspath(entry.path))
                        sizes.append(entry_stat.st_size)
                        mtimes.append(entry_stat.st_mtime)
                        inodes.append(entry_stat.st_ino)
                except (PermissionError, FileNotFoundError) as err:
                    logger.warning(f"Problem getting file stats: {entry}, error: {err}")
    except (PermissionError, FileNotFoundError):
        pass

    # Return as a dict of lists
    return {
        'path': paths,
        'size': sizes,
        'mtime': mtimes,
        'inode': inodes
    }


def scan_directory(root_path, max_workers=None, use_progress_bar=True):
    """
    Recursively scan a directory using os.scandir() with mpire
    and return file information as a Polars DataFrame.

    Args:
        root_path: Path to the directory to scan
        max_workers: Maximum number of worker processes (default: number of CPU cores)
        use_progress_bar: Whether to show a progress bar

    Returns:
        A Polars DataFrame with columns: path, size, mtime, inode
    """
    logger.info("Collecting dirs")
    # First, collect all directory paths using single-threaded BFS
    directories = []
    directory_queue = deque([root_path])

    while directory_queue:
        current_dir = directory_queue.popleft()
        directories.append(current_dir)

        try:
            with os.scandir(current_dir) as entries:
                directory_queue.extend([
                    entry.path for entry in entries
                    if entry.is_dir(follow_symlinks=False)
                ])
        except (PermissionError, FileNotFoundError) as err:
            logger.warning(f"Problem getting node stats: error: {err}")

    # Initialize result column lists
    all_paths = []
    all_sizes = []
    all_mtimes = []
    all_inodes = []

    logger.info("Loading worker pool")
    # Process directories in parallel using mpire
    with WorkerPool(n_jobs=max_workers) as pool:
        results_iter = pool.imap_unordered(
            scan_single_directory,
            directories,
            progress_bar=use_progress_bar
        )

        # Collect results as they complete
        logger.info("Starting to collect results")
        for result in results_iter:
            all_paths.extend(result['path'])
            all_sizes.extend(result['size'])
            all_mtimes.extend(result['mtime'])
            all_inodes.extend(result['inode'])

    logger.info("Done collecting results - creating dataframe")
    # Create Polars DataFrame from column data
    if not all_paths:
        # Return empty DataFrame with correct schema
        return pl.DataFrame({
            'path': [],
            'size': [],
            'mtime': [],
            'inode': [],
            'md5': None,
        })

    # Create DataFrame directly from the column data
    df = pl.DataFrame({
        'path': all_paths,
        'size': all_sizes,
        'mtime': all_mtimes,
        'inode': all_inodes,
        'md5': None,
    })

    # # Convert mtime from timestamp to datetime
    # df = df.with_columns(
    #     pl.col('mtime').map_elements(lambda ts: datetime.datetime.fromtimestamp(ts))
    # )
    logger.info("Done")
    return df

def merge_cached_scan(f: pl.DataFrame, path: str):
    cached_scan = get_cached_scan(path)
    f = merge_known_md5s(f, cached_scan)
    f = synchronize_md5_within_inodes(f)
    f = synchronize_inodes_by_md5(f)
    return f


def merge_known_md5s(f: pl.DataFrame, cached: pl.DataFrame) -> pl.DataFrame:
    if cached.is_empty():
        return f

    logger.info("Merging previous scan hashes...")
    old_subset = cached.select(["path", "mtime", "size", "md5"])
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
        logger.warning(f"Clearing MD5 for all files where path, mtime, size match but MD5 differs")
        mismatched['md5_old'] = None

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


def get_cached_scan(path: str) -> pl.DataFrame:
    logger.info("Reading in previous scan info...")
    try:
        old = pl.read_parquet(generate_fs_info_filename(path))
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

            # Create hard link TODO: This should not happen if only hashing - if purging we must group linked files and only purge first one
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


def compute_hashes_and_process(f: pl.DataFrame, skip_unique_lengths=False, hash_only=False, hash_save_path: Path=None, ):
    to_hash = get_paths_needing_hash(f, skip_unique_lengths)
    hash_to_path = init_hash_to_path(f)
    compute_hashes(to_hash, hash_to_path)

def get_paths_needing_hash(f: pl.DataFrame, skip_unique_lengths=False) -> list[str]:
    logger.info("Generating list of paths needing hash")
    if skip_unique_lengths:
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
    else:
        to_hash = f.filter(pl.col("md5").is_null()).sort("size").filter(pl.col('md5').is_null())['path'].to_list()

    if not to_hash:
        logger.info("No files need hashing")
    return to_hash

def init_hash_to_path(f: pl.DataFrame) -> dict[str, str]:
    logger.info("Initializing hash: path dict")
    hash_to_path_map = f.filter(pl.col("md5").is_not_null()).group_by("md5").agg(
        pl.col("path").first().alias("first_path")
    )
    return dict(zip(hash_to_path_map['md5'].to_list(), hash_to_path_map['first_path'].to_list()))

def compute_hashes(to_hash: list[str], f: pl.DataFrame, action: HashAction) -> None:
# TODO: This must get ctx = typer.context somehow
    logger.info(f"Computing hashes for {humanize.intcomma(len(to_hash))} files...")

    # Shared state for worker processes
    result_df = f.clone()
    last_save_time = time.time()
    hash_to_path = init_hash_to_path(f)

    # Define a callback function to process results
    def process_result(result):
        nonlocal result_df, last_save_time, hash_to_path

        path, md5 = result

        if md5 is None:
            logger.warning(f"Hashing {path} failed")
            return
        # Update the dataframe with the new hash
        result_df = result_df.with_columns(
            pl.when(pl.col("path") == path)
            .then(pl.lit(md5))
            .otherwise(pl.col("md5"))
            .alias("md5")
        )

        if ctx.info_name == Command.HASH.value:
            return

        if ctx.info_name == Command.REMOVE.value:
            # TODO: Implement removal function
            pass

        if ctx.info_name == Command.HARDLINK.value:

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
        return


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


# if __name__ == "__main__":
#     main()
def main():
    app()


if __name__ == "__main__":
    main()
