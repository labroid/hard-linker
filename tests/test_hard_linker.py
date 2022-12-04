from pathlib import Path
from collections import Counter
from itertools import permutations
from dataclasses import dataclass

import pandas as pd
import pytest

import src
from src.hard_linker import *

# df_hash_group = pd.DataFrame(
#     [
#         {"path": Path("a/a"), "inode": pd.NA, "size": 1, "mtime": 2, "links": 1, "md5": "abc"},
#         {"path": Path("a/b"), "inode": pd.NA, "size": 1, "mtime": 3, "links": 1, "md5": "abc"},
#         {"path": Path("a/c"), "inode": 2, "size": 1, "mtime": 4, "links": 1, "md5": "abc"},
#         {"path": Path("a/d"), "inode": 1, "size": 1, "mtime": 5, "links": 2, "md5": "abc"},
#         {"path": Path("a/e"), "inode": 1, "size": 1, "mtime": 6, "links": 2, "md5": "abd"},
#         {"path": Path("a/f"), "inode": 3, "size": 1, "mtime": 7, "links": 3, "md5": "def"},
#     ]
# ).set_index("path")
#
# df_inode_group = pd.DataFrame(
#     [
#         {"path": Path("a/a"), "inode": 1, "size": 1, "mtime": 2, "links": 1, "md5": pd.NA},
#         {"path": Path("a/b"), "inode": 1, "size": 1, "mtime": 3, "links": 1, "md5": pd.NA},
#         {"path": Path("a/c"), "inode": 1, "size": 1, "mtime": 4, "links": 1, "md5": "abc"},
#         {"path": Path("a/d"), "inode": 1, "size": 1, "mtime": 5, "links": 2, "md5": "abc"},
#         {"path": Path("a/e"), "inode": 1, "size": 1, "mtime": 6, "links": 2, "md5": "abc"},
#         {"path": Path("a/f"), "inode": 1, "size": 1, "mtime": 7, "links": 3, "md5": "def"},
#     ]
# ).set_index("path")
#
# df_new_scan = pd.DataFrame(
#     [
#         {"path": Path("a/a"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
#         {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
#         {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "links": 1, "md5": pd.NA},
#         {"path": Path("a/d"), "inode": 3, "size": 12, "mtime": 5, "links": 1, "md5": pd.NA},
#         {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 6, "links": 1, "md5": pd.NA},
#         {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "links": 1, "md5": pd.NA},
#     ]
# ).set_index("path")
#
# df_old_scan = pd.DataFrame(
#     [
#         {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "md5": "abc"},  # One copy of linked
#         {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "md5": "def"},  # Match with md5
#         {"path": Path("a/d"), "inode": 3, "size": 19, "mtime": 5, "md5": "def"},  # Different size
#         {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 1, "md5": "ghi"},  # Different mtime
#         {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "md5": pd.NA},  # Missing md5
#         {"path": Path("a/g"), "inode": 6, "size": 15, "mtime": 8, "md5": "mno"},  # Isn't in original
#     ]
# ).set_index("path")
#
# df_expected_merge = pd.DataFrame(
#     [
#         {"path": Path("a/a"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
#         {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": "abc"},
#         {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "links": 1, "md5": "def"},
#         {"path": Path("a/d"), "inode": 3, "size": 12, "mtime": 5, "links": 1, "md5": pd.NA},
#         {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 6, "links": 1, "md5": pd.NA},
#         {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "links": 1, "md5": pd.NA},
#     ]
# ).set_index("path")

baseline_text = "Baseline_text"
different_content = "Different content"
r2_different_content = "root 2 different content"


class FS:
    def __init__(self, tmp_path):
        self.r1 = tmp_path / "root1"
        self.r2 = tmp_path / "root2"
        self.r1base = self.r1 / "baseline"
        self.r1base_lnk = self.r1 / "baseline_linked"
        self.r1base_cpy = self.r1 / "baseline_copy"
        self.r1diff = self.r1 / "different"
        self.r2base = self.r2 / "baseline"
        self.r2base_lnk = self.r2 / "linked_elsewhere"
        self.r2different = self.r2 / "different"


@pytest.fixture
def fs(tmp_path):
    fs = FS(tmp_path)
    fs.r1.mkdir()
    fs.r2.mkdir()
    fs.r1base.write_text(baseline_text)
    fs.r1base_cpy.write_text(baseline_text)
    fs.r1base_lnk.hardlink_to(fs.r1base)
    fs.r1diff.write_text(different_content)
    fs.r2base.write_text(baseline_text)
    fs.r2base_lnk.hardlink_to(fs.r2base)
    fs.r2different.write_text(r2_different_content)
    return fs


# def fs_scan(tmp_path):
#     return scan_filesystem([tmp_path])


def test_scan_filesystem(fs, tmp_path):
    result = scan_filesystem([fs.r1, fs.r2])
    assert Counter([p for p in tmp_path.rglob("*") if p.is_file()]) == Counter(result.index.to_list())
    assert Counter(["inode", "size", "mtime", "links", "md5"]) == Counter(result.columns)
    assert all(pd.isna(result.md5))
    assert result.loc[fs.r1base].inode == result.loc[fs.r1base_lnk].inode
    assert result.loc[fs.r1diff, "size"] == len(different_content.encode())
    assert result.loc[fs.r2base_lnk].links == 2


class TestMergePreviousScanHashes:
    def test_normal_file_has_hash(self, fs, tmp_path):
        result = scan_filesystem([tmp_path])
        old = result.loc[[fs.r1base]].copy()
        test_hash = "abcd"
        old.loc[fs.r1base, "md5"] = test_hash
        merged = merge_previous_scan_hashes(result, old)
        assert merged.loc[fs.r1base].md5 == test_hash
        pd.testing.assert_frame_equal(result.drop(fs.r1base), merged.drop(fs.r1base))

    def test_file_no_longer_exists(self, fs, tmp_path):
        result = scan_filesystem([tmp_path])
        old = pd.DataFrame(
            [
                {
                    "path": fs.r1 / "no_longer_exists",
                    "inode": 1234,
                    "size": 10,
                    "mtime": 12345,
                    "links": 1,
                    "md5": "abcd",
                }
            ]
        ).set_index("path")
        pd.testing.assert_frame_equal(result, merge_previous_scan_hashes(result, old))

    def test_file_with_hash_time_changed(self, fs, tmp_path):
        result = scan_filesystem([tmp_path])
        old = result.loc[[fs.r1base]].copy()
        old.loc[fs.r1base, "md5"] = "abcd"
        old.loc[fs.r1base, "mtime"] = 23456
        merged = merge_previous_scan_hashes(result, old)
        pd.testing.assert_frame_equal(result, merged)

    def test_old_hash_doesnt_match_new_hash(self):
        assert False
        ...


class TestHardLinkMD5Groups:
    def test_normal(self, mocker):
        mocker.patch("src.hard_linker.hard_link_paths")
        hard_link_md5_groups(df_hash_group)
        src.hard_linker.hard_link_paths.assert_called_with(
            [Path("a/a"), Path("a/b"), Path("a/c"), Path("a/f")], Path("a/d")
        )

    def test_single_row(self):
        result = hard_link_md5_groups(df_hash_group.iloc[[1]])
        pd.testing.assert_frame_equal(result, df_hash_group.iloc[[1]])


def test_hard_link_paths(tmp_path):
    d = tmp_path / "link_test"
    d.mkdir()
    target = d / "target"
    target.write_text("Target content")
    candidate1 = d / "candidate1"
    candidate1.write_text("Source1 content")
    candidate2 = d / "candidate2"
    candidate2.write_text("Source2 content")
    assert len({x.stat().st_ino for x in [target, candidate1, candidate2]}) == 3
    hard_link_paths([candidate1, candidate2], target)
    assert len({x.stat().st_ino for x in [target, candidate1, candidate2]}) == 1


class TestCheckRootsOnSameFilesystem:
    def test_throws_exception_on_different_filesystems(self):
        with pytest.raises(OSError):
            check_roots_on_same_filesystem([Path(r"C:\Users"), Path(r"G:\My Drive")])

    def test_no_exception_on_same_filesystem(self):
        roots = [Path(r"C:\Users"), Path(r"C:\Windows")]
        try:
            check_roots_on_same_filesystem(roots)
        except Exception:
            assert False, f"Failed filesystem match on {roots}. Should have passed."


class TestReconcileDuplicateInodes:
    def test_no_duplicate_inodes(self, fs, tmp_path):
        fs.r1base_lnk.unlink()
        fs.r2base_lnk.unlink()
        result = scan_filesystem([fs.r1, fs.r2])
        pd.testing.assert_frame_equal(result, reconcile_duplicate_inodes(result))

    def test_inode_order_and_duplicate_inode_removed(self, fs, tmp_path):
        for target, twin in permutations([fs.r1base, fs.r1base_lnk], 2):
            fs_scan = scan_filesystem([fs.r1, fs.r2])
            fs_scan.loc[target, "md5"] = "abcd"
            result = reconcile_duplicate_inodes(fs_scan)
            assert result.loc[target].md5 == "abcd"
            assert result.index.isin([fs.r2base, fs.r2base_lnk]).sum() == 1  # Only one of the doublets remains
            assert result.index.isin([target, twin]).sum() == 1

    def test_both_different_hashes(self, fs, tmp_path):
        fs_scan = scan_filesystem([fs.r1, fs.r2])
        fs_scan.loc[fs.r1base, "md5"] = "abcd"
        fs_scan.loc[fs.r1base_lnk, "md5"] = "efgh"
        result = reconcile_duplicate_inodes(fs_scan)
        assert result.index.isin([fs.r1base, fs.r1base_lnk]).sum() == 1
        assert result.index.isin([fs.r2base, fs.r2base_lnk]).sum() == 1
        assert all(pd.isna(result.md5))


class TestReconcileInodeGroups:
    def test_one_record(self):
        pd.testing.assert_frame_equal(reconcile_inode_groups(df_inode_group.head(1)), df_inode_group.head(1))

    def test_no_hashes(self):
        df_no_hashes = df_inode_group.copy()
        df_no_hashes.loc[:, "md5"] = pd.NA
        pd.testing.assert_frame_equal(reconcile_inode_groups(df_no_hashes), df_no_hashes)

    def test_different_hashes(self):
        with pytest.raises(ValueError):
            reconcile_inode_groups(df_inode_group)

    def test_all_hashed(self):
        df_all_hashed = df_inode_group[df_inode_group.md5 == df_inode_group.md5.mode()[0]]
        pd.testing.assert_frame_equal(reconcile_inode_groups(df_all_hashed), df_all_hashed)

    def test_set_hashes(self):
        result = reconcile_inode_groups(df_inode_group.head(-1))
        assert len(set(result.md5)) == 1
        assert set(result.md5).pop() == df_inode_group.md5.mode()[0]


class TestFuntionality:
    def test_functionality(self):
        assert errors_on_different_filesystems
        assert consolidated_hard_link_groups
        assert unique_hashes_linked
        assert leverages_old_hashes
        assert ignores_missing_old_hashes
        assert ignores_old_hashes_with_changes
        assert warns_on_hash_change_maybe
