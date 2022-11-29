from pathlib import Path
from collections import Counter

import pandas as pd
import pytest

import src
from src.hard_linker import *

df_hash_group = pd.DataFrame(
    [
        {"path": Path("a/a"), "inode": pd.NA, "size": 1, "mtime": 2, "links": 1, "md5": "abc"},
        {"path": Path("a/b"), "inode": pd.NA, "size": 1, "mtime": 3, "links": 1, "md5": "abc"},
        {"path": Path("a/c"), "inode": 2, "size": 1, "mtime": 4, "links": 1, "md5": "abc"},
        {"path": Path("a/d"), "inode": 1, "size": 1, "mtime": 5, "links": 2, "md5": "abc"},
        {"path": Path("a/e"), "inode": 1, "size": 1, "mtime": 6, "links": 2, "md5": "abd"},
        {"path": Path("a/f"), "inode": 3, "size": 1, "mtime": 7, "links": 3, "md5": "def"},
    ]
).set_index("path")

df_inode_group = pd.DataFrame(
    [
        {"path": Path("a/a"), "inode": 1, "size": 1, "mtime": 2, "links": 1, "md5": pd.NA},
        {"path": Path("a/b"), "inode": 1, "size": 1, "mtime": 3, "links": 1, "md5": pd.NA},
        {"path": Path("a/c"), "inode": 1, "size": 1, "mtime": 4, "links": 1, "md5": "abc"},
        {"path": Path("a/d"), "inode": 1, "size": 1, "mtime": 5, "links": 2, "md5": "abc"},
        {"path": Path("a/e"), "inode": 1, "size": 1, "mtime": 6, "links": 2, "md5": "abc"},
        {"path": Path("a/f"), "inode": 1, "size": 1, "mtime": 7, "links": 3, "md5": "def"},
    ]
).set_index("path")

df_new_scan = pd.DataFrame(
    [
        {"path": Path("a/a"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
        {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
        {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "links": 1, "md5": pd.NA},
        {"path": Path("a/d"), "inode": 3, "size": 12, "mtime": 5, "links": 1, "md5": pd.NA},
        {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 6, "links": 1, "md5": pd.NA},
        {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "links": 1, "md5": pd.NA},
    ]
).set_index("path")

df_old_scan = pd.DataFrame(
    [
        {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "md5": "abc"},  # One copy of linked
        {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "md5": "def"},  # Match with md5
        {"path": Path("a/d"), "inode": 3, "size": 19, "mtime": 5, "md5": "def"},  # Different size
        {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 1, "md5": "ghi"},  # Different mtime
        {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "md5": pd.NA},  # Missing md5
        {"path": Path("a/g"), "inode": 6, "size": 15, "mtime": 8, "md5": "mno"},  # Isn't in original
    ]
).set_index("path")

df_expected_merge = pd.DataFrame(
    [
        {"path": Path("a/a"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": pd.NA},
        {"path": Path("a/b"), "inode": 1, "size": 10, "mtime": 2, "links": 2, "md5": "abc"},
        {"path": Path("a/c"), "inode": 2, "size": 11, "mtime": 4, "links": 1, "md5": "def"},
        {"path": Path("a/d"), "inode": 3, "size": 12, "mtime": 5, "links": 1, "md5": pd.NA},
        {"path": Path("a/e"), "inode": 4, "size": 13, "mtime": 6, "links": 1, "md5": pd.NA},
        {"path": Path("a/f"), "inode": 5, "size": 14, "mtime": 7, "links": 1, "md5": pd.NA},
    ]
).set_index("path")


@pytest.fixture
def setup_test_filesystem(tmp_path):
    baseline_text = "Baseline_text"
    (tmp_path / "root1").mkdir()
    (tmp_path / "root2").mkdir()
    (tmp_path / "root1" / "baseline").write_text(baseline_text)
    (tmp_path / "root1" / "baseline_copy").write_text(baseline_text)
    (tmp_path / "root1" / "baseline_linked").hardlink_to(tmp_path / "root1" / "baseline")
    (tmp_path / "root1" / "different").write_text("Different content")
    (tmp_path / "root2" / "baseline").write_text(baseline_text)
    (tmp_path / "root2" / "linked_elsewhere").hardlink_to(tmp_path / "root2" / "baseline")
    (tmp_path / "root2" / "different").write_text("root 2 different content")


# Need old dataframe with missing result, good result, bad result


class TestScanFilesystem:
    def test_scan_filesystem(self, setup_test_filesystem, tmp_path):
        result = scan_filesystem([tmp_path / "root1", tmp_path / "root2"])
        test_paths = [p for p in tmp_path.rglob("*") if p.is_file()]
        assert Counter(test_paths) == Counter(result.index.to_list())
        assert Counter(["inode", "size", "mtime", "links"]) == Counter(result.columns)


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


def test_merge_previous_scan_hashes():
    merged = merge_previous_scan_hashes(df_new_scan)
    pd.testing.assert_frame_equal(merged, df_expected_merge)
