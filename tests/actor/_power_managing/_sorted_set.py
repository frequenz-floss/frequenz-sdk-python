# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the sorted set."""

from frequenz.sdk.microgrid._power_managing import _sorted_set


def test_sorted_set() -> None:
    """Test the LLRB class."""
    tree = _sorted_set.SortedSet[str]()
    tree.insert("a")
    tree.insert("e")
    tree.insert("b")
    tree.insert("d")
    tree.insert("c")

    assert list(tree) == ["a", "b", "c", "d", "e"]
    assert tree.search("c") == "c"
    assert tree.search("f") is None
    tree.delete("c")
    assert list(tree) == ["a", "b", "d", "e"]
    assert tree.search("c") is None

    tree.insert("f")

    tree.delete("a")
    assert list(tree) == ["b", "d", "e", "f"]
    assert list(reversed(tree)) == ["f", "e", "d", "b"]
    tree.insert("c")
    tree.insert("c")

    tree.delete("f")
    assert list(tree) == ["b", "c", "d", "e"]

    tree.insert("q")

    assert list(tree) == ["b", "c", "d", "e", "q"]
    assert list(reversed(tree)) == ["q", "e", "d", "c", "b"]
