# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""A SortedSet implemented with a Left-Leaning Red-Black tree."""

from __future__ import annotations

import abc
import typing

_RED = True
_BLACK = False


_C = typing.TypeVar("_C", bound="_Comparable")


class _Comparable(typing.Protocol):
    @abc.abstractmethod
    def __lt__(self: _C, other: _C, /) -> bool: ...


_ComparableT = typing.TypeVar("_ComparableT", bound=_Comparable)


class _Node(typing.Generic[_ComparableT]):
    def __init__(self, value: _ComparableT, color: bool = _RED):
        """Create a new node with the given value and color.

        Args:
            value: The value to store in the node.
            color: The color of the node.
        """
        self.value = value
        self.color = color
        self.left: _Node[_ComparableT] | None = None
        self.right: _Node[_ComparableT] | None = None

    def rotate_left(self) -> _Node[_ComparableT]:
        """Rotate the node to the left.

        Returns:
            The new root of the subtree.
        """
        right = self.right
        assert right is not None
        self.right = right.left
        right.left = self
        right.color = self.color
        self.color = _RED
        return right

    def rotate_right(self) -> _Node[_ComparableT]:
        """Rotate the node to the right.

        Returns:
            The new root of the subtree.
        """
        left = self.left
        assert left is not None
        self.left = left.right
        left.right = self
        left.color = self.color
        self.color = _RED
        return left

    def flip_colors(self) -> None:
        """Flip the colors of the node and its children."""
        assert self.left is not None
        assert self.right is not None

        self.color = not self.color
        self.left.color = not self.left.color
        self.right.color = not self.right.color


class SortedSet(typing.Generic[_ComparableT]):
    """A sorted set implemented using a Left-Leaning Red-Black tree.

    It requires that the values stored in the tree are comparable.
    """

    def __init__(self) -> None:
        """Create a new LLRB tree that stores just values."""
        self._root: _Node[_ComparableT] | None = None

    def __iter__(self) -> typing.Iterator[_ComparableT]:
        """Iterate over the values in the tree in order.

        Returns:
            An iterator over the values in the tree.
        """
        return self._iter(self._root)

    def __reversed__(self) -> typing.Iterator[_ComparableT]:
        """Iterate over the values in the tree in reverse order.

        Returns:
            An iterator over the values in the tree.
        """
        return self._reverse_iter(self._root)

    def insert(self, value: _ComparableT) -> None:
        """Insert a value into the tree.

        Args:
            value: The value to insert.
        """
        self._root = self._insert(self._root, value)
        self._root.color = _BLACK

    def search(self, value: _ComparableT) -> _ComparableT | None:
        """Search for a value in the tree.

        Args:
            value: The value to search for.

        Returns:
            The value if it is found, otherwise None.
        """
        root = self._root
        while root is not None:
            if value < root.value:
                root = root.left
            elif value > root.value:
                root = root.right
            else:
                return root.value
        return None

    def min(self) -> _ComparableT | None:
        """Get the minimum value in the tree.

        Returns:
            The minimum value, or None if the tree is empty.
        """
        ret = self._min(self._root)
        return ret.value if ret else None

    def delete_min(self) -> None:
        """Delete the minimum value in the tree."""
        if self._root is None:
            return
        self._root = self._delete_min(self._root)
        if self._root is not None:
            self._root.color = _BLACK

    def delete(self, value: _ComparableT) -> None:
        """Delete a value from the tree.

        Args:
            value: The value to delete.
        """
        self._root = self._delete(self._root, value)
        if self._root is not None:
            self._root.color = _BLACK

    def _iter(self, node: _Node[_ComparableT] | None) -> typing.Iterator[_ComparableT]:
        if node is None:
            return
        yield from self._iter(node.left)
        yield node.value
        yield from self._iter(node.right)

    def _reverse_iter(
        self, node: _Node[_ComparableT] | None
    ) -> typing.Iterator[_ComparableT]:
        if node is None:
            return
        yield from self._reverse_iter(node.right)
        yield node.value
        yield from self._reverse_iter(node.left)

    def __len__(self) -> int:
        """Get the number of values in the tree.

        Returns:
            The number of values in the tree.
        """
        return self._len(self._root)

    def _is_red(
        self, node: _Node[_ComparableT] | None
    ) -> typing.TypeGuard[_Node[_ComparableT]]:
        if node is None:
            return False
        return node.color == _RED

    def _insert(
        self, node: _Node[_ComparableT] | None, value: _ComparableT
    ) -> _Node[_ComparableT]:
        if node is None:
            return _Node(value)

        if self._is_red(node.left) and self._is_red(node.right):
            node.flip_colors()

        if value < node.value:
            node.left = self._insert(node.left, value)
        elif value > node.value:
            node.right = self._insert(node.right, value)
        else:
            node.value = value

        if self._is_red(node.right) and not self._is_red(node.left):
            node = node.rotate_left()
        if self._is_red(node.left) and self._is_red(node.left.left):
            node = node.rotate_right()

        return node

    def _len(self, node: _Node[_ComparableT] | None) -> int:
        if node is None:
            return 0
        return 1 + self._len(node.left) + self._len(node.right)

    def _move_red_left(self, node: _Node[_ComparableT]) -> _Node[_ComparableT]:
        node.flip_colors()
        assert node.right is not None
        if self._is_red(node.right.left):
            node.right = node.right.rotate_right()
            node = node.rotate_left()
            node.flip_colors()
        return node

    def _move_red_right(self, node: _Node[_ComparableT]) -> _Node[_ComparableT]:
        node.flip_colors()
        assert node.left is not None
        if self._is_red(node.left.left):
            node = node.rotate_right()
            node.flip_colors()
        return node

    def _min(self, node: _Node[_ComparableT] | None) -> _Node[_ComparableT] | None:
        if node is None or node.left is None:
            return node
        return self._min(node.left)

    def _fix_up(self, node: _Node[_ComparableT]) -> _Node[_ComparableT]:
        if self._is_red(node.right):
            node = node.rotate_left()
        if self._is_red(node.left) and self._is_red(node.left.left):
            node = node.rotate_right()
        if self._is_red(node.left) and self._is_red(node.right):
            node.flip_colors()
        return node

    def _delete_min(
        self, node: _Node[_ComparableT] | None
    ) -> _Node[_ComparableT] | None:
        if node is None or node.left is None:
            return None

        if not self._is_red(node.left) and not self._is_red(node.left.left):
            node = self._move_red_left(node)

        if node.left is None:
            return None

        node.left = self._delete_min(node.left)
        return self._fix_up(node)

    def _delete(
        self, node: _Node[_ComparableT] | None, value: _ComparableT
    ) -> _Node[_ComparableT] | None:
        if node is None:
            return None

        if value < node.value:
            assert node.left is not None
            if not self._is_red(node.left) and not self._is_red(node.left.left):
                node = self._move_red_left(node)
            node.left = self._delete(node.left, value)
        else:
            if self._is_red(node.left):
                node = node.rotate_right()
            if value == node.value and node.right is None:
                return None
            assert node.right is not None
            if not self._is_red(node.right) and not self._is_red(node.right.left):
                node = self._move_red_right(node)
            if value == node.value:
                min_ = self._min(node.right)
                assert min_ is not None
                node.value = min_.value
                node.right = self._delete_min(node.right)
            else:
                node.right = self._delete(node.right, value)

        return self._fix_up(node)
