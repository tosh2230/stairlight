from __future__ import annotations

from typing import Any


class Node:
    """A Node of singly-linked list"""

    def __init__(self, val: str):
        self.val: str = val
        self.next: "Node" | None = None


def is_cyclic(tables: list[str]) -> bool:
    """Floyd's cycle-finding algorithm

    Args:
        tables (list[str]): Detected tables

    Returns:
        bool: The table dependency is cyclic or not
    """
    nodes: dict[str, Node] = {}
    for table in tables:
        if table not in nodes.keys():
            nodes[table] = Node(table)
    for i, table in enumerate(tables):
        if not nodes[table].next and i < len(tables) - 1:
            nodes[table].next = nodes[tables[i + 1]]

    slow: Node | None
    fast: Node | None
    slow = fast = nodes[tables[0]]
    while fast and fast.next:
        if slow:
            slow = slow.next
        fast = fast.next.next
        if slow == fast:
            return True
    return False


def deep_merge(original: dict[str, Any], add: dict[str, Any]) -> dict[str, Any]:
    """Merge nested dicts

    Args:
        org (dict[str, Any]): Original dict
        add (dict[str, Any]): Dict to add

    Returns:
        dict: Merged dict
    """
    new: dict[str, Any] = original
    for add_key, add_value in add.items():
        if add_key not in original:
            new[add_key] = add_value
        elif isinstance(add_value, dict):
            original_value: dict[str, Any] = original.get(add_key, {})
            new[add_key] = deep_merge(original=original_value, add=add_value)
        elif isinstance(add_value, list):
            for element in add_value:
                if element not in new[add_key]:
                    new[add_key].append(element)
    return new
