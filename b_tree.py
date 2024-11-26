class BTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []


def borrow_from_prev(node, index):
    child_node = node.children[index]
    sibling_node = node.children[index - 1]

    child_node.keys.insert(0, node.keys[index - 1])
    if not child_node.is_leaf:
        child_node.children.insert(0, sibling_node.children.pop())

    node.keys[index - 1] = sibling_node.keys.pop()


def borrow_from_next(node, index):
    child_node = node.children[index]
    sibling_node = node.children[index + 1]

    child_node.keys.append(node.keys[index])
    if not child_node.is_leaf:
        child_node.children.append(sibling_node.children.pop(0))

    node.keys[index] = sibling_node.keys.pop(0)


def merge_nodes(node, index):
    child_node = node.children[index]
    sibling_node = node.children[index + 1]

    child_node.keys.append(node.keys.pop(index))
    child_node.keys.extend(sibling_node.keys)
    if not child_node.is_leaf:
        child_node.children.extend(sibling_node.children)

    node.children.pop(index + 1)


def get_successor(node):
    current_node = node
    while not current_node.is_leaf:
        current_node = current_node.children[0]
    return current_node.keys[0]


def get_predecessor(node):
    current_node = node
    while not current_node.is_leaf:
        current_node = current_node.children[-1]
    return current_node.keys[-1]


class BTree:
    def __init__(self, min_pow):
        self.root = BTreeNode(is_leaf=True)
        self.min_pow = min_pow

    def insert(self, key_value):
        root = self.root
        if len(root.keys) == (2 * self.min_pow) - 1:
            new_root = BTreeNode()
            new_root.children.insert(0, root)
            self.root = new_root
            self.split_child(new_root, 0)
            self.insert_non_full(new_root, key_value)
        else:
            self.insert_non_full(root, key_value)

    def insert_non_full(self, node, key_value):
        key = key_value[0]
        index = len(node.keys) - 1
        if node.is_leaf:
            node.keys.append((None, None))
            while index >= 0 and key < node.keys[index][0]:
                node.keys[index + 1] = node.keys[index]
                index -= 1
            node.keys[index + 1] = key_value
        else:
            while index >= 0 and key < node.keys[index][0]:
                index -= 1
            index += 1
            if len(node.children[index].keys) == (2 * self.min_pow) - 1:
                self.split_child(node, index)
                if key > node.keys[index][0]:
                    index += 1
            self.insert_non_full(node.children[index], key_value)

    def split_child(self, parent_node, index):
        node_to_split = parent_node.children[index]
        new_node = BTreeNode(node_to_split.is_leaf)
        parent_node.children.insert(index + 1, new_node)
        parent_node.keys.insert(index, node_to_split.keys[self.min_pow - 1])

        new_node.keys = node_to_split.keys[self.min_pow:(2 * self.min_pow) - 1]
        node_to_split.keys = node_to_split.keys[0:self.min_pow - 1]

        if not node_to_split.is_leaf:
            new_node.children = node_to_split.children[self.min_pow:2 * self.min_pow]
            node_to_split.children = node_to_split.children[0:self.min_pow]

    def delete(self, node, key_value):
        key = key_value[0]
        value = key_value[1]
        index = 0
        while index < len(node.keys) and key > node.keys[index][0]:
            index += 1

        if node.is_leaf:
            if index < len(node.keys) and node.keys[index][0] == key and node.keys[index][1] == value:
                node.keys.pop(index)
            return

        if (index < len(node.keys)
                and node.keys[index][0] == key
                and node.keys[index][1] == value):
            self.delete_internal_node(node, key_value, index)
        else:
            child_node = node.children[index]
            if len(child_node.keys) >= self.min_pow:
                self.delete(child_node, key_value)
            else:
                self.fill(node, index)
                self.delete(node.children[index], key_value)

    def delete_internal_node(self, node, key_value, index):
        key = key_value[0]
        value = key_value[1]

        if node.is_leaf:
            if node.keys[index][0] == key and node.keys[index][1] == value:
                node.keys.pop(index)
            return

        predecessor_node = node.children[index]
        successor_node = node.children[index + 1]

        if len(predecessor_node.keys) >= self.min_pow:
            pred_key_value = get_predecessor(predecessor_node)
            node.keys[index] = pred_key_value
            self.delete(predecessor_node, pred_key_value)
        elif len(successor_node.keys) >= self.min_pow:
            succ_key_value = get_successor(successor_node)
            node.keys[index] = succ_key_value
            self.delete(successor_node, succ_key_value)
        else:
            merge_nodes(node, index)
            self.delete(node.children[index], key_value)

    def fill(self, node, index):
        if index != 0 and len(node.children[index - 1].keys) >= self.min_pow:
            borrow_from_prev(node, index)
        elif index != len(node.keys) and len(node.children[index + 1].keys) >= self.min_pow:
            borrow_from_next(node, index)
        else:
            if index != len(node.keys):
                merge_nodes(node, index)
            else:
                merge_nodes(node, index - 1)

    def search(self, node, key):
        index = 0
        while index < len(node.keys) and key > node.keys[index][0]:
            index += 1
        if index < len(node.keys) and node.keys[index][0] == key:
            return node.keys[index][1]
        if node.is_leaf:
            return None
        return self.search(node.children[index], key)

