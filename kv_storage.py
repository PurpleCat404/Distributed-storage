from b_tree import BTree


class KVStorage:
    def __init__(self, node_key):
        self.node_key = node_key
        self.min_pow = 8
        self.tree = BTree(8)

    def add_element(self, key, value):
        if self.tree.search(self.tree.root, key) is not None:
            raise KeyError(f"Key {key} is already exist")
        self.tree.insert((key, value))

    def delete_element(self, key, value):
        self.tree.delete(self.tree.root, (key, value))

    def get_value(self, key):
        value = self.tree.search(self.tree.root, key)
        if value is None:
            return None
        return value

    def get_all_values(self):
        values = []
        self.tree.print_tree(values, self.tree.root)
        return values
