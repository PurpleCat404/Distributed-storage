import os
import json
from b_tree import BTree

class KVStorage:
    def __init__(self, node_key):
        self.node_key = node_key
        self.min_pow = 8
        self.tree = BTree(8)
        self.backup_file = f"backups/{node_key}_backup.json"
        self.load_from_backup()

    def load_from_backup(self):
        if os.path.exists(self.backup_file):
            with open(self.backup_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for k, v in data.items():
                    self.tree.insert((k, v))

    def save_to_backup(self):
        values = []
        self.tree.print_tree(values, self.tree.root)
        # values имеет вид [(k,v), (k2,v2), ...]
        # преобразуем в словарь для удобства
        data = {k: v for k, v in values}
        with open(self.backup_file, 'w', encoding='utf-8') as f:
            json.dump(data, f)

    def add_element(self, key, value):
        if self.tree.search(self.tree.root, key) is not None:
            raise KeyError(f"Key '{key}' is already exist")
        self.tree.insert((key, value))
        self.save_to_backup()

    def delete_element(self, key, value):
        self.tree.delete(self.tree.root, (key, value))
        self.save_to_backup()

    def get_value(self, key):
        value = self.tree.search(self.tree.root, key)
        if value is None:
            return None
        return value

    def get_all_values(self):
        values = []
        self.tree.print_tree(values, self.tree.root)
        return values
