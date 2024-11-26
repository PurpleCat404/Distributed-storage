import hashlib
from bisect import bisect_left
from kv_storage import KVStorage

VIRTUAL_NODES_COUNT = 10 # при увеличении в теории должно быть более равномерное распред

def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** 32)

class NodesHelper:

    def __init__(self, nodes_keys = ["Node1", "Node2", "Node3"]):
        self.tokens = {}  # Ключ: Виртуальное имя узла (несколько имён физического узла) Значение: имя физического узла
        self.nodes = {} #node_key -> node

        for node_key in nodes_keys:
            self.nodes[node_key] = Node(node_key)
            for i in range(VIRTUAL_NODES_COUNT):
                self.tokens[hash_function(f"{node_key}-{i}")] = node_key

        self.sorted_virtual_nodes = sorted(self.tokens.keys())

    def find_node_for_key(self, key):
        key_hash = hash_function(key)
        founded_node = bisect_left(self.sorted_virtual_nodes, key_hash)

        return self.nodes[self.tokens[self.sorted_virtual_nodes[0]]] \
            if founded_node == len(self.sorted_virtual_nodes) \
            else self.nodes[self.tokens[self.sorted_virtual_nodes[founded_node]]]

    def add_element(self, key, value):
        node = self.find_node_for_key(key)
        node.add_element(key, value)

    def delete_element(self, key, value):
        node = self.find_node_for_key(key)
        node.delete_element(key, value)

    def get_value(self, key):
        node = self.find_node_for_key(key)
        return node.get_value(key)

class Node:
    def __init__(self, node_key):
        self.node_key = node_key
        self.kv_storage = KVStorage()

    def add_element(self, key, value):
        self.kv_storage.add_element(key, value)
        print(f"Key {key} with value {value} added to node {self.node_key}")

    def delete_element(self, key, value):
        self.kv_storage.delete_element(key, value)
        print(f"Key {key} with value {value} deleted from node {self.node_key}")

    def get_value(self, key):
        return self.kv_storage.get_value(key)

if __name__ == '__main__':
    nodes = NodesHelper()
    nodes.add_element("aboba", "abobovich")
    print(nodes.get_value("aboba"))
    nodes.delete_element("aboba", "abobovich")
    print(nodes.get_value("aboba"))