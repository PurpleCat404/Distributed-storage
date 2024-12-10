import hashlib
from bisect import bisect_left
from kv_storage import KVStorage

VIRTUAL_NODES_COUNT = 10 # при увеличении в теории должно быть более равномерное распред

def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** 32)

class NodesHelper:
    def __init__(self, nodes_keys = ["Node1", "Node2", "Node3"]):
        self.virtual_to_real = {}  # хэш виртуального имени узла -> имя физического узла
        self.nodes = {} #node_key -> node

        for node_key in nodes_keys:
            self.nodes[node_key] = KVStorage(node_key)
            for i in range(VIRTUAL_NODES_COUNT):
                self.virtual_to_real[hash_function(f"{node_key}-{i}")] = node_key

        self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())

    def find_node_by_key(self, key):
        virtual_node_index = bisect_left(self.sorted_virtual_nodes, hash_function(key))

        if virtual_node_index == len(self.sorted_virtual_nodes):
            first_node_key = self.virtual_to_real[self.sorted_virtual_nodes[0]]
            return self.nodes[first_node_key]

        virtual_node = self.sorted_virtual_nodes[virtual_node_index]
        node_key = self.virtual_to_real[virtual_node]

        return self.nodes[node_key]

    def add_element(self, key, value):
        node = self.find_node_by_key(key)
        node.add_element(key, value)

    def delete_element(self, key, value):
        node = self.find_node_by_key(key)
        node.delete_element(key, value)

    def get_value(self, key):
        node = self.find_node_by_key(key)
        return node.get_value(key)


if __name__ == '__main__':
    nodes = NodesHelper()
    nodes.add_element("aboba", "abobovich")
    print(nodes.get_value("aboba"))
    nodes.delete_element("aboba", "abobovich")
    print(nodes.get_value("aboba"))