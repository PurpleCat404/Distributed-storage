import hashlib
from bisect import bisect_left
from kv_storage import KVStorage

VIRTUAL_NODES_COUNT = 10 # при увеличении в теории должно быть более равномерное распред

def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** 32)

class NodesHelper:
    def __init__(self, nodes_keys = ["Node1", "Node2", "Node3"], replication_factor = 3):
        self.virtual_to_real = {}  # хэш виртуального имени узла -> имя физического узла
        self.nodes = {} # node_key -> node
        self.replication_factor = replication_factor

        for node_key in nodes_keys:
            self.nodes[node_key] = KVStorage(node_key)
            for i in range(VIRTUAL_NODES_COUNT):
                self.virtual_to_real[hash_function(f"{node_key}-{i}")] = node_key

        self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())

    def find_node_by_key(self, key):
        virtual_node_index = bisect_left(self.sorted_virtual_nodes, hash_function(key))

        if virtual_node_index == len(self.sorted_virtual_nodes):
            virtual_node_index = 0

        virtual_node = self.sorted_virtual_nodes[virtual_node_index]
        node_key = self.virtual_to_real[virtual_node]

        return self.nodes[node_key]

    def find_replica_nodes(self, key, replication_factor):
        if replication_factor <= 1:
            return [self.find_node_by_key(key)]

        h = hash_function(key)
        virtual_node_index = bisect_left(self.sorted_virtual_nodes, h)
        if virtual_node_index == len(self.sorted_virtual_nodes):
            virtual_node_index = 0

        unique_nodes = []
        unique_node_keys = set()

        total_nodes = len(self.sorted_virtual_nodes)
        i = 0
        while len(unique_nodes) < replication_factor and i < total_nodes:
            idx = (virtual_node_index + i) % total_nodes
            vn = self.sorted_virtual_nodes[idx]
            node_key = self.virtual_to_real[vn]
            if node_key not in unique_node_keys:
                unique_node_keys.add(node_key)
                unique_nodes.append(self.nodes[node_key])
            i += 1

        return unique_nodes

    def add_element(self, key, value):
        replica_nodes = self.find_replica_nodes(key, self.replication_factor)
        for node in replica_nodes:
            node.add_element(key, value)

    def delete_element(self, key, value):
        replica_nodes = self.find_replica_nodes(key, self.replication_factor)

        for node in replica_nodes:
            node.delete_element(key, value)

    def get_value(self, key):
        primary_node = self.find_node_by_key(key)
        value = primary_node.get_value(key)
        if value is not None:
            return value

        replica_nodes = self.find_replica_nodes(key, self.replication_factor)
        for node in replica_nodes:
            value = node.get_value(key)
            if value is not None:
                return value

        return None

    def add_node(self, node_key):
        self.nodes[node_key] = KVStorage(node_key)
        for i in range(VIRTUAL_NODES_COUNT):
            self.virtual_to_real[hash_function(f"{node_key}-{i}")] = node_key
        self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())

        self.rebalance_data()

    def remove_node(self, node_key):
        to_remove = [hval for hval, nkey in self.virtual_to_real.items() if nkey == node_key]
        for hval in to_remove:
            del self.virtual_to_real[hval]

        del self.nodes[node_key]
        self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())

        self.rebalance_data()

    def rebalance_data(self):
        all_data = []
        for node_key, node in self.nodes.items():
            values = node.get_all_values() # Возвращает список (key, value)
            all_data.extend(values)

        for node in self.nodes.values():
            old_values = node.get_all_values()
            for (k, v) in old_values:
                node.delete_element(k, v)

        for (k, v) in all_data:
            self.add_element(k, v)
