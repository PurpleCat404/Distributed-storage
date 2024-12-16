import hashlib
from bisect import bisect_left
import requests

VIRTUAL_NODES_COUNT = 10


def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** 32)


class NodesHelper:
    def __init__(self, nodes_mapping, replication_factor=3):
        """
        nodes_mapping: dict[str, str]
            Например: {"Node1": "http://localhost:5000"}
            где ключ - имя узла, значение - URL узла
        """
        self.virtual_to_real = {}
        self.nodes = {}  # node_key -> {"url": node_url}
        self.replication_factor = replication_factor

        for node_key, node_url in nodes_mapping.items():
            self.nodes[node_key] = {"url": node_url}
            for i in range(VIRTUAL_NODES_COUNT):
                self.virtual_to_real[hash_function(f"{node_key}-{i}")] = node_key

        self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())

    def find_node_by_key(self, key):
        h = hash_function(key)
        virtual_node_index = bisect_left(self.sorted_virtual_nodes, h)
        if virtual_node_index == len(self.sorted_virtual_nodes):
            virtual_node_index = 0
        virtual_node = self.sorted_virtual_nodes[virtual_node_index]
        node_key = self.virtual_to_real[virtual_node]
        return node_key

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
                unique_nodes.append(node_key)
            i += 1

        return unique_nodes

    def add_element(self, key, value):
        replica_nodes = self.find_replica_nodes(key, self.replication_factor)
        for node_key in replica_nodes:
            url = self.nodes[node_key]["url"]
            r = requests.post(f"{url}/add", json={"key": key, "value": value})
            r.raise_for_status()

    def delete_element(self, key, value):
        replica_nodes = self.find_replica_nodes(key, self.replication_factor)
        for node_key in replica_nodes:
            url = self.nodes[node_key]["url"]
            r = requests.post(f"{url}/delete", json={"key": key, "value": value})
            r.raise_for_status()

    def get_value(self, key):
        replica_nodes = self.find_replica_nodes(key, self.replication_factor)
        for node_key in replica_nodes:
            url = self.nodes[node_key]["url"]
            r = requests.get(f"{url}/get", params={"key": key})
            r.raise_for_status()
            data = r.json()
            value = data.get("value")
            if value is not None:
                return value
        return None

    # def add_node(self, node_key, node_url):
    #     self.nodes[node_key] = {"url": node_url}
    #     for i in range(VIRTUAL_NODES_COUNT):
    #         self.virtual_to_real[hash_function(f"{node_key}-{i}")] = node_key
    #     self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())
    #     self.rebalance_data()
    #
    # def remove_node(self, node_key):
    #     to_remove = [hval for hval, nkey in self.virtual_to_real.items() if nkey == node_key]
    #     for hval in to_remove:
    #         del self.virtual_to_real[hval]
    #
    #     del self.nodes[node_key]
    #     self.sorted_virtual_nodes = sorted(self.virtual_to_real.keys())
    #     self.rebalance_data()

    def get_all_values_from_node(self, node_key):
        url = self.nodes[node_key]["url"]
        r = requests.get(f"{url}/all_values")
        r.raise_for_status()
        data = r.json()
        return data.get("values", [])

    def get_all_data(self):
        all_data = []
        for node_key in self.nodes.keys():
            values = self.get_all_values_from_node(node_key)
            print(f"В {node_key} содержится {len(values)} ключей")
            all_data.extend(values)

        all_data_tuples = [tuple(item) for item in all_data]
        all_data_tuples = list(set(all_data_tuples))  # если нужно убрать дубликаты
        return all_data_tuples

    def rebalance_data(self):
        all_data = []
        for node_key in self.nodes.keys():
            values = self.get_all_values_from_node(node_key)
            all_data.extend(values)

        for node_key in self.nodes.keys():
            values = self.get_all_values_from_node(node_key)
            for (k, v) in values:
                self.delete_element(k, v)

        for (k, v) in all_data:
            self.add_element(k, v)
