import hashlib
from bisect import bisect_left
import matplotlib.pyplot as plt


VIRTUAL_NODES_COUNT = 10 # при увеличении в теории должно быть более равномерное распред

def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2 ** 32)

class NodesHelper:

    def __init__(self, nodes_keys = ["Node1", "Node2", "Node3"]):
        self.tokens = {}  # Ключ: Виртуальное имя узла (несколько имён физического узла) Значение: имя физического узла

        for node_key in nodes_keys:
            for i in range(VIRTUAL_NODES_COUNT):
                self.tokens[hash_function(f"{node_key}-{i}")] = node_key

        self.sorted_virtual_nodes = sorted(self.tokens.keys())

    def find_node_for_key(self, key):
        key_hash = hash_function(key)
        founded_node = bisect_left(self.sorted_virtual_nodes, key_hash)

        return self.tokens[self.sorted_virtual_nodes[0]] \
            if founded_node == len(self.sorted_virtual_nodes) \
            else self.tokens[self.sorted_virtual_nodes[founded_node]]

#TESTING
if __name__ == "__main__":
    nodes = [f"192.168.1.{i}" for i in range(1, 6)]
    node_helper = NodesHelper(nodes)
    quantity_keys_in_node = {node: 0 for node in nodes}
    num_keys = 10_000

    for i in range(1, num_keys + 1):
        smth_key = f"key-{i}"
        node = node_helper.find_node_for_key(smth_key)
        quantity_keys_in_node[node] += 1

    for node, count in quantity_keys_in_node.items():
        print(f"Node {node} has {count} keys")

    #тут рисуется красиво
    plt.bar(quantity_keys_in_node.keys(), quantity_keys_in_node.values())
    plt.xlabel("Node")
    plt.ylabel("Number of Keys")
    plt.title("Distribution of Keys Across Nodes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()