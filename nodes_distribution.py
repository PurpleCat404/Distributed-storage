import hashlib
from bisect import bisect_left
import matplotlib.pyplot as plt


VIRTUAL_NODES_COUNT = 10 # при увеличении в теории должно быть более равномерное распред
def hash_function(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2**32)

def initialize_tokens(nodes_keys = ["Node1", "Node2", "Node3"]):
    tokens = {}  # Ключ: Виртуальное имя узла (несколько имён физического узла) Значение: имя физического узла
    for node_key in nodes_keys:
        for i in range(VIRTUAL_NODES_COUNT):
            tokens[hash_function(f"{node_key}-{i}")] = node_key

    sorted_virtual_nodes = sorted(tokens.keys())
    return tokens, sorted_virtual_nodes

def find_node_with_virtual_nodes(key, tokens, sorted_virtual_nodes):
    key_hash = hash_function(key)
    founded_node = bisect_left(sorted_virtual_nodes, key_hash)

    return tokens[sorted_virtual_nodes[0]] \
        if founded_node == len(sorted_virtual_nodes) \
        else tokens[sorted_virtual_nodes[founded_node]]

if __name__ == "__main__":
    nodes = [f"192.168.1.{i}" for i in range(1, 6)]
    quantity_keys_in_node = {node: 0 for node in nodes}

    tokens, sorted_virtual_nodes = initialize_tokens(nodes)

    num_keys = 10_000
    for i in range(1, num_keys + 1):
        smth_key = f"key-{i}"
        node = find_node_with_virtual_nodes(smth_key, tokens, sorted_virtual_nodes)
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
