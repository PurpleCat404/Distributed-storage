from nodes_helper import NodesHelper
import matplotlib.pyplot as plt


if __name__ == '__main__':
    nodes = [f"192.168.1.{i}" for i in range(1, 6)]
    nodes_mapping = {}
    for i in range(1, 6):
        nodes_mapping.update({f"192.168.1.{i}": f"192.168.1.{i}"})
    nodes_helper = NodesHelper(nodes_mapping)
    quantity_keys_in_node = {node: 0 for node in nodes}
    num_keys = 10_000

    for i in range(1, num_keys + 1):
        smth_key = f"key-{i}"
        node = nodes_helper.find_node_by_key(smth_key)
        quantity_keys_in_node[node] += 1

    for node, count in quantity_keys_in_node.items():
        print(f"Node {node} has {count} keys")

    plt.bar(quantity_keys_in_node.keys(), quantity_keys_in_node.values())
    plt.xlabel("Node")
    plt.ylabel("Number of Keys")
    plt.title("Distribution of Keys Across Nodes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()