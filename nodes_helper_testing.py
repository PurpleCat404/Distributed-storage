from nodes_helper import NodesHelper
import matplotlib.pyplot as plt


if __name__ == '__main__':
    nodes = [f"192.168.1.{i}" for i in range(1, 6)]
    node_helper = NodesHelper(nodes)
    quantity_keys_in_node = {node: 0 for node in nodes}
    num_keys = 10_000

    for i in range(1, num_keys + 1):
        smth_key = f"key-{i}"
        node = node_helper.find_node_for_key(smth_key).node_key
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