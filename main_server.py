import argparse
import subprocess
import time
from flask import Flask, request, jsonify
from nodes_helper import NodesHelper

app = Flask(__name__)

def start_nodes(num_nodes, base_port=5000):
    """Запускает num_nodes узлов, каждый в отдельном процессе, начиная с base_port."""
    processes = []
    nodes_mapping = {}

    for i in range(num_nodes):
        node_key = f"Node{i}"
        port = base_port + i
        p = subprocess.Popen(["python", "node_server.py", "--node-key", node_key, "--port", str(port)])
        processes.append(p)
        nodes_mapping[node_key] = f"http://localhost:{port}"

    time.sleep(5)
    return nodes_mapping, processes

@app.route('/add', methods=['POST'])
def add_element():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    try:
        helper.add_element(key, value)
        return jsonify({"status": "ok"}), 200
    except:
        return jsonify({"error": f"Key {key} is already exist"})

@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get("key")
    value = helper.get_value(key)
    return jsonify({"value": value}), 200

@app.route('/delete', methods=['POST'])
def delete_element():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    helper.delete_element(key, value)
    return jsonify({"status": "ok"}), 200

@app.route('/all_values', methods=['GET'])
def all_values():
    data = helper.get_all_data()
    return jsonify({"values": data}), 200


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Main server for distributed storage")
    parser.add_argument('--num-nodes', type=int, default=3, help='Number of nodes to start')
    parser.add_argument('--base-port', type=int, default=5000, help='Base port for starting nodes')
    parser.add_argument('--replication-factor', type=int, default=1, help='Replication factor')
    parser.add_argument('--main-port', type=int, default=8000, help='Port for main server')
    args = parser.parse_args()

    nodes_mapping, processes = start_nodes(args.num_nodes, args.base_port)

    helper = NodesHelper(nodes_mapping=nodes_mapping, replication_factor=args.replication_factor)
    helper.get_all_data()
    app.run(host='0.0.0.0', port=args.main_port, debug=False, use_reloader=False)
