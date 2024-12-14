import requests
from flask import Flask, request, jsonify
from kv_storage import KVStorage
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--node-key', default="Node0")
parser.add_argument('--port', default=5000, type=int)
args = parser.parse_args()

node_key = args.node_key
port = args.port

app = Flask(__name__)
storage = KVStorage(node_key)

@app.route('/add', methods=['POST'])
def add_element():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    try:
        storage.add_element(key, value)
        return jsonify({"status": "ok"})
    except KeyError as e:
        return jsonify({"error": str(e)}), 400

@app.route('/delete', methods=['POST'])
def delete_element():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    storage.delete_element(key, value)
    return jsonify({"status": "ok"}), 200

@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get("key")
    val = storage.get_value(key)
    return jsonify({"value": val}), 200

@app.route('/all_values', methods=['GET'])
def all_values():
    vals = storage.get_all_values()
    return jsonify({"values": vals}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)