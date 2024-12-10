from flask import Flask, request, jsonify
from kv_storage import KVStorage
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--node-key')
parser.add_argument('--port', type=int)
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
    storage.add_element(key, value)
    return jsonify({"status": "ok"})

@app.route('/delete', methods=['POST'])
def delete_element():
    data = request.get_json()
    key = data["key"]
    value = data["value"]
    storage.delete_element(key, value)
    return jsonify({"status": "ok"})

@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get("key")
    val = storage.get_value(key)
    return jsonify({"value": val})

@app.route('/all_values', methods=['GET'])
def all_values():
    vals = storage.get_all_values()
    return jsonify({"values": vals})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
