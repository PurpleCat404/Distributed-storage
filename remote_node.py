import requests

class RemoteNode:
    def __init__(self, node_key, base_url):
        self.node_key = node_key
        self.base_url = base_url.rstrip('/')

    def add_element(self, key, value):
        r = requests.post(f"{self.base_url}/add", json={"key": key, "value": value})
        r.raise_for_status()

    def delete_element(self, key, value):
        r = requests.post(f"{self.base_url}/delete", json={"key": key, "value": value})
        r.raise_for_status()

    def get_value(self, key):
        r = requests.get(f"{self.base_url}/get", params={"key": key})
        r.raise_for_status()
        data = r.json()
        return data.get("value")

    def get_all_values(self):
        r = requests.get(f"{self.base_url}/all_values")
        r.raise_for_status()
        return r.json().get("values", [])
