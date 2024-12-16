import pytest
from unittest.mock import patch, MagicMock
from nodes_helper import NodesHelper


@pytest.fixture
def nodes_mapping1():
    return {
        "Node1": "http://localhost:5000",
        "Node2": "http://localhost:5001"
    }


@pytest.fixture
def nodes_mapping():
    return {
        "Node1": "http://localhost:5000",
        "Node2": "http://localhost:5001",
        "Node3": "http://localhost:5002"
    }


@pytest.fixture
def helper(nodes_mapping):
    return NodesHelper(nodes_mapping, replication_factor=2)


@pytest.fixture
def mock_requests():
    with patch('requests.post') as mock_post, patch('requests.get') as mock_get:
        def side_effect_post(url, json):
            return mock_node_response('post', url, json=json)

        def side_effect_get(url, params=None):
            return mock_node_response('get', url, params=params)

        mock_post.side_effect = side_effect_post
        mock_get.side_effect = side_effect_get
        yield


def mock_node_response(method, url, json=None, params=None):
    if not hasattr(mock_node_response, "node_data"):
        mock_node_response.node_data = {
            "Node1": {},
            "Node2": {},
            "Node3": {}
        }

    node = None
    for nk, nurl in [("Node1", "http://localhost:5000"), ("Node2", "http://localhost:5001"),
                     ("Node3", "http://localhost:5002")]:
        if url.startswith(nurl):
            node = nk
            break

    if node is None:
        response = MagicMock()
        response.raise_for_status.side_effect = Exception("Unknown node")
        return response

    response = MagicMock()
    response.raise_for_status = MagicMock()

    if "/add" in url and method == "post":
        k = json["key"]
        v = json["value"]
        if k in mock_node_response.node_data[node]:
            response.json.return_value = {"error": f"Key {k} exist"}
            response.status_code = 400
            response.raise_for_status.side_effect = Exception("400 Bad Request")
        else:
            mock_node_response.node_data[node][k] = v
            response.json.return_value = {"status": "ok"}
            response.status_code = 200

    elif "/delete" in url and method == "post":
        k = json["key"]
        v = json["value"]
        if k in mock_node_response.node_data[node] and mock_node_response.node_data[node][k] == v:
            del mock_node_response.node_data[node][k]
        response.json.return_value = {"status": "ok"}
        response.status_code = 200

    elif "/get" in url and method == "get":
        k = params["key"]
        val = mock_node_response.node_data[node].get(k, None)
        response.json.return_value = {"value": val}
        response.status_code = 200

    elif "/all_values" in url and method == "get":
        items = list(mock_node_response.node_data[node].items())
        response.json.return_value = {"values": items}
        response.status_code = 200
    else:
        response.json.return_value = {"error": "Not found"}
        response.status_code = 404
        response.raise_for_status.side_effect = Exception("404 Not Found")

    return response


class TestNodesHelper:
    def test_find_node_by_key(self, nodes_mapping1):
        helper = NodesHelper(nodes_mapping1, replication_factor=1)
        node = helper.find_node_by_key("some_key")
        assert node in nodes_mapping1.keys()


    def test_add_element(self, nodes_mapping1):
        helper = NodesHelper(nodes_mapping1, replication_factor=1)
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_post.return_value = mock_response

            helper.add_element("k", "v")
            mock_post.assert_called_once()


    def test_get_value(self, nodes_mapping1):
        helper = NodesHelper(nodes_mapping1, replication_factor=2)
        with patch('requests.get') as mock_get:
            def side_effect(url, params=None):
                r = MagicMock()
                r.raise_for_status = MagicMock()
                if "5000" in url:
                    r.json.return_value = {"value": None}
                else:
                    r.json.return_value = {"value": "X"}
                return r

            mock_get.side_effect = side_effect
            val = helper.get_value("keyX")
            assert val == "X"


    def test_delete_element(self, nodes_mapping1):
        helper = NodesHelper(nodes_mapping1, replication_factor=2)
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_post.return_value = mock_response

            helper.delete_element("keyD", "valD")
            assert mock_post.call_count == 2  # replication_factor=2


    def test_get_all_data(self, nodes_mapping1):
        helper = NodesHelper(nodes_mapping1, replication_factor=1)
        with patch('requests.get') as mock_get:
            mock_response_1 = MagicMock()
            mock_response_1.raise_for_status = MagicMock()
            mock_response_1.json.return_value = {"values": [("k1", "v1"), ("k2", "v2")]}

            mock_response_2 = MagicMock()
            mock_response_2.raise_for_status = MagicMock()
            mock_response_2.json.return_value = {"values": [("k3", "v3")]}

            mock_get.side_effect = [mock_response_1, mock_response_2]

            data = helper.get_all_data()
            expected = {("k1", "v1"), ("k2", "v2"), ("k3", "v3")}
            assert set(data) == expected


    def test_massive_add_get_delete(self, helper, mock_requests):
        for i in range(50):
            helper.add_element(f"key{i}", f"val{i}")

        assert helper.get_value("key0") == "val0"
        assert helper.get_value("key25") == "val25"
        assert helper.get_value("key49") == "val49"
        assert helper.get_value("no_key") is None

        for i in range(0, 50, 5):
            helper.delete_element(f"key{i}", f"val{i}")

        for i in range(0, 50, 5):
            assert helper.get_value(f"key{i}") is None

        assert helper.get_value("key1") == "val1"
        assert helper.get_value("key48") == "val48"

        all_data = helper.get_all_data()
        keys = [k for k, v in all_data]
        for i in range(0, 50, 5):
            assert f"key{i}" not in keys

        assert "key9" in keys


    def test_add_existing_key(self, helper, mock_requests):
        helper.add_element("key", "valX")

        with pytest.raises(Exception, match="400 Bad Request"):
            helper.add_element("key", "valY")


    def test_replica_nodes_distribution(self, helper):
        replicas = helper.find_replica_nodes("some_key", 2)
        assert len(set(replicas)) == 2


    def test_mixed_operations(self, helper, mock_requests):
        helper.add_element("A", "alpha")
        helper.add_element("B", "beta")
        helper.add_element("C", "gamma")

        helper.delete_element("B", "beta")

        for i in range(10):
            helper.add_element(f"m{i}", f"val{i}")

        assert helper.get_value("A") == "alpha"
        assert helper.get_value("B") is None
        assert helper.get_value("m9") == "val9"
        assert helper.get_value("m0") == "val0"

        all_data = helper.get_all_data()
        keys = [k for k, v in all_data]
        assert "A" in keys
        assert "B" not in keys
        for i in range(10):
            assert f"m{i}" in keys
