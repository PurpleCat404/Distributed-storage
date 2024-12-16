import os
import json
import pytest
from kv_storage import KVStorage

@pytest.fixture
def kvstorage(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    os.chdir(tmp_path)
    storage = KVStorage("NodeTest")
    return storage


class TestKVStorage:
    def test_add_and_get(self, kvstorage):
        kvstorage.add_element("key1", "value1")
        assert kvstorage.get_value("key1") == "value1"
        assert kvstorage.get_value("not_exist") is None


    def test_add_existing_key(self, kvstorage):
        kvstorage.add_element("key1", "value1")
        with pytest.raises(KeyError, match=r"Key 'key1' is already exist"):
            kvstorage.add_element("key1", "another_value")


    def test_delete_element(self, kvstorage):
        kvstorage.add_element("k1", "v1")
        kvstorage.add_element("k2", "v2")
        # Удалим k1
        kvstorage.delete_element("k1","v1")
        assert kvstorage.get_value("k1") is None
        assert kvstorage.get_value("k2") == "v2"


    def test_backup_on_disk(self, kvstorage):
        kvstorage.add_element("k1","v1")
        kvstorage.add_element("k2","v2")

        # Проверим, что backup файл существует
        backup_file = f"backups/NodeTest_backup.json"
        assert os.path.exists(backup_file)

        with open(backup_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        assert data["k1"] == "v1"
        assert data["k2"] == "v2"


    def test_load_from_backup(self, tmp_path):
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()
        backup_file = backup_dir / "NodeX_backup.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump({"a":"A","b":"B"}, f)

        os.chdir(tmp_path)
        storage = KVStorage("NodeX")
        assert storage.get_value("a") == "A"
        assert storage.get_value("b") == "B"


    def test_multiple_add(self, kvstorage):
        kvstorage.add_element("k1", "v1")
        kvstorage.add_element("k2", "v2")
        kvstorage.add_element("k3", 100)  # числовое значение
        assert kvstorage.get_value("k1") == "v1"
        assert kvstorage.get_value("k2") == "v2"
        assert kvstorage.get_value("k3") == 100


    def test_add_existing_key_error(self, kvstorage):
        kvstorage.add_element("k1", "v1")
        with pytest.raises(KeyError, match="Key 'k1' is already exist"):
            kvstorage.add_element("k1", "another_val")


    def test_delete_existing_key(self, kvstorage):
        kvstorage.add_element("a", "A")
        kvstorage.add_element("b", "B")
        kvstorage.delete_element("a", "A")
        assert kvstorage.get_value("a") is None
        assert kvstorage.get_value("b") == "B"


    def test_delete_nonexisting_key(self, kvstorage):
        kvstorage.delete_element("not_here", "v")
        assert kvstorage.get_value("not_here") is None


    def test_massive_operations(self, kvstorage):
        for i in range(100):
            kvstorage.add_element(f"key{i}", f"value{i}")

        assert kvstorage.get_value("key0") == "value0"
        assert kvstorage.get_value("key50") == "value50"
        assert kvstorage.get_value("key99") == "value99"
        assert kvstorage.get_value("no_key") is None

        for i in range(0,100,10):
            kvstorage.delete_element(f"key{i}", f"value{i}")

        for i in range(0,100,10):
            assert kvstorage.get_value(f"key{i}") is None

        assert kvstorage.get_value("key1") == "value1"
        assert kvstorage.get_value("key98") == "value98"


    def test_backup_persistence(self, kvstorage, tmp_path):
        kvstorage.add_element("p1","val1")
        kvstorage.add_element("p2","val2")

        backup_file = tmp_path / "backups" / "NodeTest_backup.json"
        assert backup_file.exists()

        new_storage = KVStorage("NodeTest")
        assert new_storage.get_value("p1") == "val1"
        assert new_storage.get_value("p2") == "val2"
