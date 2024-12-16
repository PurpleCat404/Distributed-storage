import threading
import pytest
import requests
import time
import subprocess
import os


# Запускать тесты только с пустыми backup.json
# и с закрытием всех процессов через диспетчер задач
@pytest.fixture(scope="session")
def run_main_server():
    test_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(test_file_dir)
    main_server_path = os.path.join(project_dir, "main_server.py")

    if not os.path.exists(main_server_path):
        raise FileNotFoundError(f"main_server.py not found at {main_server_path}")

    cmd = [
        "python.exe",
        main_server_path,
        "--num-nodes", "2",
        "--replication-factor", "1",
        "--main-port", "8000",
        "--base-port", "5000"
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def read_output(proc):
        for line in proc.stdout:
            print(line.strip())

    t = threading.Thread(target=read_output, args=(proc,), daemon=True)
    t.start()

    start_time = time.time()
    while time.time() - start_time < 60:
        try:
            r = requests.get("http://localhost:8000/all_values")
            if r.status_code == 200:
                print("Main server started successfully.")
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    else:
        proc.terminate()
        try:
            output = proc.communicate(timeout=5)[0]
            print("Main server output:\n", output)
        except subprocess.TimeoutExpired:
            proc.kill()
            output = proc.communicate()[0]
            print("Main server output after kill:\n", output)
        raise RuntimeError("Main server not started in time")

    yield

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


class TestIntegrationMainServer:
    def test_add_get(self, run_main_server):
        r = requests.post("http://localhost:8000/add", json={"key": "test_key", "value": "test_value"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("status") == "ok"

        r = requests.get("http://localhost:8000/get", params={"key": "test_key"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("value") == "test_value"

    def test_delete(self, run_main_server):
        r = requests.post("http://localhost:8000/add", json={"key": "del_key", "value": "del_value"})
        assert r.status_code == 200
        assert r.json().get("status") == "ok"

        r = requests.post("http://localhost:8000/delete", json={"key": "del_key", "value": "del_value"})
        assert r.status_code == 200
        assert r.json().get("status") == "ok"

        r = requests.get("http://localhost:8000/get", params={"key": "del_key"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("value") is None

    def test_all_values(self, run_main_server):
        for i in range(1, 6):
            r = requests.post("http://localhost:8000/add", json={"key": f"key{i}", "value": f"val{i}"})
            assert r.status_code == 200
            assert r.json().get("status") == "ok"

        r = requests.get("http://localhost:8000/all_values")
        assert r.status_code == 200
        data = r.json()
        values = data.get("values", [])
        keys = [k for k, v in values]
        for i in range(1, 6):
            assert f"key{i}" in keys

    def test_add_existing_key(self, run_main_server):
        r = requests.post("http://localhost:8000/add", json={"key": "ex_key", "value": "ex_val"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("status") == "ok"

        r = requests.post("http://localhost:8000/add", json={"key": "ex_key", "value": "ex_val2"})
        data = r.json()

        assert "error" in data
