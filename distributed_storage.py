import struct

class DistributedStorage:
    def __init__(self, file_path):
        self.file_path = file_path

    def initialize_db(self, table_size=1024):
        with open(self.file_path, 'wb') as f:
            header = struct.pack('B I Q I I', 1, 0, table_size * 16 + 24, 0, table_size)
            f.write(header)
            f.write(b'\x00' * (table_size * 16))

    def hash_key(self, key, table_size = 1024):
        return hash(key) % table_size & 0xFFFFFFFF

    def add_record(self, key, value):
        with open(self.file_path, 'r+b') as f:
            f.seek(0)
            version, count, free_offset, checksum, table_size = struct.unpack('B I Q I I', f.read(24))

            index = self.hash_key(key, table_size)
            key_hash = index

            for _ in range(table_size):
                f.seek(24 + index * 16)
                stored_hash, offset = struct.unpack('I Q', f.read(16))

                if offset == 0:
                    break
                index = (index + 1) % table_size

            f.seek(free_offset)
            key_data = key.encode()
            value_data = value.encode()

            # Запись в формате: длина ключа (H), ключ (s), длина значения (H), значение (s)
            record = struct.pack(f'H{len(key_data)}sH{len(value_data)}s', len(key_data), key_data, len(value_data), value_data)
            f.write(record)

            f.seek(24 + index * 16)
            f.write(struct.pack('I Q', key_hash, free_offset))

            free_offset += len(record)
            count += 1
            f.seek(0)
            f.write(struct.pack('B I Q I I', version, count, free_offset, 0, table_size))

    def get_record(self, key):
        with open(self.file_path, 'rb') as f:
            f.seek(0)
            _, _, _, _, table_size = struct.unpack('B I Q I I', f.read(24))

            index = self.hash_key(key, table_size)
            key_hash = self.hash_key(key)

            for _ in range(table_size):
                f.seek(24 + index * 16)
                stored_hash, offset = struct.unpack('I Q', f.read(16))

                if offset == 0:
                    return None
                if stored_hash == key_hash:
                    f.seek(offset)
                    key_len = struct.unpack('H', f.read(2))[0]
                    stored_key = f.read(key_len).decode()
                    if stored_key == key:
                        value_len = struct.unpack('H', f.read(2))[0]
                        value = f.read(value_len).decode()
                        return value
                index = (index + 1) % table_size
