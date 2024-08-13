# 定义record
# keySize: 4 bytes
# key: keySize bytes
# valueSize: 4 bytes
# value: valueSize bytes
# checksum: 4 bytes
# 总长度：16 bytes
# 如果valueSize为0，则表示删除操作，此时value为空

import struct
import zlib

class Record:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"Record(key={self.key}, value={self.value})"
    # 将record转换为bytes
    def to_bytes(self):
        key_bytes = self.key.encode('utf-8')
        value_bytes = self.value.encode('utf-8') if self.value else b''
        key_size = len(key_bytes)
        value_size = len(value_bytes)
        
        # 使用 zlib.crc32 计算 checksum
        checksum = zlib.crc32(struct.pack(f'!I{key_size}sI{value_size}s', key_size, key_bytes, value_size, value_bytes)) & 0xffffffff

        # 使用 struct 打包
        return struct.pack(f'!I{key_size}sI{value_size}sI', key_size, key_bytes, value_size, value_bytes, checksum)

    @staticmethod
    def from_bytes(bytes_data):
        # 解析 keySize
        key_size = struct.unpack('!I', bytes_data[:4])[0]
        
        # 解析 key
        key = bytes_data[4:4 + key_size].decode('utf-8')
        
        # 解析 valueSize
        value_size_offset = 4 + key_size
        value_size = struct.unpack('!I', bytes_data[value_size_offset:value_size_offset + 4])[0]
        
        # 解析 value
        value = bytes_data[value_size_offset + 4:value_size_offset + 4 + value_size].decode('utf-8') if value_size > 0 else None
        
        # 解析 checksum
        checksum_offset = value_size_offset + 4 + value_size
        checksum = struct.unpack('!I', bytes_data[checksum_offset:checksum_offset + 4])[0]
        
        # 校验 checksum
        calculated_checksum = zlib.crc32(bytes_data[:checksum_offset]) & 0xffffffff
        if checksum != calculated_checksum:
            raise ValueError("Checksum does not match!")

        return Record(key, value)


# # 进行测试
# if __name__ == "__main__":
#     # 创建一个Record对象
#     record = Record("test_key", "test_value")
    
#     # 将Record对象转换为bytes
#     record_bytes = record.bytes()
#     print(f"Record bytes: {record_bytes}")
    
#     # 从bytes转换回Record对象
#     restored_record = Record.from_bytes(record_bytes)
#     print(f"Restored Record - Key: {restored_record.key}, Value: {restored_record.value}")
    
#     # 测试删除操作
#     delete_record = Record("test_key", "")
#     delete_record_bytes = delete_record.bytes()
#     print(f"Delete Record bytes: {delete_record_bytes}")
    
#     restored_delete_record = Record.from_bytes(delete_record_bytes)
#     print(f"Restored Delete Record - Key: {restored_delete_record.key}, Value: {restored_delete_record.value}")
