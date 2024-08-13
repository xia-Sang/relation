import struct


class SparseIndex:
    def __init__(self,min_key,max_key,block_index,block_offset,block_length,file_name):
        self.min_key=min_key
        self.max_key=max_key
        self.block_index=block_index
        self.block_offset=block_offset
        self.block_length=block_length
        self.file_name=file_name
    def __str__(self):
        return f"min_key: {self.min_key}, max_key: {self.max_key}, block_index: {self.block_index}, block_offset: {self.block_offset}, block_length: {self.block_length}, file_name: {self.file_name}"

    @staticmethod
    def from_bytes(bytes_data):
        min_key_size = struct.unpack('!I', bytes_data[:4])[0]
        min_key = bytes_data[4:4 + min_key_size].decode('utf-8')
        
        max_key_size_offset = 4 + min_key_size
        max_key_size = struct.unpack('!I', bytes_data[max_key_size_offset:max_key_size_offset + 4])[0]
        max_key = bytes_data[max_key_size_offset + 4:max_key_size_offset + 4 + max_key_size].decode('utf-8')
        
        block_index_offset = max_key_size_offset + 4 + max_key_size
        block_index = struct.unpack('!I', bytes_data[block_index_offset:block_index_offset + 4])[0]
        
        block_offset_offset = block_index_offset + 4
        block_offset = struct.unpack('!I', bytes_data[block_offset_offset:block_offset_offset + 4])[0]
        
        block_length_offset = block_offset_offset + 4
        block_length = struct.unpack('!I', bytes_data[block_length_offset:block_length_offset + 4])[0]
        
        file_name_size_offset = block_length_offset + 4
        file_name_size = struct.unpack('!I', bytes_data[file_name_size_offset:file_name_size_offset + 4])[0]
        file_name = bytes_data[file_name_size_offset + 4:file_name_size_offset + 4 + file_name_size].decode('utf-8')
        
        return SparseIndex(min_key, max_key, block_index, block_offset, block_length, file_name)

    def to_bytes(self):
        try:
            min_key_bytes = self.min_key.encode('utf-8')
            max_key_bytes = self.max_key.encode('utf-8')
            file_name_bytes = self.file_name.encode('utf-8')
            
            min_key_size = len(min_key_bytes)
            max_key_size = len(max_key_bytes)
            file_name_size = len(file_name_bytes)
            
            return struct.pack(f'!I{min_key_size}sI{max_key_size}sIII', 
                               min_key_size, min_key_bytes, 
                               max_key_size, max_key_bytes, 
                               self.block_index, self.block_offset, self.block_length) + struct.pack(f'!I{file_name_size}s', 
                               file_name_size, file_name_bytes)
        except Exception as e:
            raise ValueError(f"Error in to_bytes: {e}")

    @staticmethod
    def from_bytes(bytes_data):
        min_key_size = struct.unpack('!I', bytes_data[:4])[0]
        min_key = bytes_data[4:4 + min_key_size].decode('utf-8')
        
        max_key_size_offset = 4 + min_key_size
        max_key_size = struct.unpack('!I', bytes_data[max_key_size_offset:max_key_size_offset + 4])[0]
        max_key = bytes_data[max_key_size_offset + 4:max_key_size_offset + 4 + max_key_size].decode('utf-8')
        
        block_index_offset = max_key_size_offset + 4 + max_key_size
        block_index = struct.unpack('!I', bytes_data[block_index_offset:block_index_offset + 4])[0]
        
        block_offset_offset = block_index_offset + 4
        block_offset = struct.unpack('!I', bytes_data[block_offset_offset:block_offset_offset + 4])[0]
        
        block_length_offset = block_offset_offset + 4
        block_length = struct.unpack('!I', bytes_data[block_length_offset:block_length_offset + 4])[0]
        
        file_name_size_offset = block_length_offset + 4
        file_name_size = struct.unpack('!I', bytes_data[file_name_size_offset:file_name_size_offset + 4])[0]
        file_name = bytes_data[file_name_size_offset + 4:file_name_size_offset + 4 + file_name_size].decode('utf-8')
        
        return SparseIndex(min_key, max_key, block_index, block_offset, block_length, file_name)


def test_sparse_index():
    sparse_index = SparseIndex("1", "100", 1, 100, 200, "test.txt")
    print(sparse_index)
    print(sparse_index.to_bytes())
    print(SparseIndex.from_bytes(sparse_index.to_bytes()))

def parse_multiple_indices(bytes_data):
    indices = []
    offset = 0
    while offset < len(bytes_data):
        min_key_size = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        min_key = bytes_data[offset:offset + min_key_size].decode('utf-8')
        offset += min_key_size

        max_key_size = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        max_key = bytes_data[offset:offset + max_key_size].decode('utf-8')
        offset += max_key_size

        block_index = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4

        block_offset = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4

        block_length = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4

        file_name_size = struct.unpack('!I', bytes_data[offset:offset + 4])[0]
        offset += 4
        file_name = bytes_data[offset:offset + file_name_size].decode('utf-8')
        offset += file_name_size

        indices.append(SparseIndex(min_key, max_key, block_index, block_offset, block_length, file_name))
    return indices

def test_parse_multiple_indices():
    sparse_index1 = SparseIndex("1", "100", 1, 100, 200, "test1.txt")
    sparse_index2 = SparseIndex("101", "200", 2, 200, 300, "test2.txt")
    bytes_data = sparse_index1.to_bytes() + sparse_index2.to_bytes()
    indices = parse_multiple_indices(bytes_data)
    for index in indices:
        print(index)

test_parse_multiple_indices()
test_sparse_index()