import hashlib
import bitarray

class BloomFilter:
    def __init__(self, size, hash_count, seed, bit_array=None):
        self.size = size
        self.hash_count = hash_count
        self.seed = seed
        if bit_array:
            self.bit_array = bitarray.bitarray(bit_array)
        else:
            self.bit_array = bitarray.bitarray(size)
            self.bit_array.setall(0)

    def _hashes(self, item):
        result = []
        for i in range(self.hash_count):
            combined_seed = self.seed + i
            hash_val = int(hashlib.sha256((str(combined_seed) + item).encode()).hexdigest(), 16)
            result.append(hash_val % self.size)
        return result

    def add(self, item):
        for hash_val in self._hashes(item):
            self.bit_array[hash_val] = 1

    def check(self, item):
        return all(self.bit_array[hash_val] for hash_val in self._hashes(item))

    def save_bitmap(self, file_path):
        with open(file_path, 'wb') as f:
            self.bit_array.tofile(f)

    @staticmethod
    def load_bitmap(file_path, size, hash_count, seed):
        bit_array = bitarray.bitarray()
        with open(file_path, 'rb') as f:
            bit_array.fromfile(f)
        return BloomFilter(size, hash_count, seed, bit_array)

# def test_bloom_filter():
#     bloom = BloomFilter(size=1000, hash_count=5, seed=42)
#     bloom.add("hello")
#     bloom.add("world")

#     # 保存位图到文件
#     bloom.save_bitmap('bloom_filter_bitmap.dat')

#     # 从文件加载位图并恢复 Bloom 过滤   器
#     loaded_bloom = BloomFilter.load_bitmap('bloom_filter_bitmap.dat', size=1000, hash_count=5, seed=42)

#     # 检查元素
#     print(loaded_bloom.check("hello"))  # 输出: True
#     print(loaded_bloom.check("world"))  # 输出: True
#     print(loaded_bloom.check("python"))  # 输出: False (可能为True，但False意味着确定不在集合中)

# test_bloom_filter()



