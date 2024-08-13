import os
from bloom_filter import BloomFilter
from mem_table import Tree
from meta import Meta
from options import Options, new_options
from record import Record, from_bytes_multiple
from sparse_index import SparseIndex, parse_multiple_indices
from util import generate_key, generate_random_string


class SSTable:
    def __init__(self, options: Options, level, sst_index):
        self.options = options
        # self.file = open(f"{options.dir_path}/sst_{options.memtable_index}.sst", "wb")
        self.file_name = "{}_{}.sst".format(level, sst_index)
        self.meta = None

    def write(self, mem_table: Tree):
        self.file = open(self.file_name, "wb")
        items = list(mem_table.items())
        chunk_size = len(items) // self.options.table_num
        offset = 0
        sparse_index = []
        bloom_filter = self.options.bloom_filter
        bloom_filter.reset()
        for i in range(chunk_size):
            buf = bytearray()
            chunk = items[i * chunk_size:(i + 1) * chunk_size]
            cur_length = 0
            for key, value in chunk:
                bloom_filter.add(key)
                record = Record(key, value)
                data = record.to_bytes()
                buf.extend(data)
                cur_length += len(data)
            min_key = chunk[0][0]
            max_key = chunk[-1][0]
            sparse_index.append(SparseIndex(min_key, max_key, i, offset, cur_length, self.file_name))
            offset += cur_length
            self.file.write(buf)

        # 写入稀疏索引
        sparse_length = 0
        for sp in sparse_index:
            data = sp.to_bytes()
            sparse_length += len(data)
            self.file.write(data)
        data = bloom_filter.to_bytes()
        self.file.write(data)
        meta = Meta(offset, sparse_length, len(data))
        self.file.write(meta.to_bytes())
        self.file.flush()
        self.file.close()

    # 读取sst文件
    def read(self, key):
        pass

    # 删除sst文件
    def delete(self):
        os.remove(self.file_name)

    # 读取meta信息
    def read_meta(self):
        self.file = open(self.file_name, "rb")
        self.file.seek(-12, os.SEEK_END)
        meta_bytes = self.file.read(12)
        meta = Meta(0, 0, 0)
        meta.from_bytes(meta_bytes)
        self.meta = meta

    def read_data(self):
        self.file.seek(0, os.SEEK_SET)
        data = self.file.read(self.meta.block_length)
        records = from_bytes_multiple(data)
        return records

    def read_data_by_index(self, offset, length):
        self.file.seek(offset, os.SEEK_SET)
        data = self.file.read(length)
        records = from_bytes_multiple(data)
        return records

    def read_sparse_index(self) -> [SparseIndex]:
        self.file.seek(self.meta.block_length, os.SEEK_SET)
        sparse_index_bytes = self.file.read(self.meta.parse_index_length)
        sparse_index = parse_multiple_indices(sparse_index_bytes)
        return sparse_index

    def read_bloom_filter(self):
        self.file.seek(self.meta.block_length + self.meta.parse_index_length, os.SEEK_SET)
        bloom_filter_bytes = self.file.read(self.meta.filter_length)
        bloom_filter = self.options.bloom_filter
        bloom_filter.from_bytes(bloom_filter_bytes)
        return bloom_filter


def Test():
    options, _ = new_options("./data")
    level = 0
    sst_index = 0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst = SSTable(options, level, sst_index)
    sst.write(rbtree)


def Test1():
    options, _ = new_options("./data")
    level = 0
    sst_index = 0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst = SSTable(options, level, sst_index)
    # sst.write(rbtree)
    parse = sst.read_meta()
    print(parse)
    sparse = sst.read_sparse_index()
    print("sparse", sparse)
    for i in sparse:
        records = sst.read_data_by_index(i.block_offset, i.block_length)
        print("records", records)
    bloom_filter = sst.read_bloom_filter()
    print("bloom_filter", bloom_filter)
    for i in range(101):
        key=generate_key(i)
        print("key",key,bloom_filter.check(key))

    # key=generate_key(10001)
    # print("key",key,bloom_filter.check(key))
    # records=sst.read_data()
    # print("records",records)


Test()
Test1()
