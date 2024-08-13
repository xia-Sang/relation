import os
from mem_table import Tree
from node import Node
from options import Options, new_options
from sst import SSTable
from wal import WriteAheadLog
from record import Record
from util import generate_key, generate_random_string, parse_sst_file_path, parse_wal_file_path
from bloom_filter import BloomFilter
import asyncio

class OldMemTable:
    def __init__(self, mem_table_index, mem_table):
        self.mem_table_index = mem_table_index
        self.mem_table = mem_table

    def __str__(self):
        return f"mem_table_index: {self.mem_table_index}, mem_table: {self.mem_table}"


class LSMTree:
    def __init__(self, opts: Options):
        self.options = opts
        self.mem_table = Tree()
        self.mem_table_index = 0
        self.sst_index = 0
        self.wal:WriteAheadLog = None
        self.old_mem_tables = []
        self.nodes: [Node] = []

        if not os.path.exists(self.options.dir_path):
            os.mkdir(self.options.dir_path)
        self.load_mem_table()
        self.load_nodes()

    def insert(self, key, value):
        self.wal.write(Record(key, value))
        self.mem_table.insert(key, value)
        self.try_to_refresh_mem_table()

    def get(self, key):
        val, ok = self.mem_table.search(key)
        if ok:
            return val
        for i in range(len(self.nodes) - 1, -1, -1):
            val, ok = self.nodes[i].search(key)
            if ok:
                print(key,val,self.nodes[i].file_name)
                return val
        return "false"

    def delete(self, key):
        self.wal.write(Record(key, None))
        self.mem_table.insert(key, None)
        self.try_to_refresh_mem_table()

    def check_mem_table_overflow(self):
        return self.mem_table.get_capacity() > self.options.mem_table_size
    
    async def handle_mem_table(self):
        sst = SSTable(self.options, 0, self.sst_index)
        await asyncio.to_thread(sst.write, self.mem_table)
        self.nodes.append(Node(self.options, 0, self.sst_index))
        for i in range(len(self.old_mem_tables)):
            if self.old_mem_tables[i]!=self.mem_table:
                continue
            self.old_mem_tables=self.old_mem_tables[i+1:]
        self.wal.delete()
    def new_mem_table(self):
        # self.old_mem_tables.append(OldMemTable(self.mem_table,self.mem_table))
        # sst = SSTable(self.options, 0, self.sst_index)
        # sst.write(self.mem_table)
        # self.nodes.append(Node(self.options, 0, self.sst_index))
        # self.wal.delete()

        self.old_mem_tables.append(OldMemTable(self.mem_table, self.mem_table))            
        asyncio.run(self.handle_mem_table())

        self.mem_table_index += 1
        self.sst_index += 1
        self.mem_table = Tree()
        self.wal = WriteAheadLog(self.options.dir_path, self.mem_table_index)

    def try_to_refresh_mem_table(self):
        if self.check_mem_table_overflow():
            self.new_mem_table()

    def load_mem_table(self):
        wal_files= sorted([f for f in os.listdir(self.options.dir_path) if f.endswith('.wal')], key=lambda x: parse_wal_file_path(x))
        if not wal_files:
            self.wal=WriteAheadLog(self.options.dir_path, self.mem_table_index)
            return
        for k,wal in enumerate(wal_files):
            mem_table_index = parse_wal_file_path(wal)
            wal = WriteAheadLog(self.options.dir_path, mem_table_index)
            records: [Record] = wal.read_all()
            curr_mem_table=Tree()
            for rec in records:
                curr_mem_table.insert(rec.key, rec.value)
            if k==len(wal_files)-1:
                self.mem_table=curr_mem_table
                self.mem_table_index = mem_table_index 
                self.wal=wal
            else:
                self.old_mem_tables.append(OldMemTable(mem_table_index, curr_mem_table))  
                asyncio.run(self.handle_mem_table())
    def load_nodes(self):
        sst_files = sorted([f for f in os.listdir(self.options.dir_path) if f.endswith('.sst')])
        sst_files.sort(key=lambda f: parse_sst_file_path(f))
        for sst_file in sst_files:
            level, sst_index = parse_sst_file_path(sst_file)
            self.nodes.append(Node(self.options, level, sst_index))
            self.sst_index = sst_index


def test_lsm_tree():
    opts = Options("./data", mem_table_size=80)
    lsm_tree = LSMTree(opts)
    for i in range(1000):
        key, value = generate_key(i), generate_random_string(12)
        lsm_tree.insert(key, value)
    for i in range(900):
        key, value = generate_key(i), generate_random_string(12)
        lsm_tree.delete(key)
    for node in lsm_tree.nodes:
        print(node, node.file_name)
    for i in range(1000):
        key, _ = generate_key(i), generate_random_string(12)
        val = lsm_tree.get(key)
        # print(key, val)
        if i<900:
            assert val == None
        else:
            assert len(val)==12
    # print("测试！！！")
    # lsm_tree.mem_table.show()
    # for node in lsm_tree.nodes:
    #     for pi in node.parse_index:
    #         records=node._read_block_data(pi.block_offset,pi.block_length)
    #         for rec in records:
    #             print(node.file_name,rec)

def test_lsm_tree1():
    opts = Options("./data", mem_table_size=80)
    lsm_tree = LSMTree(opts)

    lsm_tree.mem_table.show()
    for i in range(1000):
        key, _ = generate_key(i), generate_random_string(12)
        val = lsm_tree.get(key)
        if i < 900:
            assert val == None
        else:
            assert len(val) == 12

def test_sst_info():
    options = Options("./data", )
    sst = SSTable(options, 0, 0, flag=True)
    print(sst, sst.file_name)
    sparse_index = sst.read_sparse_index()
    bloom_filter = BloomFilter(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_seed,
                               sst.read_bloom_filter())
    for i in range(100):
        key, value = generate_key(i), generate_random_string(12)
        ok = bloom_filter.check(key)
        print(key, ok)

    for si in sparse_index:
        records = sst.read_data_by_index(si.block_offset, si.block_length)
        for rec in records:
            print(rec)

    print(sst.read_data())
    print(sst.read_bloom_filter())
    print(sst.read_sparse_index())


def test_sst_data():
    options = Options("./data")
    level = 0
    sst_index = 0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst = SSTable(options, level, sst_index)
    # sst.write(rbtree)
    parse = sst._read_meta()
    print(parse)
    sparse = sst.read_sparse_index()
    print("sparse", sparse)
    for i in sparse:
        records = sst.read_data_by_index(i.block_offset, i.block_length)
        print("records", records)
    print(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_size,
          sst.read_bloom_filter())
    bloom_filter = BloomFilter(options.bloom_filter_size, options.bloom_filter_hash_num, options.bloom_filter_seed,
                               sst.read_bloom_filter())

    print("bloom_filter", bloom_filter)
    for i in range(101):
        key = generate_key(i)
        print("key", key, bloom_filter.check(key))
