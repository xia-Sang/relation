from mem_table import Tree
from options import Options, new_options
from wal import WriteAheadLog
from record import Record

class OldMemtable:
    def __init__(self,mem_table_index,memtable):
        self.mem_table_index = mem_table_index
        self.memtable = memtable
    def __str__(self):
        return f"mem_table_index: {self.mem_table_index}, memtable: {self.memtable}"

class LSMTree:
    def __init__(self,opts:Options):
        self.options = opts
        self.memtable = Tree()
        self.memtable_index = 0
        self.sstables_index = 0
        self.wal = WriteAheadLog("./data",self.memtable_index)
        self.old_memtables = []

    def insert(self, key, value):
        self.wal.write(Record(key, value))
        self.memtable.insert(key, value)
    
    def get(self, key):
        return self.memtable.search(key)
    
    def delete(self, key):
        self.wal.write(Record(key, None))
        self.memtable.insert(key, None)

    def check_memtable_overflow(self):
        return self.memtable.size() > self.options.mem_table_size
    
    def new_memtable(self):
        self.old_memtables.append(OldMemtable(self.memtable_index, self.memtable))
        self.memtable_index += 1
        self.memtable = Tree()

    
def test_lsm_tree():
    lsm_tree = LSMTree(new_options("./data"))
    lsm_tree.insert("key1", "value1")
    lsm_tree.insert("key2", "value2")
    lsm_tree.insert("key3", "value3")
    lsm_tree.delete("key2")
    print(lsm_tree.get("key1"))
    print(lsm_tree.get("key2"))
    print(lsm_tree.get("key3"))

test_lsm_tree()