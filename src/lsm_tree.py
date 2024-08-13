from mem_table import Tree
from options import Options
from wal import WriteAheadLog
from record import Record

class LSMTree:
    def __init__(self,opts:Options):
        self.options = opts
        self.memtable = Tree()
        self.memtable_index = 0
        self.sstables_index = 0
        self.wal = WriteAheadLog("./data",self.memtable_index)


    def insert(self, key, value):
        self.wal.write(Record(key, value))
        self.memtable.insert(key, value)
    
    def get(self, key):
        return self.memtable.search(key)
    
    def delete(self, key):
        self.wal.write(Record(key, None))
        self.memtable.delete(key)


def test_lsm_tree():
    lsm_tree = LSMTree()
    lsm_tree.insert("key1", "value1")
    lsm_tree.insert("key2", "value2")
    lsm_tree.insert("key3", "value3")
    print(lsm_tree.get("key1"))
    print(lsm_tree.get("key2"))
    print(lsm_tree.get("key3"))

test_lsm_tree()