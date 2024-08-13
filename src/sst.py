import os
from mem_table import Tree
from meta import Meta
from options import new_options
from record import Record
from sparse_index import SparseIndex
from util import generate_key, generate_random_string

class SSTable:
    def __init__(self, options,level,sst_index):
        self.options = options
        # self.file = open(f"{options.dir_path}/sst_{options.memtable_index}.sst", "wb")
        self.file_name="{}_{}.sst".format(level,sst_index)
       
    def write(self,mem_table:Tree):
        self.file=open(self.file_name,"wb")
        items = list(mem_table.items())
        print(self.options)
        chunk_size = len(items) // self.options.table_num
        print(chunk_size)
        offset = 0
        sparse_index=[]
        for i in range(chunk_size):
            buf = bytearray()
            chunk = items[i * chunk_size:(i + 1) * chunk_size]
            cur_length=0
            for key, value in chunk:
                record = Record(key, value)
                data=record.to_bytes()
                buf.extend(data)
                cur_length+=len(data)
            min_key=chunk[0][0]
            max_key=chunk[-1][0]
            sparse_index.append(SparseIndex(min_key,max_key,i,offset,cur_length,self.file_name))
            offset+=cur_length
            self.file.write(buf)
            self.file.flush()

        meta=Meta(offset,0,0)
        # 写入稀疏索引
        sparse_length=0
        for sp in sparse_index:
            print(sp)
            data=sp.to_bytes()
            sparse_length+=len(data)
            self.file.write(data)
        meta.set_parse_index_length(sparse_length)
        self.file.write(meta.to_bytes())
        self.file.flush()
        self.file.close()

    # 读取sst文件
    def read(self,key):
        pass

    # 删除sst文件
    def delete(self):
        os.remove(self.file_name)

    # 读取meta信息
    def read_meta(self):
        self.file=open(self.file_name,"rb")
        self.file.seek(-12, os.SEEK_END)
        meta_bytes = self.file.read(12)
        meta = Meta(0, 0, 0)
        meta.from_bytes(meta_bytes)
        return meta
        
def Test():
    options,_=new_options("./data")
    level=0
    sst_index=0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst=SSTable(options,level,sst_index)
    sst.write(rbtree)

def Test1():
    options,_=new_options("./data")
    level=0
    sst_index=0
    rbtree = Tree()
    for i in range(100):
        rbtree.insert(generate_key(i), generate_random_string())
    # rbtree.show()

    sst=SSTable(options,level,sst_index)
    # sst.write(rbtree)
    parse=sst.read_meta()
    print(parse)

Test()
Test1()