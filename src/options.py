import os

class Options:
    def __init__(self, dir_path,memtable_size=1024,max_sst_size=1024, max_level=7, max_level_num=7, table_num=10):
        self.dir_path = dir_path
        self.max_sst_size = max_sst_size
        self.max_level = max_level
        self.max_level_num = max_level_num
        self.table_num = table_num
        self.memtable_size = memtable_size
        self._set_default_options()
    
    def __str__(self) -> str:
        return f"Options(dir_path={self.dir_path}, max_sst_size={self.max_sst_size}, max_level={self.max_level}, max_level_num={self.max_level_num}, table_num={self.table_num})"
   
    def _set_default_options(self):
        if self.max_sst_size <= 0:
            self.max_sst_size = 1024
        if self.max_level <= 0:
            self.max_level = 7
        if self.max_level_num <= 0:
            self.max_level_num = 7
        if self.table_num <= 0:
            self.table_num = 10
        if self.memtable_size <= 0:
            self.memtable_size = 1024
    def check(self):
        try:
            os.makedirs(self.dir_path, exist_ok=True)
            os.makedirs(os.path.join(self.dir_path, "wal"), exist_ok=True)
        except OSError as e:
            return e
        return None

# 配置函数
def with_max_sst_size(size):
    def option(o):
        o.max_sst_size = size
    return option

def with_max_level(level):
    def option(o):
        o.max_level = level
    return option

def with_max_level_num(num):
    def option(o):
        o.max_level_num = num
    return option

def with_table_num(num):
    def option(o):
        o.table_num = num
    return option

# 工厂函数
def new_options(dir_path, *opts):
    options = Options(dir_path)

    for opt in opts:
        opt(options)

    error = options.check()
    if error:
        return None, error

    return options, None

# 使用示例
options, err = new_options("/path/to/config",
                           with_max_sst_size(2048),
                           with_max_level(10),
                           with_max_level_num(5),
                           with_table_num(20))

if err:
    print(f"Error: {err}")
else:
    print(f"Options: {options.__dict__}")
