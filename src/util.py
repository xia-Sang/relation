import random
import string
import os

def generate_random_number():
    return random.randint(0, 100)

def generate_random_string(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def generate_key(key):
    return "test_key_" + str(key)

def get_wal_file_path(dir_path, key):
    return os.path.join(dir_path, "{0:09d}.wal".format(key))

def parse_wal_file_path(wal_file_path):
    return int(os.path.basename(wal_file_path).split('.')[0])


# # 测试解析wal文件路径
# if __name__ == "__main__":
#     print(parse_wal_file_path("data/000000001.wal"))
#     print(get_wal_file_path("data", 1))