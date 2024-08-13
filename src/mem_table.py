from bintrees import RBTree

import threading

from util import generate_key, generate_random_string

class Tree:
    # 实例化
    def __init__(self):
        self.tree = RBTree()
        self.lock = threading.RLock()
    # 插入
    def insert(self, k, v):
        with self.lock:
            self.tree.insert(k, v)
    # 查找
    def search(self, k):
        with self.lock:
            try:
                return self.tree[k]
            except KeyError:
                return None
    # 显示
    def show(self):
        with self.lock:
            for key, value in self.tree.items():
                print(f"Key: {key}, Value: {value}")

    def fold(self, func):
        with self.lock:
            for key, value in self.tree.items():
                if not func(key, value):
                    return False
            return True
    
    def items(self):
        with self.lock:
            return self.tree.items()
    # 获取容量
    def get_capacity(self):
        with self.lock:
            return self.tree.count
        
    # 实现合并
    def merge(self, other):
        with self.lock:
            self.tree.update(other.tree)

# def test_rbtree():
#     rbtree = Tree()
#     for i in range(10):
#         rbtree.insert(generate_key(i), generate_random_string())
#     rbtree.show()

#     print("Folded:")
#     def print_key_value(key, value):
#         print(f"Key: {key}, Value: {value}")
#         return True

#     rbtree.fold(print_key_value)
#     print("Capacity: ", rbtree.get_capacity())
#     print("Size: ", rbtree.get_capacity())


#     rbtree1 = Tree()
#     for i in range(10,100):
#         rbtree1.insert(generate_key(i), generate_random_string())

#     rbtree.merge(rbtree1)
#     rbtree.show()
# # Run the test cases
# test_rbtree()
