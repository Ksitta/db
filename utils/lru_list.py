import numpy as np

from utils.linked_list import LinkedList


class LRUList:
    
    
    def __init__(self, capacity:int):
        self._capacity = capacity
        self.linked_list = LinkedList(capacity)
        for idx in range(capacity - 1, -1, -1):
            self.linked_list.insert_front(idx)

    
    def find(self):
        ''' Find the least recently used node by get_front().
            Should call access() to move it to the back.
        '''
        return self.linked_list.get_front()
    

    def free(self, idx:int):
        ''' Free a node by moving it to the front.
        '''
        self.linked_list.insert_front(idx)
        
        
    def access(self, idx:int):
        ''' Access a node by moving it to the back.
        '''
        self.linked_list.insert_back(idx)
        
        
def display(lru_list):
    print(f'prev: {lru_list.linked_list._prev}')
    print(f'next: {lru_list.linked_list._next}')
        

if __name__ == '__main__':
    replace = LRUList(10)
    display(replace)
    for i in range(15):
        first = replace.find()
        replace.access(first)
        print(first)
        display(replace)
        