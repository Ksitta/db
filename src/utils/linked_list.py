import numpy as np


class LinkedList:
    
    
    def __init__(self, capacity:int):
        ''' Stores <capacity> nodes in a bilateral linked list.
            Use the node at index <capacity + 1> as the head and tail.
            Nodes with index == themselves are unlinked.
        '''
        self._capacity = capacity
        self._next = np.arange(capacity + 1)
        self._prev = np.arange(capacity + 1)
        
        
    def _link(self, prev:int, next:int):
        ''' Basic operation 1: link the <prev> and <next> two nodes.
        '''
        self._next[prev] = next
        self._prev[next] = prev
        
        
    def _remove(self, idx:int):
        ''' Basic operation 2: remove a single node from the linked list.
        '''
        self._prev[idx] = idx
        self._next[idx] = idx
        
        
    def remove(self, idx:int):
        ''' Remove a node and link the prev and next ones.
        '''
        if idx >= self._capacity or self._prev[idx] == idx:
            return
        self._link(self._prev[idx], self._next[idx])
        self._remove(idx)
        
    
    def insert_front(self, idx:int):
        ''' Insert a node to the front (i.e. between the head and the previous front).
        '''
        self.remove(idx)
        head = self._capacity
        front = self._next[head]
        self._link(head, idx)
        self._link(idx, front)
        
        
    def insert_back(self, idx:int):
        ''' Insert a node to the back (i.e. between the head and the previous back).
        '''
        self.remove(idx)
        head = self._capacity
        back = self._prev[head]
        self._link(back, idx)
        self._link(idx, head)
        
        
    def get_front(self):
        ''' Get the first node, indicated by the head node at index <self._capacity>.
        '''
        return self._next[self._capacity]
        