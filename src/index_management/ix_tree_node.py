import os
import struct
import numpy as np

import config as cf
from paged_file.pf_manager import pf_manager
from errors.err_index_management import *


class IX_TreeNodeHeader:
    
    
    def __init__(self, node_type:int, parent:int, page_no:int, entry_number:int,
        prev_sib:int, next_sib:int, child_key_number:int, first_child:int) -> None:
        ''' Init the TreeNodeHeader.
        args:
            node_type: int, in {NODE_TYPE_INTER, NODE_TYPE_LEAF}.
            parent: int, the parent page number, INVALID for no parent (root).
            page_no: int, page number of the current tree node.
            entry_number: int, the valid entry number in this node.
            prev_sib: int, page number of the previous sibling.
            next_sib: int, page number of the next sibling.
            child_key_number: int, the total key number of all children of this node,
                will be ignored if node_type == NODE_TYPE_LEAF.
            first_child: int, the page number of the first child (the smallest one),
                will be ignored if node_type == NODE_TYPE_LEAF.
        '''
        (self.node_type, self.parent, self.page_no, self.entry_number,
            self.prev_sib, self.next_sib, self.child_key_number, self.first_child) \
            = node_type, parent, page_no, entry_number, \
            prev_sib, next_sib, child_key_number, first_child
            
            
    @staticmethod
    def size():
        return 32
    
    
    @staticmethod
    def deserialize(data:np.ndarray):
        ''' Deserialize np.ndarray[>=IX_TreeNodeHeader.size(), uint8] to tree node header.
        '''
        buffer = data[:IX_TreeNodeHeader.size()].tobytes()
        return IX_TreeNodeHeader(*struct.unpack(f'{cf.BYTE_ORDER}iiiiiiii', buffer))
    

    def serialize(self) -> np.ndarray:
        ''' Serialize tree node header to np.ndarray[IX_TreeNodeHeader.size(), uint8].
        '''
        buffer = struct.pack(f'{cf.BYTE_ORDER}iiiiiiii',
            self.node_type, self.parent, self.page_no, self.entry_number,
            self.prev_sib, self.next_sib, self.child_key_number, self.first_child)
        return np.frombuffer(buffer, dtype=np.uint8)
    

class IX_TreeNode:
    
    def __init__(self):
        pass
    
    
if __name__ == '__main__':
    pass
