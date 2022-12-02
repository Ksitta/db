import os
import struct
import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_rid_bucket import IX_RidBucket
from errors.err_index_management import *


class IX_TreeNodeHeader:
    
    
    def __init__(self, node_type:int, parent:int, page_no:int, entry_number:int,
        prev_sib:int, next_sib:int, first_child:int) -> None:
        ''' Init the TreeNodeHeader.
        args:
            node_type: int, in {NODE_TYPE_INTER, NODE_TYPE_LEAF}.
            parent: int, the parent page number, INVALID for no parent (root).
            page_no: int, page number of the current tree node.
            entry_number: int, the valid entry number in this node.
            prev_sib: int, page number of the previous sibling.
            next_sib: int, page number of the next sibling.
            first_child: int, the page number of the first child (the smallest one),
                will be ignored if node_type == NODE_TYPE_LEAF.
        '''
        (self.node_type, self.parent, self.page_no, self.entry_number,
            self.prev_sib, self.next_sib, self.first_child) \
            = (node_type, parent, page_no, entry_number, prev_sib, next_sib, first_child)
            
    
    def __str__(self) -> str:
        return f'{{{self.node_type}, {self.parent}, {self.page_no}, {self.entry_number}, ' \
            + f'{self.prev_sib}, {self.next_sib}, {self.first_child}}}'
            
            
    @staticmethod
    def size():
        return 28
    
    
    @staticmethod
    def deserialize(data:np.ndarray):
        ''' Deserialize np.ndarray[>=IX_TreeNodeHeader.size(), uint8] to tree node header.
        '''
        buffer = data[:IX_TreeNodeHeader.size()].tobytes()
        return IX_TreeNodeHeader(*struct.unpack(f'{cf.BYTE_ORDER}iiiiiii', buffer))
    

    def serialize(self) -> np.ndarray:
        ''' Serialize tree node header to np.ndarray[IX_TreeNodeHeader.size(), uint8].
        '''
        buffer = struct.pack(f'{cf.BYTE_ORDER}iiiiiii', self.node_type, self.parent,
            self.page_no, self.entry_number, self.prev_sib, self.next_sib, self.first_child)
        return np.frombuffer(buffer, dtype=np.uint8)
    

class IX_TreeNodeEntry:
    
    
    def __init__(self, field_type:int, field_size:int,
        field_value:Union[int, float, str], page_no:int, slot_no:int) -> None:
        ''' Init a tree node entry.
        args:
            field_type: int, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_size: int, field size in bytes.
            field_value: Union[int, float, str], the unserialized field value.
            page_no: int, the page_no in the entry pointer.
            slot_no: int, the slot_no in the entry pointer.
        '''
        (self.field_type, self.field_size, self.field_value, self.page_no, self.slot_no) \
            = field_type, field_size, field_value, page_no, slot_no
            
    def __eq__(self, other):
        ''' Check if two entries has equal fields.
        '''
        if isinstance(other, self.__class__):
            return (self.field_type == other.field_type and self.field_size == other.field_size
                and self.field_value == other.field_value)
        return False
    
    # only works when self __class__, field_type, and field_size all match with other.
    def __ne__(self, other): return not self.__eq__(other)
    def __lt__(self, other): return self.field_value < other.field_value
    def __le__(self, other): return self.field_value <= other.field_value
    def __gt__(self, other): return self.field_value > other.field_value
    def __ge__(self, other): return self.field_value >= other.field_value
    
    
    def size(self):
        ''' Return the entry size.
        '''
        return self.field_size + 8
    

    @staticmethod
    def deserialize(field_type:int, field_size:int, data:np.ndarray):
        ''' Deserialize np.ndarray[>=field_size+8, uint8] into tree node entry,
            with specific field_type and field_size.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        if field_type == cf.TYPE_INT:
            field_value, = struct.unpack(f'{BYTE_ORDER}i', data[:field_size].tobytes())
        elif field_type == cf.TYPE_FLOAT:
            field_value, = struct.unpack(f'{BYTE_ORDER}d', data[:field_size].tobytes())
        elif field_type == cf.TYPE_STR:
            field_value, = struct.unpack(f'{BYTE_ORDER}{field_size}s', data[:field_size].tobytes())
            field_value = str(field_value, encoding='utf-8').strip('\0')
        else: raise IndexFieldTypeError(f'Field type {field_type} is invalid.')
        (page_no, slot_no) = struct.unpack(f'{BYTE_ORDER}ii', data[field_size:field_size+8])
        return IX_TreeNodeEntry(field_type, field_size, field_value, page_no, slot_no)
        
        
    def serialize(self) -> np.ndarray:
        ''' Serialize tree node entry into np.ndarray[field_size+8, uint8].
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        (field_type, field_size, field_value) = \
            (self.field_type, self.field_size, self.field_value)
        data = np.zeros(field_size + 8, dtype=np.uint8)
        if field_type == cf.TYPE_INT:
            data[:field_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', field_value), dtype=np.uint8)
        elif field_type == cf.TYPE_FLOAT:
            data[:field_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}d', field_value), dtype=np.uint8)
        elif field_type == cf.TYPE_STR:
            buffer = bytes(field_value[:field_size].ljust(field_size, '\0'))
            data[:field_size] = np.frombuffer(buffer, dtype=np.uint8)
        else: raise IndexFieldTypeError(f'Field type {field_type} is invalid.')
        data[field_size:field_size+8] = struct.pack(f'{BYTE_ORDER}ii', self.page_no, self.slot_no)
        return data
    
    
    def get_all_rids(self, data_file_id:int) -> List[RM_Rid]:
        ''' Get all rids under this entry. Used ONLY when this is a leaf node entry.
        args:
            data_file_id: int, the index data file id.
        '''
        if self.slot_no != cf.INVALID:
            return [RM_Rid(self.page_no, self.slot_no)]
        bucket_page = self.page_no
        rids = []
        while bucket_page != cf.INVALID:
            bucket = IX_RidBucket.deserialize(pf_manager.read_page(data_file_id, bucket_page))
            rids.extend(bucket.get_all_rids())
            bucket_page = bucket.header.next_page
        return rids
        

class IX_TreeNode:
    
    
    def __init__(self, file_id:int, field_type:int, field_size:int, node_capacity:int,
            node_type:int, parent:int, page_no:int):
        ''' Init an empty tree node.
        args:
            file_id: int, the data file id of this node.
            field_type: int, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_size: int, entry key size in bytes, not including page_no and slot_no.
            node_capacity: int, how many entries a node can store.
            node_type: int, in {NODE_TYPE_INTER, NODE_TYPE_LEAF}.
            parent: int, the parent page number, INVALID for no parent (root).
            page_no: int, page number of the current tree node.
        '''
        (self.file_id, self.field_type, self.field_size, self.node_capacity) = \
            (file_id, field_type, field_size, node_capacity)
        self.header = IX_TreeNodeHeader(node_type=node_type, parent=parent,
            page_no=page_no, entry_number=0, prev_sib=cf.INVALID,
            next_sib=cf.INVALID, first_child=cf.INVALID)
        self.entries:List[IX_TreeNodeEntry] = list()
        self.data_modified = True
        
    
    def search_child_idx(self, field_value:Union[int, float, str]) -> int:
        ''' Search the child index of a value in the node entries.
            Suppose there are N entries: K_0, K_1, ..., K_{N-1},
            if field_value in [K_{i-1}, K_i), return i;
            else if field_value < K_0, return 0;
            else if field_value >= K_{N-1}, return N;
            if there are no entries, return 0 directly.
            The returned index can further be interpreted as the insert position.
        args:
            field_value: Union[int, float, str], type must match with self.field_type.
        return: int, the searched key position.
        '''
        entries:List[IX_TreeNodeEntry] = self.entries
        if len(entries) == 0: return 0
        if entries[-1].field_value <= field_value: return len(entries)
        left, right, mid = 0, len(entries), 0
        while left < right:
            mid = (left + right) // 2
            if entries[mid].field_value <= field_value:
                left = mid + 1
            else: right = mid
        return left
        
    
    @staticmethod
    def deserialize(file_id:int, field_type:int, field_size:int, node_capacity:int, data:np.ndarray):
        ''' Deserialize a data page into tree node.
        '''
        node = IX_TreeNode(file_id, field_type, field_size, node_capacity, 0, 0, 0)
        header_size = IX_TreeNodeHeader.size()
        entry_size = field_size + 8
        header = IX_TreeNodeHeader.deserialize(data[:header_size])
        entries = []
        for i in range(header.entry_number):
            off = header_size + i * entry_size
            entries.append(IX_TreeNodeEntry.deserialize(field_type, field_size, data[off:off+entry_size]))
        node.header = header
        node.entries = entries
        return node
        
    
    def serialize(self) -> np.ndarray:
        ''' Serialize this tree node into a data page.
        '''
        header, entries = self.header, self.entries
        header_size = IX_TreeNodeHeader.size()
        entry_size = self.field_size + 8
        data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
        data[:header_size] = header.serialize()
        off = header_size
        for entry in entries:
            data[off:off+entry_size] = entry.serialize()
            off += entry_size
        return data
    

    def sync(self) -> None:
        ''' Sync this node into disk.
        '''
        if not self.data_modified: return
        pf_manager.write_page(self.file_id, self.header.page_no, self.serialize())
        self.data_modified = False
        
    
    def insert(self, field_value:Union[int, float, str], page_no:int, slot_no:int) -> None:
        ''' Insert an entry to this node recursively.
            Should use different strategies based on the node type.
            Should deal with entry overflow and split the node recursively.
            This interface must be called firstly on a leaf node
            (i.e. will not deal with the search process).
        args:
            field_value: Union[int, float, str], the field value in the entry.
            page_no: int, if this node is internal node, page_no means the child node page.
                If this node is leaf node, page_no means the rid.page_no.
            slot_no: int, if this node is internal node, slot_no must be INVALID.
                If this node is leaf node, slot_no means the rid.slot_no.
        '''
        (file_id, field_type, field_size, node_capacity) = \
            (self.file_id, self.field_type, self.field_size, self.node_capacity)
        header = self.header
        entries = self.entries
        if header.entry_number < node_capacity: # no need to split
            idx = self.search_child_idx(field_value)
            if idx > 0 and entries[idx-1].field_value == field_value:
                # encountered a reapeated entry in a leaf node
                if header.node_type != cf.NODE_TYPE_LEAF:
                    raise NodeInsertError(f'Encounter a repeated entry in a non-leaf node.')
                entry = entries[idx-1]
                if entry.slot_no == cf.INVALID:
                    bucket_page = entry.page_no
                    while True:     # find the first free bucket
                        bucket = IX_RidBucket.deserialize(pf_manager.read_page(file_id, bucket_page))
                        if bucket.free_space() > 0: break
                        if bucket.header.next_page == cf.INVALID:   # alloc a new bucket
                            new_page = pf_manager.append_page(file_id)
                            bucket.set_next_page(new_page)
                            bucket.sync(file_id, bucket_page)
                            bucket = IX_RidBucket()
                            bucket_page = new_page
                            break
                        bucket_page = bucket.header.next_page
                    bucket.insert_rid(RM_Rid(page_no, slot_no))
                    bucket.sync(file_id, bucket_page)
                else:   # alloc the first bucket page here
                    bucket_page = pf_manager.append_page(file_id)
                    bucket = IX_RidBucket()
                    bucket.insert_rid(RM_Rid(entry.page_no, entry.slot_no))
                    bucket.insert_rid(RM_Rid(page_no, slot_no))
                    bucket.sync(file_id, bucket_page)
                    entry.page_no = bucket_page
                    entry.slot_no = cf.INVALID
            else:   # a different node entry
                entries.insert(idx, IX_TreeNodeEntry(field_type,
                    field_size, field_value, page_no, slot_no))
                header.entry_number += 1
        else:   # overflow, need to split this node
            idx = self.search_child_idx(field_value)
            entries.insert(idx, IX_TreeNodeEntry(field_type, field_size, field_value, page_no, slot_no))
            mid_idx = (node_capacity+1) // 2
            # alloc a split new page
            new_page = pf_manager.append_page(file_id)
            # first determine the entry needed to pass upward
            (up_field_value, up_page_no, up_slot_no) = \
                (entries[mid_idx].field_value, new_page, cf.INVALID)
            # create a new node
            new_node = IX_TreeNode(file_id, field_type, field_size,
                node_capacity, header.node_type, header.parent, new_page)
            if header.node_type == cf.NODE_TYPE_INTER:
                new_node.header.entry_number = node_capacity // 2
                new_node.header.first_child = entries[mid_idx].page_no
                new_node.entries = entries[mid_idx+1:]
            elif header.node_type == cf.NODE_TYPE_LEAF:
                new_node.header.entry_number = node_capacity // 2 + 1
                new_node.entries = entries[mid_idx:]
            else: raise NodeInsertError(f'Wrong node type: {header.node_type}.')
            # modify the current node
            header.entry_number = mid_idx
            entries = entries[:mid_idx]
            # prev_sib and next_sib
            new_node.header.prev_sib = header.page_no
            new_node.header.next_sib = header.next_sib
            if header.next_sib != cf.INVALID:
                next_node = IX_TreeNode.deserialize(file_id, field_type, field_size,
                    node_capacity, pf_manager.read_page(file_id, header.next_sib))
                next_node.header.prev_sib = new_page
                next_node.data_modified = True
                next_node.sync()
            header.next_sib = new_page
            new_node.data_modified = True
            new_node.sync()
            # pass the overflowed entry upward
            if header.parent == cf.INVALID: # root spilled, need to create a new root
                if header.page_no != cf.INDEX_ROOT_PAGE:
                    raise NodeInsertError(f'Root page no {header.page_no} is not {cf.INDEX_ROOT_PAGE}.')
                # must swap the new node page with INDEX_ROOT_PAGE
                new_page = pf_manager.append_page(file_id)
                header.page_no = new_page
                header.parent = cf.INDEX_ROOT_PAGE
                if header.prev_sib != cf.INVALID:
                    raise NodeInsertError(f'Current root has a prev sib.')
                # set the parent of the new split node
                new_node.header.parent = cf.INDEX_ROOT_PAGE
                new_node.data_modified = True
                new_node.sync()
                if header.node_type != cf.NODE_TYPE_LEAF:
                    for entry in entries: # change children's parent
                        node = IX_TreeNode.deserialize(file_id, field_type, field_size,
                            node_capacity, pf_manager.read_page(file_id, entry.page_no))
                        node.header.parent = new_page
                        node.data_modified = True
                        node.sync()
                # new root node
                new_root = IX_TreeNode(file_id, field_type, field_size, node_capacity,
                    cf.NODE_TYPE_INTER, cf.INVALID, cf.INDEX_ROOT_PAGE)
                new_root.header.entry_number = 1
                new_root.header.first_child = header.page_no
                new_root.entries = IX_TreeNodeEntry(field_type, field_size,
                    up_field_value, up_page_no, up_slot_no)
                new_root.data_modified = True
                new_root.sync()
            else: # current node is not root, insert upward recursively
                node = IX_TreeNode.deserialize(file_id, field_type, field_size,
                    node_capacity, pf_manager.read_page(file_id, header.parent))
                node.insert(up_field_value, up_page_no, up_slot_no)
        self.data_modified = True
        self.sync()

    
if __name__ == '__main__':
    pass
