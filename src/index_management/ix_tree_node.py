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
    
    
    def __init__(self, node_type:int, page_no:int, entry_number:int,
        prev_sib:int, next_sib:int, first_child:int) -> None:
        ''' Init the TreeNodeHeader.
        args:
            node_type: int, in {NODE_TYPE_INTER, NODE_TYPE_LEAF}.
            page_no: int, page number of the current tree node.
            entry_number: int, the valid entry number in this node.
            prev_sib: int, page number of the previous sibling.
            next_sib: int, page number of the next sibling.
            first_child: int, the page number of the first child (the smallest one),
                will be ignored if node_type == NODE_TYPE_LEAF.
        '''
        (self.node_type, self.page_no, self.entry_number,
            self.prev_sib, self.next_sib, self.first_child) \
            = (node_type, page_no, entry_number, prev_sib, next_sib, first_child)
            
    
    def __str__(self) -> str:
        return f'{{{self.node_type}, {self.page_no}, {self.entry_number}, ' \
            + f'{self.prev_sib}, {self.next_sib}, {self.first_child}}}'
            
            
    @staticmethod
    def size():
        return 24
    
    
    @staticmethod
    def deserialize(data:np.ndarray):
        ''' Deserialize np.ndarray[>=IX_TreeNodeHeader.size(), uint8] to tree node header.
        '''
        buffer = data[:IX_TreeNodeHeader.size()].tobytes()
        return IX_TreeNodeHeader(*struct.unpack(f'{cf.BYTE_ORDER}iiiiii', buffer))
    

    def serialize(self) -> np.ndarray:
        ''' Serialize tree node header to np.ndarray[IX_TreeNodeHeader.size(), uint8].
        '''
        buffer = struct.pack(f'{cf.BYTE_ORDER}iiiiii', self.node_type, self.page_no,
            self.entry_number, self.prev_sib, self.next_sib, self.first_child)
        return np.frombuffer(buffer, dtype=np.uint8)
    

class IX_TreeNodeEntry:
    
    
    def __init__(self, field_types:List[int], field_sizes:List[int],
        field_values:List[Union[int,float,str]], page_no:int, slot_no:int, verbose:int) -> None:
        ''' Init a tree node entry.
        args:
            field_types: List[int], in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_sizes: List[int], field size in bytes.
            field_values: List[Union[int,float,str]], the unserialized field value.
            page_no: int, the page_no in the entry pointer.
            slot_no: int, the slot_no in the entry pointer.
            verbose: int, the verbose field value.
        '''
        (self.field_types, self.field_sizes, self.field_values, self.page_no, self.slot_no,
            self.verbose) = field_types, field_sizes, field_values, page_no, slot_no, verbose
        self.total_field_size = np.sum(field_sizes)
        
    
    def size(self):
        ''' Return the entry size.
        '''
        return self.total_field_size + 12
    

    @staticmethod
    def eq(values1:List[Union[int,float,str]], values2:List[Union[int,float,str]]):
        for v1, v2 in zip(values1, values2):
            if v1 != v2: return False
        return True
    

    @staticmethod
    def le(values1:List[Union[int,float,str]], values2:List[Union[int,float,str]]):
        for v1, v2 in zip(values1, values2):
            if v1 < v2: return True
            elif v1 > v2: return False
        return True
    

    @staticmethod
    def lt(values1:List[Union[int,float,str]], values2:List[Union[int,float,str]]):
        for v1, v2 in zip(values1, values2):
            if v1 < v2: return True
            elif v1 > v2: return False
        return False
    

    @staticmethod
    def deserialize(field_types:List[int], field_sizes:List[int], data:np.ndarray):
        ''' Deserialize np.ndarray[>=total_field_size+12, uint8] into tree node entry,
            with specific field_type and field_size.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        field_values, off = [], 0
        for field_type, field_size in zip(field_types, field_sizes):
            if field_type == cf.TYPE_INT:
                field_values.append(struct.unpack(f'{BYTE_ORDER}i', data[off:off+field_size].tobytes())[0])
            elif field_type == cf.TYPE_FLOAT:
                field_values.append(struct.unpack(f'{BYTE_ORDER}d', data[off:off+field_size].tobytes()[0]))
            elif field_type == cf.TYPE_STR:
                field_value, = struct.unpack(f'{BYTE_ORDER}{field_size}s', data[off:off+field_size].tobytes())
                field_values.append(str(field_value, encoding='utf-8').strip('\0'))
            else: raise IndexFieldTypeError(f'Field type {field_type} is invalid.')
            off += field_size
        (page_no, slot_no, verbose) = struct.unpack(f'{BYTE_ORDER}iii', data[off:off+12])
        return IX_TreeNodeEntry(field_types, field_sizes, field_values, page_no, slot_no, verbose)
        
        
    def serialize(self) -> np.ndarray:
        ''' Serialize tree node entry into np.ndarray[total_field_size+12, uint8].
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        (field_types, field_sizes, field_values, total_field_size) = \
            (self.field_types, self.field_sizes, self.field_values, self.total_field_size)
        data = np.zeros(total_field_size+12, dtype=np.uint8)
        off = 0
        for field_type, field_size, field_value in zip(field_types, field_sizes, field_values):
            if field_type == cf.TYPE_INT:
                data[off:off+field_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', field_value), dtype=np.uint8)
            elif field_type == cf.TYPE_FLOAT:
                data[off:off+field_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}d', field_value), dtype=np.uint8)
            elif field_type == cf.TYPE_STR:
                buffer = bytes(field_value[:field_size].ljust(field_size, '\0'))
                data[off:off+field_size] = np.frombuffer(buffer, dtype=np.uint8)
            else: raise IndexFieldTypeError(f'Field type {field_type} is invalid.')
            off += field_size
        data[total_field_size:total_field_size+12] = np.frombuffer(
            struct.pack(f'{BYTE_ORDER}iii', self.page_no, self.slot_no, self.verbose), dtype=np.uint8)
        return data
    
    
    def get_all_rids(self, data_file_id:int) -> List[Tuple[RM_Rid,int]]:
        ''' Get all rids under this entry. Used ONLY when this is a leaf node entry.
        args:
            data_file_id: int, the index data file id.
        '''
        if self.page_no == cf.INVALID and self.slot_no == cf.INVALID: return []
        if self.slot_no != cf.INVALID:
            return [(RM_Rid(self.page_no, self.slot_no), self.verbose)]
        bucket_page = self.page_no
        res = []
        while bucket_page != cf.INVALID:
            bucket = IX_RidBucket.deserialize(pf_manager.read_page(data_file_id, bucket_page))
            res.extend(bucket.get_all_rids())
            bucket_page = bucket.header.next_page
        return res
        

class IX_TreeNode:
    
    
    def __init__(self, file_id:int, field_types:List[int], field_sizes:List[int],
            node_capacity:int, node_type:int, page_no:int):
        ''' Init an empty tree node.
        args:
            file_id: int, the data file id of this node.
            field_types: List[int], in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_sizes: List[int], entry key size in bytes, not including page_no and slot_no.
            node_capacity: int, how many entries a node can store.
            node_type: int, in {NODE_TYPE_INTER, NODE_TYPE_LEAF}.
            page_no: int, page number of the current tree node.
        '''
        (self.file_id, self.field_types, self.field_sizes, self.node_capacity) = \
            (file_id, field_types, field_sizes, node_capacity)
        self.total_field_size = np.sum(field_sizes)
        self.header = IX_TreeNodeHeader(node_type=node_type,
            page_no=page_no, entry_number=0, prev_sib=cf.INVALID,
            next_sib=cf.INVALID, first_child=cf.INVALID)
        self.data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
        self.data[:IX_TreeNodeHeader.size()] = self.header.serialize()
        self.data_modified = True
        
    
    def __str__(self) -> str:
        header = self.header
        entries = self.get_all_entries()
        to_str = lambda x : str(x.field_values)
        res = f'{{{header.page_no}, [{", ".join(map(to_str, entries))}]}}'
        if header.node_type == cf.NODE_TYPE_INTER: return res
        for entry in entries:
            rids = f'{entry.field_values}: '
            for rid, verbose in entry.get_all_rids(self.file_id):
                rids += f'({rid.page_no},{rid.slot_no},{verbose}), '
            res += f'\n{rids}'
        return res
    

    def get_entry(self, idx:int) -> IX_TreeNodeEntry:
        entry_size = self.total_field_size + 12
        off = IX_TreeNodeHeader.size() + idx * entry_size
        return IX_TreeNodeEntry.deserialize(self.field_types,
            self.field_sizes, self.data[off:off+entry_size])
        
        
    def get_all_entries(self) -> List[IX_TreeNodeEntry]:
        entries = []
        field_types, field_sizes, data = self.field_types, self.field_sizes, self.data
        entry_number = self.header.entry_number
        entry_size = self.total_field_size + 12
        off = IX_TreeNodeHeader.size()
        for _ in range(entry_number):
            entries.append(IX_TreeNodeEntry.deserialize(
                field_types, field_sizes, data[off:off+entry_size]))
            off += entry_size
        return entries
        
    
    def set_entry(self, idx:int, entry:IX_TreeNodeEntry) -> None:
        entry_size = self.total_field_size + 12
        off = IX_TreeNodeHeader.size() + idx * entry_size
        self.data[off:off+entry_size] = entry.serialize()
        
    
    def search_child_idx(self, field_values:List[Union[int,float,str]]) -> int:
        ''' Search the child index of a list of values in the node entries.
            Suppose there are N entries: K_0, K_1, ..., K_{N-1},
            if field_value in [K_{i-1}, K_i), return i;
            else if field_value < K_0, return 0;
            else if field_value >= K_{N-1}, return N;
            if there are no entries, return 0 directly.
            The returned index can further be interpreted as the insert position.
        args:
            field_values: List[Union[int,float,str]], type must match with self.field_types.
        return: int, the searched key position.
        '''
        entry_number = self.header.entry_number
        if entry_number == 0: return 0
        if IX_TreeNodeEntry.le(self.get_entry(entry_number-1).field_values, field_values):
            return entry_number
        left, right, mid = 0, entry_number, 0
        while left < right:
            mid = (left + right) // 2
            if IX_TreeNodeEntry.le(self.get_entry(mid).field_values, field_values):
                left = mid + 1
            else: right = mid
        return left
        
    
    @staticmethod
    def deserialize(file_id:int, field_types:int, field_sizes:int, node_capacity:int, data:np.ndarray):
        ''' Deserialize a data page into tree node.
        '''
        cf.NODE_DESERIALIZE_CNT += 1
        node = IX_TreeNode(file_id, field_types, field_sizes, node_capacity, 0, 0)
        node.header = IX_TreeNodeHeader.deserialize(data[:IX_TreeNodeHeader.size()])
        node.data[:] = data[:]
        return node
        
    
    def serialize(self) -> np.ndarray:
        ''' Serialize this tree node into a data page.
        '''
        cf.NODE_SERIALIZE_CNT += 1
        return self.data.copy()
    

    def sync(self) -> None:
        ''' Sync this node into disk.
        '''
        if not self.data_modified: return
        pf_manager.write_page(self.file_id, self.header.page_no, self.serialize())
        self.data_modified = False
        
    
    def print_subtree(self, depth=0):
        lines = self.__str__().split('\n')
        print(f' '*4*depth, end=''); print(lines[0])
        if len(lines) > 0:
            for line in lines[1:]:
                print(f' '*4*(depth+1), end=''); print(line)
        if self.header.node_type == cf.NODE_TYPE_INTER:
            for page_no in [self.header.first_child] + [x.page_no for x in self.get_all_entries()]:
                node = IX_TreeNode.deserialize(self.file_id,
                    self.field_types, self.field_sizes, self.node_capacity,
                    pf_manager.read_page(self.file_id, page_no))
                node.print_subtree(depth+1)
        
    
    def insert(self, field_values:List[Union[int,float,str]], page_no:int, slot_no:int, verbose:int,
        ancestors:Tuple[int]) -> None:
        ''' Insert an entry to this node recursively.
            Should use different strategies based on the node type.
            Should deal with entry overflow and split the node recursively.
            This interface must be called firstly on a leaf node
            (i.e. will not deal with the search process).
        args:
            field_values: List[Union[int,float,str]], the field values in the entry.
            page_no: int, if this node is internal node, page_no means the child node page.
                If this node is leaf node, page_no means the rid.page_no.
            slot_no: int, if this node is internal node, slot_no must be INVALID.
                If this node is leaf node, slot_no means the rid.slot_no.
            verbose: int, the verbose field.
            ancestors: Tuple[int], the ancestors of this node from top to bottom,
                meaning that ancestors[-1] is the parent of this node, and if
                len(ancestors) == 0, this node must be the root.
        '''
        (file_id, field_types, field_sizes, node_capacity) = \
            (self.file_id, self.field_types, self.field_sizes, self.node_capacity)
        header = self.header
        header_size = IX_TreeNodeHeader.size()
        entry_size = self.total_field_size + 12
        data = self.data
        idx = self.search_child_idx(field_values)
        repeated = idx > 0 and IX_TreeNodeEntry.eq(self.get_entry(idx-1).field_values, field_values)
        if header.entry_number < node_capacity or repeated: # no need to split
            if repeated: # encountered a reapeated entry in a leaf node
                if header.node_type != cf.NODE_TYPE_LEAF:
                    raise NodeInsertError(f'Encounter a repeated entry in a non-leaf node.')
                entry = self.get_entry(idx-1)
                if entry.page_no == cf.INVALID and entry.slot_no == cf.INVALID:
                    # the only one rid has been removed
                    entry.page_no = page_no
                    entry.slot_no = slot_no
                    self.set_entry(idx-1, entry)
                elif entry.slot_no == cf.INVALID:
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
                    bucket.insert_rid(RM_Rid(page_no, slot_no), verbose)
                    bucket.sync(file_id, bucket_page)
                else:   # alloc the first bucket page here
                    bucket_page = pf_manager.append_page(file_id)
                    bucket = IX_RidBucket()
                    bucket.insert_rid(RM_Rid(entry.page_no, entry.slot_no), entry.verbose)
                    bucket.insert_rid(RM_Rid(page_no, slot_no), verbose)
                    bucket.sync(file_id, bucket_page)
                    entry.page_no = bucket_page
                    entry.slot_no = cf.INVALID
                    self.set_entry(idx-1, entry)
            else:   # a different node entry
                off = header_size + header.entry_number * entry_size
                for _ in range(header.entry_number, idx, -1):
                    data[off:off+entry_size] = data[off-entry_size:off]
                    off -= entry_size
                data[off:off+entry_size] = IX_TreeNodeEntry(field_types,
                    field_sizes, field_values, page_no, slot_no, verbose).serialize()
                header.entry_number += 1
        else:   # overflow, need to split this node
            entries = self.get_all_entries()
            insert_idx = self.search_child_idx(field_values)
            entries.insert(insert_idx, IX_TreeNodeEntry(field_types,
                field_sizes, field_values, page_no, slot_no, verbose))
            mid_idx = (node_capacity+1) // 2
            # alloc a split new page
            new_page = pf_manager.append_page(file_id)
            # first determine the entry needed to pass upward
            (up_field_values, up_page_no, up_slot_no, up_verbose) = \
                (entries[mid_idx].field_values, new_page, cf.INVALID, cf.INVALID)
            # create a new node
            new_node = IX_TreeNode(file_id, field_types, field_sizes,
                node_capacity, header.node_type, new_page)
            if header.node_type == cf.NODE_TYPE_INTER:
                new_node.header.entry_number = node_capacity // 2
                new_node.header.first_child = entries[mid_idx].page_no
                for i in range(new_node.header.entry_number):
                    new_node.set_entry(i, entries[mid_idx+1+i])
            elif header.node_type == cf.NODE_TYPE_LEAF:
                new_node.header.entry_number = node_capacity // 2 + 1
                for i in range(new_node.header.entry_number):
                    new_node.set_entry(i, entries[mid_idx+i])
            else: raise NodeInsertError(f'Wrong node type: {header.node_type}.')
            # modify the current node
            header.entry_number = mid_idx
            for i in range(header.entry_number):
                self.set_entry(i, entries[i])
            # prev_sib and next_sib
            new_node.header.prev_sib = header.page_no
            new_node.header.next_sib = header.next_sib
            if header.next_sib != cf.INVALID:
                next_node = IX_TreeNode.deserialize(file_id, field_types, field_sizes,
                    node_capacity, pf_manager.read_page(file_id, header.next_sib))
                next_node.header.prev_sib = new_page
                next_node.data_modified = True
                next_node.sync()
            header.next_sib = new_page
            new_node.data[:header_size] = new_node.header.serialize()
            new_node.data_modified = True
            new_node.sync()
            # pass the overflowed entry upward
            if len(ancestors) == 0: # root spilled, need to create a new root
                if header.page_no != cf.INDEX_ROOT_PAGE:
                    raise NodeInsertError(f'Root page no {header.page_no} is not {cf.INDEX_ROOT_PAGE}.')
                # must swap the new node page with INDEX_ROOT_PAGE
                new_page = pf_manager.append_page(file_id)
                header.page_no = new_page
                if header.prev_sib != cf.INVALID:
                    raise NodeInsertError(f'Current root has a prev sib.')
                # new root node
                new_root = IX_TreeNode(file_id, field_types, field_sizes,
                    node_capacity, cf.NODE_TYPE_INTER, cf.INDEX_ROOT_PAGE)
                new_root.header.entry_number = 1
                new_root.header.first_child = header.page_no
                new_root.set_entry(0, IX_TreeNodeEntry(field_types, field_sizes,
                    up_field_values, up_page_no, up_slot_no, up_verbose))
                new_root.data[:header_size] = new_root.header.serialize()
                new_root.data_modified = True
                new_root.sync()
            else: # current node is not root, insert upward recursively
                up_node = IX_TreeNode.deserialize(file_id, field_types, field_sizes,
                    node_capacity, pf_manager.read_page(file_id, ancestors[-1]))
                up_node.insert(up_field_values, up_page_no, up_slot_no, up_verbose, ancestors[:-1])
        self.data[:header_size] = header.serialize()
        self.data_modified = True
        self.sync()
        
    
    def remove(self, field_values:List[Union[int,float,str]], page_no:int, slot_no:int) -> None:
        ''' Lasily mark the rid as invalid to remove data, without changing the
            actual data structures of the B+ tree.
        '''
        header = self.header
        if header.node_type != cf.NODE_TYPE_LEAF:
            raise NodeRemoveError(f'Trying to remove an entry from a non-leaf node.')
        (file_id, field_types, field_sizes, node_capacity) = \
            (self.file_id, self.field_types, self.field_sizes, self.node_capacity)
        idx = self.search_child_idx(field_values)
        if idx <= 0 or not IX_TreeNodeEntry.eq(self.get_entry(idx-1).field_values, field_values):
            return
        entry = self.get_entry(idx-1)
        if entry.page_no == cf.INVALID and entry.slot_no == cf.INVALID: return
        if entry.page_no == page_no and entry.slot_no == slot_no:
            entry.page_no = cf.INVALID
            entry.slot_no = cf.INVALID
            self.set_entry(idx-1, entry)
            self.sync()
        elif entry.slot_no == cf.INVALID:   # point to a rid bucket
            bucket_page = entry.page_no
            while True:
                bucket = IX_RidBucket.deserialize(pf_manager.read_page(file_id, bucket_page))
                slot = bucket.search_rid(page_no, slot_no)
                if slot != cf.INVALID:
                    bucket.remove_rid(slot)
                    bucket.sync(file_id, bucket_page)
                    break
                if bucket.header.next_page == cf.INVALID: break
                bucket_page = bucket.header.next_page
                
    
    def modify_verbose(self, field_values:List[Union[int,float,str]], delta:int) -> List[int]:
        ''' Add delta to all verbose fileds of the field values.
            Return the modified verbose values.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        header = self.header
        if header.node_type != cf.NODE_TYPE_LEAF:
            raise NodeRemoveError(f'Trying to remove an entry from a non-leaf node.')
        (file_id, field_types, field_sizes, node_capacity) = \
            (self.file_id, self.field_types, self.field_sizes, self.node_capacity)
        idx = self.search_child_idx(field_values)
        if idx <= 0 or not IX_TreeNodeEntry.eq(self.get_entry(idx-1).field_values, field_values):
            return
        entry = self.get_entry(idx-1)
        if entry.page_no == cf.INVALID and entry.slot_no == cf.INVALID: return []
        if entry.slot_no != cf.INVALID:
            entry.verbose += delta
            self.set_entry(idx-1, entry)
            self.sync()
            return [entry.verbose]
        res = []
        bucket_page = entry.page_no
        while True:
            bucket = IX_RidBucket.deserialize(pf_manager.read_page(file_id, bucket_page))
            base_off = bucket.header.size() + bucket.bitmap.size
            entry_size = 12
            for i in bucket.bitmap.occupied_slots():
                off = base_off + i * entry_size + 8
                verbose, = struct.unpack(f'{BYTE_ORDER}i', bucket.data[off:off+4].tobytes())
                bucket.data[off:off+4] = np.frombuffer(
                    struct.pack(f'{BYTE_ORDER}i', verbose+delta), dtype=np.uint8)
                res.append(verbose + delta)
            bucket.data_modified = True
            bucket.sync(self.file_id, bucket_page)
            if bucket.header.next_page == cf.INVALID: break
            bucket_page = bucket.header.next_page
        return res
                
    
if __name__ == '__main__':
    pass
