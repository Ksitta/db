import os
import struct
import numpy as np
from typing import Tuple, List, Dict, Set, Union

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_tree_node import IX_TreeNodeHeader, IX_TreeNode, node_cache
from errors.err_index_management import *



class IX_IndexHandle:
    
    
    def __init__(self, file_name:str, index_no:int, meta_file_id:int, data_file_id:int) -> None:
        ''' Init the index handle for <file_name>.<index_no>.
        args:
            file_name: str, the name of the file to be indexed, without extension.
            index_no: int, the index number on the file.
            meta_file_id: int, the meta file id of the index.
            data_file_id: int, the data file id of the index.
        '''
        self.file_name = file_name
        self.index_no = index_no
        self.meta_file_id = meta_file_id
        self.data_file_id = data_file_id
        self.meta = dict()
        self.meta_modified = False
        self.is_opened = True
        
        
    @staticmethod
    def _serialize_meta(meta:dict) -> np.ndarray:
        ''' Serialize meta into np.ndarray[PAGE_SIZE, uint8].
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        data = np.ndarray(cf.PAGE_SIZE, dtype=np.uint8)
        field_number, fields, node_capacity = meta['field_number'], meta['fields'], meta['node_capacity']
        data[:4] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', field_number), dtype=np.uint8)
        off = 4
        for field_type, field_size in fields:
            data[off:off+8] = np.frombuffer(struct.pack(f'{BYTE_ORDER}ii',
                field_type, field_size), dtype=np.uint8); off += 8
        data[off:off+4] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', node_capacity), dtype=np.uint8)
        return data
        
    
    @staticmethod
    def _desetialize_meta(data:np.ndarray) -> dict:
        ''' Deserialize np.ndarray[PAGE_SIZE, uint8] into meta dict.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        field_number, = struct.unpack(f'{BYTE_ORDER}i', data[:4].tobytes())
        fields, off = [], 4
        for _ in range(field_number):
            fields.append(struct.unpack(f'{BYTE_ORDER}ii', data[off:off+8].tobytes()))
            off += 8
        node_capacity, = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4].tobytes())
        meta = {'field_number': field_number, 'fields': fields, 'node_capacity': node_capacity}
        return meta
    

    def min_leaf(self) -> IX_TreeNode:
        ''' Search the min leaf node.
        return: IX_TreeNode, the min leaf node.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        fields = meta['fields']
        field_types = [field[0] for field in fields]
        field_sizes = [field[1] for field in fields]
        while True:
            current_node = node_cache[self.data_file_id].get(current_page)
            if current_node is None:
                current_node = IX_TreeNode.deserialize(self.data_file_id, field_types, field_sizes,
                    meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
                node_cache[self.data_file_id][current_page] = current_node
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            current_page = current_node.header.first_child
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the min leaf node.')
        return current_node
    
    
    def max_leaf(self) -> IX_TreeNode:
        ''' Search the max leaf node.
        return: IX_TreeNode, the max leaf node.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        fields = meta['fields']
        field_types = [field[0] for field in fields]
        field_sizes = [field[1] for field in fields]
        while True:
            current_node = node_cache[self.data_file_id].get(current_page)
            if current_node is None:
                current_node = IX_TreeNode.deserialize(self.data_file_id, field_types, field_sizes,
                    meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
                node_cache[self.data_file_id][current_page] = current_node
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            entry_number = current_node.header.entry_number
            current_page = current_node.get_entry(entry_number-1).page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the min leaf node.')
        return current_node
        
    
    def search_leaf(self, field_value:List[Union[int,float,str]]) -> Tuple[IX_TreeNode, Tuple[int]]:
        ''' Search the leaf node by a field value in this index.
        return: Tuple[IX_TreeNode, Tuple[int]], the leaf node and its ancestors' page numbers.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        fields = meta['fields']
        field_types = [field[0] for field in fields]
        field_sizes = [field[1] for field in fields]
        ancestors = []
        while True:
            current_node = node_cache[self.data_file_id].get(current_page)
            if current_node is None:
                current_node = IX_TreeNode.deserialize(self.data_file_id, field_types, field_sizes,
                    meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
                node_cache[self.data_file_id][current_page] = current_node
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            ancestors.append(current_page)
            child_idx = current_node.search_child_idx(field_value)
            if child_idx == 0:
                current_page = current_node.header.first_child
            else: current_page = current_node.get_entry(child_idx-1).page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the leaf node.')
        return current_node, tuple(ancestors)
        
    
    def init_meta(self, meta:dict) -> None:
        ''' Init the index meta info. Use ONLY when the index was just created.
        args:
            meta: dict, data structure = {
                'field_number': int,                    # MUST, the number of fields to be indexed.
                'fields': List[Tuple[int,int]],         # MUST, the fields to be indexed.
                                                          The two ints in each tuple are field_type and field_size.
                                                          field_type must be in {TYPE_INT, TYPE_FLOAT, TYPE_STR},
                                                          field_size must be in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
                'node_capacity': int,                   # OPTIONAL, will be calculated from fields.
            } TO BE CONTINUED ...
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        field_number, fields = meta['field_number'], meta['fields']
        if field_number > (cf.PAGE_SIZE - 8) // 8:
            raise IndexInitMetaError(f'Index meta overflowed, must be in one page.')
        total_field_size = np.sum([field[1] for field in fields[:field_number]])
        node_capacity = (cf.PAGE_SIZE - IX_TreeNodeHeader.size()) // (total_field_size + 12)
        if node_capacity < 2:
            raise IndexInitMetaError(f'Node capacity = {node_capacity} is too small.')
        meta = {'field_number': field_number, 'fields': fields[:field_number], 'node_capacity': node_capacity}
        self.meta = meta
        meta_page = IX_IndexHandle._serialize_meta(meta)
        pf_manager.append_page(self.meta_file_id, meta_page)
        self.meta_modified = False
        # create root node
        root_page = pf_manager.append_page(self.data_file_id)
        if root_page != cf.INDEX_ROOT_PAGE:
            raise IndexInitMetaError(f'Root node has not been allocated on page {cf.INDEX_ROOT_PAGE}.')
        field_types = [field[0] for field in fields[:field_number]]
        field_sizes = [field[1] for field in fields[:field_number]]
        root_node = IX_TreeNode(self.data_file_id, field_types, field_sizes,
            meta['node_capacity'], cf.NODE_TYPE_LEAF, cf.INDEX_ROOT_PAGE)
        root_node.sync()
        
    
    def read_meta(self) -> dict:
        ''' Read the meta info from .ixmeta file. Organized as a dict,
            update self.meta, and return it.
            The returned dict must be READ ONLY. Do not modify it out of this class.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        meta_page = pf_manager.read_page(self.meta_file_id, 0)
        self.meta = IX_IndexHandle._desetialize_meta(meta_page)
        self.meta_modified = False
        return self.meta
        
    
    def sync_meta(self) -> None:
        ''' Sync self.meta to the .ixmeta file. Use this interface since we do not
            want to write .ixmeta file each time we modify the index entries.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        if not self.meta_modified: return
        meta_page = IX_IndexHandle._serialize_meta(self.meta)
        pf_manager.write_page(self.meta_file_id, 0, meta_page)
        self.meta_modified = False
        
    
    def insert_entry(self, field_values:List[Union[int,float,str]],
            rid:RM_Rid, verbose:int=0) -> None:
        ''' Insert an entry to the index.
        args:
            field_values: List[Union[int,float,str]], a list of fields to be indexed.
            rid: RM_Rid, the record rid.
            verbose: int, the verbose field, default = 0.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        leaf_node, ancestors = self.search_leaf(field_values)
        leaf_node.insert(field_values, rid.page_no, rid.slot_no, verbose, ancestors)
    
    
    def remove_entry(self, field_values:List[Union[int,float,str]], rid:RM_Rid) -> None:
        ''' Remove an entry.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        leaf_node, _ = self.search_leaf(field_values)
        leaf_node.remove(field_values, rid.page_no, rid.slot_no)
        
    
    def modify_verbose(self, field_values:List[Union[int,float,str]], delta:int) -> List[int]:
        ''' Modify all verbose fields of the field_value in the index.
            Use ONLY when verbose_en is True.
        args:
            delta: int, will act like this: verbose += delta.
        return:
            List[int], the modified verbose values.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        leaf_node, _ = self.search_leaf(field_values)
        return leaf_node.modify_verbose(field_values, delta)
        

if __name__ == '__main__':
    pass
