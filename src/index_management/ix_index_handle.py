import os
import struct
import numpy as np
from typing import Tuple, List, Dict, Set, Union

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_tree_node import IX_TreeNodeHeader, IX_TreeNode
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
        data = np.ndarray(cf.PAGE_SIZE, dtype=np.uint8)
        data[:12] = np.frombuffer(struct.pack(f'{cf.BYTE_ORDER}iii', meta['field_type'],
            meta['field_size'], meta['node_capacity']), dtype=np.uint8)
        return data
        
    
    @staticmethod
    def _desetialize_meta(data:np.ndarray) -> dict:
        ''' Deserialize np.ndarray[PAGE_SIZE, uint8] into meta dict.
        '''
        (field_type, field_size, node_capacity) = \
            struct.unpack(f'{cf.BYTE_ORDER}iii', data[:12].tobytes())
        meta = {'field_type': field_type, 'field_size': field_size,
            'node_capacity': node_capacity}
        return meta
    

    def min_leaf(self) -> int:
        ''' Search the min leaf node.
        return: int, the page_no of the min leaf node.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        while True:
            current_node = IX_TreeNode.deserialize(self.data_file_id, meta['field_type'], meta['field_size'],
                meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            current_page = current_node.header.first_child
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the min leaf node.')
        return current_page
    
    
    def max_leaf(self) -> int:
        ''' Search the max leaf node.
        return: int, the page_no of the min leaf node.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        while True:
            current_node = IX_TreeNode.deserialize(self.data_file_id, meta['field_type'], meta['field_size'],
                meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            entry_number = current_node.header.entry_number
            current_page = current_node.get_entry(entry_number-1).page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the min leaf node.')
        return current_page
        
    
    def search_leaf(self, field_value:List[Union[int,float,str]]) -> Tuple[int, Tuple[int]]:
        ''' Search the leaf node by a field value in this index.
        return: Tuple[int, Tuple[int]], the page_no of the leaf node and its ancestors.
        '''
        meta, current_page = self.meta, cf.INDEX_ROOT_PAGE
        ancestors = []
        while True:
            current_node = IX_TreeNode.deserialize(self.data_file_id, meta['field_type'], meta['field_size'],
                meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            ancestors.append(current_page)
            child_idx = current_node.search_child_idx(field_value)
            if child_idx == 0:
                current_page = current_node.header.first_child
            else: current_page = current_node.get_entry(child_idx-1).page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the leaf node.')
        return current_page, tuple(ancestors)
        
    
    def init_meta(self, meta:dict) -> None:
        ''' Init the index meta info. Use ONLY when the index was just created.
        args:
            meta: dict, data structure = {
                'field_number': int,                    # MUST, the number of fields to be indexed.
                'fields': List[Tuple[int,int]],         # MUST, the fields to be indexed.
                                                          the two ints in each tuple are field_type and field_size.
                                                          field_type must be in {TYPE_INT, TYPE_FLOAT, TYPE_STR},
                                                          field_size must be in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
                'verbose_en': bool,                     # MUST, whether to enable verbose field in the index or not.
                'node_capacity': int,                   # OPTIONAL, will be calculated from fields.
            } TO BE CONTINUED ...
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        (field_type, field_size) = meta['field_type'], meta['field_size']
        node_capacity = (cf.PAGE_SIZE - IX_TreeNodeHeader.size()) // (field_size + 8)
        if node_capacity < 2:
            raise IndexInitMetaError(f'Node capacity = {node_capacity} is too small.')
        meta = {'field_type': field_type, 'field_size': field_size, 'node_capacity': node_capacity}
        self.meta = meta
        meta_page = IX_IndexHandle._serialize_meta(meta)
        pf_manager.append_page(self.meta_file_id, meta_page)
        self.meta_modified = False
        # create root node
        root_page = pf_manager.append_page(self.data_file_id)
        if root_page != cf.INDEX_ROOT_PAGE:
            raise IndexInitMetaError(f'Root node has not been allocated on page {cf.INDEX_ROOT_PAGE}.')
        root_node = IX_TreeNode(self.data_file_id, meta['field_type'], meta['field_size'],
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
        
    
    def insert_entry(self, field_value:List[Union[int,float,str]],
            rid:RM_Rid, verbose:int=0) -> None:
        ''' Insert an entry to the index.
        args:
            field_value: List[Union[int,float,str]], a list of fields to be indexed.
            rid: RM_Rid, the record rid.
            verbose: int, the verbose field. If verbose_en is False, this arg will be ignored.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        meta = self.meta
        leaf_page, ancestors = self.search_leaf(field_value)
        leaf_node = IX_TreeNode.deserialize(self.data_file_id, meta['field_type'], meta['field_size'],
            meta['node_capacity'], pf_manager.read_page(self.data_file_id, leaf_page))
        leaf_node.insert(field_value, rid.page_no, rid.slot_no, ancestors)
    
    
    def remove_entry(self, field_value:List[Union[int,float,str]], rid:RM_Rid) -> None:
        ''' Remove an entry.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        meta = self.meta
        leaf_page, _ = self.search_leaf(field_value)
        leaf_node = IX_TreeNode.deserialize(self.data_file_id, meta['field_type'], meta['field_size'],
            meta['node_capacity'], pf_manager.read_page(self.data_file_id, leaf_page))
        leaf_node.remove(field_value, rid.page_no, rid.slot_no)
        
    
    def modify_verbose(self, field_value:List[Union[int,float,str]], delta:int) -> List[int]:
        ''' Modify all verbose fields of the field_value in the index.
            Use ONLY when verbose_en is True.
        args:
            delta: int, will act like this: verbose += delta.
        return:
            List[int], the modified verbose values.
        '''
        
    

if __name__ == '__main__':
    pass
