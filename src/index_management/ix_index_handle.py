import os
import sys
import time
import numpy as np
from typing import Tuple, List, Dict, Set, Union

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_tree_node import IX_TreeNodeHeader, IX_TreeNode
from index_management.ix_rid_bucket import IX_RidBucket
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
        
    
    def init_meta(self, meta:dict) -> None:
        ''' Init the index meta info. Use ONLY when the index was just created.
        args:
            meta: dict, data structure = {
                'field_type': int,                      # MUST, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
                'field_size': int,                      # MUST, the field size in bytes.
                'node_capacity': int,                   # OPTIONAL, will be calculated from field_size.
            } TO BE CONTINUED ...
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        (field_type, field_size) = meta['field_type'], meta['field_size']
        node_capacity = (cf.PAGE_SIZE - IX_TreeNodeHeader.size()) // (field_size + 8)
        if node_capacity < 2:
            raise IndexInitMetaError(f'Node capacity = {node_capacity} is too small.')
        self.meta = {'field_type': field_type, 'field_size': field_size, 'node_capacity': node_capacity}
        
    
    def read_meta(self) -> dict:
        ''' Read the meta info from .ixmeta file. Organized as a dict,
            update self.meta, and return it.
            The returned dict must be READ ONLY. Do not modify it out of this class.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        
    
    def sync_meta(self) -> None:
        ''' Sync self.meta to the .ixmeta file. Use this interface since we do not
            want to write .ixmeta file each time we modify the index entries.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        
    
    def min_leaf(self) -> int:
        ''' Search the min leaf node.
        return: int, the page_no of the min leaf node.
        '''
        meta, current_page = self.meta, 0
        while True:
            current_node = IX_TreeNode.deserialize(meta['field_type'], meta['field_size'],
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
        meta, current_page = self.meta, 0
        while True:
            current_node = IX_TreeNode.deserialize(meta['field_type'], meta['field_size'],
                meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            current_page = current_node.entries[-1].page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the min leaf node.')
        return current_page
        
    
    def search_leaf(self, field_value:Union[int, float, str]) -> int:
        ''' Search the leaf node by a field value in this index.
        return: int, the page_no of the leaf node.
        '''
        meta, current_page = self.meta, 0
        while True:
            current_node = IX_TreeNode.deserialize(meta['field_type'], meta['field_size'],
                meta['node_capacity'], pf_manager.read_page(self.data_file_id, current_page))
            if current_node.header.node_type != cf.NODE_TYPE_INTER: break
            child_idx = current_node.search_child_idx(field_value)
            if child_idx == 0:
                current_page = current_node.header.first_child
            else: current_page = current_node.entries[child_idx-1].page_no
        if current_node.header.node_type != cf.NODE_TYPE_LEAF:
            raise IndexSearchError(f'Failed to find the leaf node.')
        return current_page
    
    
    def insert_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Insert an entry to the index.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
    
    
    def remove_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Remove an entry.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')


if __name__ == '__main__':
    pass
