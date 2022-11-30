import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from utils.enums import CompOp
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_index_handle import IX_IndexHandle
from index_management.ix_tree_node import IX_TreeNode
from index_management.ix_rid_bucket import IX_RidBucket
from errors.err_index_management import *


class IX_IndexScan:
    
    
    def __init__(self):
        ''' Init the index scan.
        '''
        self.index_handle:IX_IndexHandle = None
        self.is_opened = False
    
    
    def open_scan(self, index_handle:IX_IndexHandle, comp_op:CompOp=CompOp.NO,
        field_value:Union[int, float, str]=None) -> None:
        ''' Open the index scan.
        args:
            index_handle: IX_IndexHandle, an opened index handle instance.
            comp_op: CompOp, the filter condition. If comp_op == CompOp.NO,
                field_value will be ignored.
            field_value: Union[int, float, str], the value to be compared with.
        '''
        self.is_opened = True
        self.index_handle = index_handle
        if not index_handle.is_opened:
            raise IndexOpenScanError(f'The index to be scanned is not opened.')
        self.comp_op, self.field_value = comp_op, field_value
    
    
    def next(self) -> RM_Rid:
        ''' Yield the next scanned rid that satisfies the filtering condition.
        '''
        if not self.is_opened: return None
        index_handle = self.index_handle
        meta = index_handle.meta
        (field_type, field_size, node_capacity) = \
            (meta['field_type'], meta['field_size'], meta['node_capacity'])
        data_file_id = index_handle.data_file_id
        comp_op, field_value = self.comp_op, self.field_value
        if comp_op == CompOp.NO or comp_op == CompOp.NE:
            current_page = index_handle.min_leaf()
            while True:
                current_node = IX_TreeNode.deserialize(field_type, field_size, node_capacity,
                    pf_manager.read_page(data_file_id, current_page))
                for entry in current_node.entries:
                    if comp_op == CompOp.NE and entry.field_value == field_value: continue
                    for rid in entry.get_all_rids(data_file_id): yield rid
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
        elif comp_op == CompOp.EQ:
            current_page = index_handle.search_leaf(field_value)
            current_node = IX_TreeNode.deserialize(field_type, field_size, node_capacity,
                    pf_manager.read_page(data_file_id, current_page))
            child_idx = current_node.search_child_idx(field_value)
            if child_idx == 0 or current_node.entries[child_idx-1].field_value != field_value:
                return None
            for rid in current_node.entries[child_idx-1].get_all_rids(data_file_id): yield rid
        elif comp_op == CompOp.LT or comp_op == CompOp.LE:
            current_page = index_handle.min_leaf()
            while True:
                current_node = IX_TreeNode.deserialize(field_type, field_size, node_capacity,
                    pf_manager.read_page(data_file_id, current_page))
                for entry in current_node.entries:
                    if entry.field_value > field_value: return None
                    if comp_op == CompOp.LT and entry.field_value == field_value: return None
                    for rid in entry.get_all_rids(data_file_id): yield rid
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
        elif comp_op == CompOp.GT or comp_op == CompOp.GE:
            current_page = index_handle.search_leaf(field_value)
            while True:
                current_node = IX_TreeNode.deserialize(field_type, field_size, node_capacity,
                    pf_manager.read_page(data_file_id, current_page))
                for entry in current_node.entries:
                    if entry.field_value < field_value: continue
                    if comp_op == CompOp.GT and entry.field_value == field_value: continue
                    for rid in entry.get_all_rids(data_file_id): yield rid
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
        else: raise IndexScanNextError(f'Encountered wrong comp_op.')

    
    def close_scan(self) -> None:
        ''' Close the index scan.
        '''
        self.is_opened = False


if __name__ == '__main__':
    pass
