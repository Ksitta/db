import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from utils.enums import CompOp
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_index_handle import IX_IndexHandle
from index_management.ix_tree_node import IX_TreeNode, IX_TreeNodeEntry, node_cache
from errors.err_index_management import *


class IX_IndexScan:
    
    
    def __init__(self):
        ''' Init the index scan.
        '''
        self.index_handle:IX_IndexHandle = None
        self.is_opened = False
    
    
    def open_scan(self, index_handle:IX_IndexHandle, comp_op:CompOp=CompOp.NO,
        field_values:List[Union[int,float,str]]=None) -> None:
        ''' Open the index scan.
        args:
            index_handle: IX_IndexHandle, an opened index handle instance.
            comp_op: CompOp, the filter condition. If comp_op == CompOp.NO,
                field_value will be ignored.
            field_values: List[Union[int,float,str]], the value to be compared with.
        '''
        self.is_opened = True
        self.index_handle = index_handle
        if not index_handle.is_opened:
            raise IndexOpenScanError(f'The index to be scanned is not opened.')
        self.comp_op, self.field_values = comp_op, field_values
    
    
    def next(self) -> Tuple[RM_Rid,int]:
        ''' Yield the next scanned rid that satisfies the filtering condition.
        return:
            Tuple[RM_Rid, int], the record rid and the verbose field.
        '''
        if not self.is_opened: return None
        index_handle = self.index_handle
        meta = index_handle.meta
        (field_number, fields, node_capacity) = \
            (meta['field_number'], meta['fields'], meta['node_capacity'])
        field_types = [field[0] for field in fields[:field_number]]
        field_sizes = [field[1] for field in fields[:field_number]]
        data_file_id = index_handle.data_file_id
        comp_op, field_values = self.comp_op, self.field_values
        if comp_op == CompOp.NO or comp_op == CompOp.NE:
            current_node = index_handle.min_leaf()
            while True:
                for entry in current_node.get_all_entries():
                    if comp_op == CompOp.NE and IX_TreeNodeEntry.eq(entry.field_values, field_values):
                        continue
                    for item in entry.get_all_rids(data_file_id): yield item
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
                current_node = node_cache[data_file_id].get(current_page)
                if current_node is None:
                    current_node = IX_TreeNode.deserialize(index_handle.data_file_id, field_types,
                        field_sizes, node_capacity, pf_manager.read_page(data_file_id, current_page))
                    node_cache[data_file_id][current_page] = current_node
        elif comp_op == CompOp.EQ:
            current_node, _ = index_handle.search_leaf(field_values)
            child_idx = current_node.search_child_idx(field_values)
            if child_idx == 0 or not IX_TreeNodeEntry.eq(
                current_node.get_entry(child_idx-1).field_values, field_values):
                return None
            for item in current_node.get_entry(child_idx-1).get_all_rids(data_file_id): yield item
        elif comp_op == CompOp.LT or comp_op == CompOp.LE:
            current_node = index_handle.min_leaf()
            while True:
                for entry in current_node.get_all_entries():
                    if not IX_TreeNodeEntry.le(entry.field_values, field_values): return None
                    if comp_op == CompOp.LT and IX_TreeNodeEntry.eq(
                        entry.field_values, field_values): return None
                    for item in entry.get_all_rids(data_file_id): yield item
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
                current_node = node_cache[data_file_id].get(current_page)
                if current_node is None:
                    current_node = IX_TreeNode.deserialize(index_handle.data_file_id, field_types,
                        field_sizes, node_capacity, pf_manager.read_page(data_file_id, current_page))
                    node_cache[data_file_id][current_page] = current_node
        elif comp_op == CompOp.GT or comp_op == CompOp.GE:
            current_node, _ = index_handle.search_leaf(field_values)
            while True:
                for entry in current_node.get_all_entries():
                    if IX_TreeNodeEntry.lt(entry.field_values, field_values): continue
                    if comp_op == CompOp.GT and IX_TreeNodeEntry.eq(
                        entry.field_values, field_values): continue
                    for item in entry.get_all_rids(data_file_id): yield item
                current_page = current_node.header.next_sib
                if current_page == cf.INVALID: break
                current_node = node_cache[data_file_id].get(current_page)
                if current_node is None:
                    current_node = IX_TreeNode.deserialize(index_handle.data_file_id, field_types,
                        field_sizes, node_capacity, pf_manager.read_page(data_file_id, current_page))
                    node_cache[data_file_id][current_page] = current_node
        else: raise IndexScanNextError(f'Encountered wrong comp_op.')

    
    def close_scan(self) -> None:
        ''' Close the index scan.
        '''
        self.is_opened = False


if __name__ == '__main__':
    pass
