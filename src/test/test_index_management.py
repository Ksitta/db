import os
import struct
import numpy as np

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from index_management.ix_tree_node import IX_TreeNodeHeader, IX_TreeNode
from index_management.ix_index_handle import IX_IndexHandle
from index_management.ix_index_scan import IX_IndexScan
from index_management.ix_manager import ix_manager
from utils.enums import CompOp


def test_index_init():
    file_name = os.path.join(cf.TEST_ROOT, 'test_index_init')
    index_no = 0
    ix_manager.create_index(file_name, index_no)
    index_handle: IX_IndexHandle = ix_manager.open_index(file_name, index_no)
    meta = {'field_type': cf.TYPE_INT, 'field_size': cf.SIZE_INT}
    node_capacity = (cf.PAGE_SIZE-IX_TreeNodeHeader.size()) // (cf.SIZE_INT+8)
    index_handle.init_meta(meta)
    read_meta = IX_IndexHandle._desetialize_meta(
        pf_manager.read_page(index_handle.meta_file_id, 0))
    assert read_meta['field_type'] == cf.TYPE_INT
    assert read_meta['field_size'] == cf.SIZE_INT
    assert read_meta['node_capacity'] == node_capacity
    root_node = IX_TreeNode.deserialize(index_handle.data_file_id, cf.TYPE_INT, cf.SIZE_INT,
        node_capacity, pf_manager.read_page(index_handle.data_file_id, cf.INDEX_ROOT_PAGE))
    header = root_node.header
    assert header.node_type == cf.NODE_TYPE_LEAF
    assert header.parent == cf.INVALID
    assert header.page_no == cf.INDEX_ROOT_PAGE
    assert header.entry_number == 0
    assert header.prev_sib == cf.INVALID
    assert header.next_sib == cf.INVALID
    assert header.first_child == cf.INVALID
    ix_manager.close_index(file_name, index_no)
    ix_manager.remove_index(file_name, index_no)
    print(f'Index init passed!')
    

def test_index_insert():
    file_name = os.path.join(cf.TEST_ROOT, 'test_index_insert')
    index_no = 0
    ix_manager.create_index(file_name, index_no)
    index_handle: IX_IndexHandle = ix_manager.open_index(file_name, index_no)
    meta = {'field_type': cf.TYPE_INT, 'field_size': cf.SIZE_INT}
    node_capacity = (cf.PAGE_SIZE-IX_TreeNodeHeader.size()) // (cf.SIZE_INT+8)
    index_handle.init_meta(meta)
    
    N, M = 20, 10 
    values = np.repeat(np.arange(N), M)
    np.random.shuffle(values)
    print(f'### values[:41]: {values[:41]}')
    values = [1, 17, 10, 9, 18, 0, 14, 15, 6, 12, 8, 3, 13, 7, 4, 5, 11, 2, 13]
    # for i in range(len(values)):
    for i in range(9):
        index_handle.insert_entry(values[i], RM_Rid(0, i))
    scanned = np.zeros_like(values) - 1
    index_scan = IX_IndexScan()
    index_scan.open_scan(index_handle, CompOp.NO)
    for i, rid in enumerate(index_scan.next()):
        scanned[i] = rid.slot_no
    scanned = sorted(scanned)
    root_node = IX_TreeNode.deserialize(index_handle.data_file_id, cf.TYPE_INT, cf.SIZE_INT,
        node_capacity, pf_manager.read_page(index_handle.data_file_id, cf.INDEX_ROOT_PAGE))
    root_node.print_subtree(0)
    assert np.min(np.arange(N*M) == scanned) == True
    
    ix_manager.close_index(file_name, index_no)
    ix_manager.remove_index(file_name, index_no)
    print(f'Index insert passed!')


def test():
    print(f'-------- Test index management --------')
    test_index_init()
    test_index_insert()
