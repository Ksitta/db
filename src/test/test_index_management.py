import os
import time
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
    meta = {'field_number': 2, 'fields': [(cf.TYPE_INT, 4), (cf.TYPE_INT, 4)]}
    node_capacity = (cf.PAGE_SIZE-IX_TreeNodeHeader.size()) // 20
    index_handle.init_meta(meta)
    read_meta = IX_IndexHandle._desetialize_meta(
        pf_manager.read_page(index_handle.meta_file_id, 0))
    assert read_meta['field_number'] == 2
    fields = read_meta['fields']
    assert fields[0] == (cf.TYPE_INT, 4) and fields[1] == (cf.TYPE_INT, 4)
    assert read_meta['node_capacity'] == node_capacity
    root_node = IX_TreeNode.deserialize(index_handle.data_file_id, [cf.TYPE_INT, cf.TYPE_INT],
        [4, 4], node_capacity, pf_manager.read_page(index_handle.data_file_id, cf.INDEX_ROOT_PAGE))
    header = root_node.header
    assert header.node_type == cf.NODE_TYPE_LEAF
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
    meta = {'field_number': 2, 'fields': [(cf.TYPE_INT, 4), (cf.TYPE_INT, 4)]}
    node_capacity = (cf.PAGE_SIZE-IX_TreeNodeHeader.size()) // 20
    index_handle.init_meta(meta)
    N, M = 100, 10
    values = np.repeat(np.arange(N), M)
    np.random.shuffle(values)
    for i in range(N * M):
        index_handle.insert_entry([values[i], values[i]*2], RM_Rid(0, i), i)
    scanned1 = np.zeros_like(values) - 1
    scanned2 = scanned1.copy()
    index_scan = IX_IndexScan()
    index_scan.open_scan(index_handle, CompOp.NO)
    for i, (rid, verbose) in enumerate(index_scan.next()):
        scanned1[i] = rid.slot_no
        scanned2[i] = verbose
    scanned1 = sorted(scanned1)
    scanned2 = sorted(scanned2)
    assert np.min(np.arange(N*M) == scanned1) == True
    assert np.min(np.arange(N*M) == scanned2) == True
    ix_manager.close_index(file_name, index_no)
    ix_manager.remove_index(file_name, index_no)
    print(f'Index insert passed!')
    

def test_index_remove():
    file_name = os.path.join(cf.TEST_ROOT, 'test_index_remove')
    index_no = 0
    ix_manager.create_index(file_name, index_no)
    index_handle: IX_IndexHandle = ix_manager.open_index(file_name, index_no)
    meta = {'field_number': 2, 'fields': [(cf.TYPE_INT, 4), (cf.TYPE_INT, 4)]}
    index_handle.init_meta(meta)
    N, M = 100, 100
    values = np.repeat(np.arange(N), M)
    np.random.shuffle(values)
    tic = time.perf_counter()
    for i in range(N * M):
        index_handle.insert_entry([values[i], values[i]*2], RM_Rid(0, i), i)
    toc = time.perf_counter()
    print(f'### insert time cost: {(toc-tic)*1000:.3f} ms')
    print(f'NODE_SERIALIZE_CNT: {cf.NODE_SERIALIZE_CNT}')
    print(f'NODE_DESERIALIZE_CNT: {cf.NODE_DESERIALIZE_CNT}')
    tic = time.perf_counter()
    for i in range((N*M)//2, N*M):
        index_handle.remove_entry([values[i], values[i]*2], RM_Rid(0, i))
    toc = time.perf_counter()
    print(f'### remove time cost: {(toc-tic)*1000:.3f} ms')
    print(f'NODE_SERIALIZE_CNT: {cf.NODE_SERIALIZE_CNT}')
    print(f'NODE_DESERIALIZE_CNT: {cf.NODE_DESERIALIZE_CNT}')
    scanned1 = np.zeros((N*M)//2, dtype=values.dtype) - 1
    scanned2 = scanned1.copy()
    index_scan = IX_IndexScan()
    index_scan.open_scan(index_handle, CompOp.NO)
    for i, (rid, verbose) in enumerate(index_scan.next()):
        scanned1[i] = rid.slot_no
        scanned2[i] = verbose
    scanned1 = sorted(scanned1)
    scanned2 = sorted(scanned2)
    assert np.min(np.arange((N*M) // 2) == scanned1) == True
    assert np.min(np.arange((N*M) // 2) == scanned2) == True
    ix_manager.close_index(file_name, index_no)
    ix_manager.remove_index(file_name, index_no)
    print(f'Index remove passed!')
    

def test_modify_verbose():
    file_name = os.path.join(cf.TEST_ROOT, 'test_modify_verbose')
    index_no = 0
    ix_manager.create_index(file_name, index_no)
    index_handle: IX_IndexHandle = ix_manager.open_index(file_name, index_no)
    meta = {'field_number': 2, 'fields': [(cf.TYPE_INT, 4), (cf.TYPE_INT, 4)]}
    node_capacity = (cf.PAGE_SIZE-IX_TreeNodeHeader.size()) // 20
    index_handle.init_meta(meta)
    N, M = 10, 10 
    values = np.repeat(np.arange(N), M)
    np.random.shuffle(values)
    for i in range(N * M):
        index_handle.insert_entry([values[i], values[i]*2], RM_Rid(0, i), i)
    for i in range(N * M):
        index_handle.modify_verbose([values[i], values[i]*2], 1)
    index_scan = IX_IndexScan()
    index_scan.open_scan(index_handle, CompOp.NO)
    for i, (rid, verbose) in enumerate(index_scan.next()):
        assert verbose == rid.slot_no + M
    ix_manager.close_index(file_name, index_no)
    ix_manager.remove_index(file_name, index_no)
    print(f'Modify verbose passed!')


def test():
    print(f'-------- Test index management --------')
    # test_index_init()
    # test_index_insert()
    test_index_remove()
    # test_modify_verbose()
