import os
from matplotlib.offsetbox import PaddedBox
import numpy as np

import config as cf
from paged_file.pf_file_manager import PF_FileManager


def test_alloc_buffer():
    # BUFFER_CAPACITY = 4, PAGE_SIZE = 16
    manager = PF_FileManager()
    name = os.path.join(cf.TEST_ROOT, 'test_alloc_buffer.data')
    manager.create_file(name)
    file_id = manager.open_file(name)
    data1 = []
    for i in range(2*cf.BUFFER_CAPACITY):
        data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8) + i
        data1.append(data)
        manager.append_page(file_id, data)
    manager.close_file(file_id)
    data1 = np.concatenate(data1).tobytes()
    file_id = os.open(name, os.O_RDWR)
    os.lseek(file_id, 0, os.SEEK_SET)
    data2 = os.read(file_id, 2 * cf.BUFFER_CAPACITY * cf.PAGE_SIZE)
    os.close(file_id)
    manager.remove_file(name)
    if data1 == data2:
        print(f'test_alloc_buffer passed!')
    else: print(f'test_alloc_buffer failed!')
    
    
def test_lru():
    # BUFFER_CAPACITY = 4, PAGE_SIZE = 16
    manager = PF_FileManager()
    name = os.path.join(cf.TEST_ROOT, 'test_lru.data')
    manager.create_file(name)
    file_id = manager.open_file(name)
    for i in range(cf.BUFFER_CAPACITY):
        data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8) + i
        manager.append_page(file_id, data)
    page_ids = [0, 2, 1, 0]
    for page_id in page_ids:
        _ = manager.read_page(file_id, page_id)
    buffer_ids = []
    for i in range(cf.BUFFER_CAPACITY):
        buffer_ids.append(manager._alloc_buffer())
    manager.close_file(file_id)
    manager.remove_file(name)
    assert buffer_ids == [3, 2, 1, 0], 'test_lru failed!'
    print(f'test_lru passed!')
    

def test_write_page():
    # BUFFER_CAPACITY = 4, PAGE_SIZE = 16
    manager = PF_FileManager()
    name = os.path.join(cf.TEST_ROOT, 'test_write_page.data')
    manager.create_file(name)
    file_id = manager.open_file(name)
    manager.allocate_pages(file_id, cf.BUFFER_CAPACITY)
    data1 = []
    for page_id in range(cf.BUFFER_CAPACITY):
        data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8) + page_id
        data1.append(data)
        manager.write_page(file_id, page_id, data)
    manager.close_file(file_id)
    data1 = np.concatenate(data1).tobytes()
    file_id = os.open(name, os.O_RDWR)
    os.lseek(file_id, 0, os.SEEK_SET)
    data2 = os.read(file_id, cf.BUFFER_CAPACITY * cf.PAGE_SIZE)
    os.close(file_id)
    manager.remove_file(name)
    assert data1 == data2, 'test_write_page failed!'
    print(f'test_write_page passed!')
    
    
def test_data_structures():
    # BUFFER_CAPACITY = 4, PAGE_SIZE = 16
    manager = PF_FileManager()
    name = os.path.join(cf.TEST_ROOT, 'test_data_structures.data')
    manager.create_file(name)
    file_id = manager.open_file(name)
    for page_id in range(cf.BUFFER_CAPACITY):
        data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8) + page_id
        manager.append_page(file_id, data)
    assert list(manager.buffered_pages[file_id]) == list(range(cf.BUFFER_CAPACITY))
    manager.close_file(file_id)
    assert len(manager.page_cnt) == 0, 'test_data_structures failed!'
    assert file_id not in manager.buffered_pages
    file_id = manager.open_file(name)
    assert file_id in manager.page_cnt and manager.page_cnt[file_id] == cf.BUFFER_CAPACITY
    manager.close_file(file_id)
    manager.remove_file(name)
    print(f'test_data_structures passed!')


def test():
    print(f'-------- Test paged file --------')
    test_alloc_buffer()
    test_lru()
    test_write_page()
    test_data_structures()
