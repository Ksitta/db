import os
import numpy as np

import config as cf
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_record_manager import rm_manager


def test_meta():
    ''' Test write and read table meta info.
    +----------------------------------------+
    |   id   |   age   |   temp   |   name   |
    +----------------------------------------+
    |   int  |   int   |  double  |    str   |
    +----------------------------------------+
    |    4   |    4    |     8    |    16    |
    +----------------------------------------+
    '''
    # PAGE_SIZE = 64
    file_name = f'test_meta'
    meta = {'record_size': 32, 'column_number': 4, 'columns': [
        {'column_type': 0, 'column_size': 4, 'column_name_length': 2, 'column_name': 'id'},
        {'column_type': 0, 'column_size': 4, 'column_name_length': 3, 'column_name': 'age'},
        {'column_type': 1, 'column_size': 8, 'column_name_length': 4, 'column_name': 'temp'},
        {'column_type': 2, 'column_size': 16,'column_name_length': 4, 'column_name': 'name'},
    ]}
    rm_manager.create_file(file_name)
    handle:RM_FileHandle = rm_manager.open_file(file_name)
    handle.init_meta(meta)
    read_meta = handle.read_meta()
    assert read_meta['record_per_page'] == 1
    assert read_meta['bitmap_size'] == 1
    assert read_meta['meta_page_number'] == 2
    assert meta['columns'] == read_meta['columns']
    rm_manager.close_file(file_name)
    rm_manager.remove_file(file_name)
    print(f'test_meta passed!')
    

def test():
    print(f'-------- Test record management --------')
    test_meta()