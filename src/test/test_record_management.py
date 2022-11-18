import os
import struct
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
    meta = {
        'record_size': 32,
        'column_number': 4,
        'columns': [ {
                'column_type': 0,
                'column_size': 4,
                'column_name_length': 2,
                'column_name': 'id',
                'column_default_en': False,
                'column_default': np.zeros(4, dtype=np.uint8),
            }, {
                'column_type': 0,
                'column_size': 4,
                'column_name_length': 3,
                'column_name': 'age',
                'column_default_en': False,
                'column_default': np.zeros(4, dtype=np.uint8),
            }, {
                'column_type': 1,
                'column_size': 8,
                'column_name_length': 4,
                'column_name': 'temp',
                'column_default_en': True,
                'column_default': np.frombuffer(struct.pack(f'<d', 36.0), dtype=np.uint8)
            }, {
                'column_type': 2,
                'column_size': 16,
                'column_name_length': 4,
                'column_name': 'name',
                'column_default_en': True,
                'column_default': np.frombuffer(b'default_name0000', dtype=np.uint8)
            },
        ],
        'primary_key_size': 2,
        'primary_keys': [0, 3],
        'foreign_key_number': 2,
        'foreign_keys': [ {
                'foreign_key_name_length': 3,
                'foreign_key_name': 'fk1',
                'target_table_name_length': 7,
                'target_table_name': 'student',
                'foreign_key_size': 2,
                'foreign_key_pairs': [(0, 1), (3, 2)],
            }, {
                'foreign_key_name_length': 3,
                'foreign_key_name': 'fk2',
                'target_table_name_length': 12,
                'target_table_name': 'target_table',
                'foreign_key_size': 3,
                'foreign_key_pairs': [(0, 1), (1, 2), (2, 3)],
            },
        ],
    }
    rm_manager.create_file(file_name)
    handle:RM_FileHandle = rm_manager.open_file(file_name)
    handle.init_meta(meta)
    handle.meta['next_free_page'] = 1
    handle.meta_modified = True
    handle.sync_meta()
    read_meta = handle.read_meta()
    assert read_meta['next_free_page'] == 1
    rm_manager.close_file(file_name)
    rm_manager.remove_file(file_name)
    print(f'test_meta passed!')
    

def test():
    print(f'-------- Test record management --------')
    test_meta()