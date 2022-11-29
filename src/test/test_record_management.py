import os
import struct
import numpy as np

import config as cf
from record_management.rm_rid import RM_Rid
from record_management.rm_record import RM_Record
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_file_scan import RM_FileScan
from record_management.rm_manager import rm_manager
from utils.enums import CompOp


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
    file_name = os.path.join(cf.TEST_ROOT, 'test_meta')
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
    

def test_pack_unpack_record():
    file_name = os.path.join(cf.TEST_ROOT, 'test_pack_unpack_record')
    meta = {
        'record_size': 22,
        'column_number': 3,
        'columns': [ {
                'column_type': cf.TYPE_INT,
                'column_size': cf.SIZE_INT,
                'column_name_length': 6,
                'column_name': 'pi_int',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_INT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_FLOAT,
                'column_size': cf.SIZE_FLOAT,
                'column_name_length': 8,
                'column_name': 'pi_float',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_FLOAT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_STR,
                'column_size': 10,
                'column_name_length': 6,
                'column_name': 'pi_str',
                'column_default_en': False,
                'column_default': np.zeros(10, dtype=np.uint8),
            },
        ],
        'primary_key_size': 0, 'primary_keys': [],
        'foreign_key_number': 0, 'foreign_keys': [],
    }
    rm_manager.create_file(file_name)
    handle:RM_FileHandle = rm_manager.open_file(file_name)
    handle.init_meta(meta)
    data = struct.pack(f'<id10s', 3, 3.14, b'3.14\0\0\0\0\0\0')
    data = np.frombuffer(data, dtype=np.uint8)
    assert len(data) == 22
    fields = handle.unpack_record(data)
    assert fields[0] == 3
    assert fields[1] == 3.14
    assert fields[2] == '3.14'
    packed_data = handle.pack_record(fields)
    assert np.min(data == packed_data) == True
    rm_manager.close_file(file_name)
    rm_manager.remove_file(file_name)
    print(f'test_pack_unpack_record passed!')
    

def test_record():
    data = np.frombuffer(b'1234567890\x00\x01\x02\x03', dtype=np.uint8)
    record1 = RM_Record(RM_Rid(10, 20), data)
    record2 = RM_Record(RM_Rid(10, 20), data)
    record3 = RM_Record(RM_Rid(10, 30), data)
    record_set = set()
    record_set.add(record1)
    record_set.add(record2)
    record_set.add(record3)
    assert len(record_set) == 2
    print(f'test_record passed!')
    

def test_file_scan():
    ''' Test insert, remove, update record interfaces and file scan.
    '''
    file_name = os.path.join(cf.TEST_ROOT, 'test_file_scan')
    str_size = 10
    meta = {
        'record_size': 12 + str_size,
        'column_number': 3,
        'columns': [ {
                'column_type': cf.TYPE_INT,
                'column_size': cf.SIZE_INT,
                'column_name_length': 7,
                'column_name': 'col_int',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_INT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_FLOAT,
                'column_size': cf.SIZE_FLOAT,
                'column_name_length': 9,
                'column_name': 'col_float',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_FLOAT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_STR,
                'column_size': str_size,
                'column_name_length': 7,
                'column_name': 'col_str',
                'column_default_en': False,
                'column_default': np.zeros(str_size, dtype=np.uint8),
            },
        ],
        'primary_key_size': 0, 'primary_keys': [],
        'foreign_key_number': 0, 'foreign_keys': [],
    }
    rm_manager.create_file(file_name)
    handle:RM_FileHandle = rm_manager.open_file(file_name)
    handle.init_meta(meta)
    N, rids = 100, []
    # insert records
    for i in range(N):
        data = struct.pack(f'<id{str_size}s', int(i*2), float(i*10),
            bytes(str(i*100).ljust(str_size, '\0'), encoding='utf-8'))
        rids.append(handle.insert_record(np.frombuffer(data, dtype=np.uint8)))
    # update records
    for i in range(N):
        data = struct.pack(f'<id{str_size}s', int(i), float(i*10),
            bytes(str(i*100).ljust(str_size, '\0'), encoding='utf-8'))
        handle.update_record(rids[i], np.frombuffer(data, dtype=np.uint8))
    # remove records
    for rid in rids:
        record = handle.get_record(rid)
        (i, d, s) = handle.unpack_record(record.data)
        if i % 2 != 0:
            handle.remove_record(rid)
    # file scan
    file_scan = RM_FileScan()
    file_scan.open_scan(handle)
    for idx, record in enumerate(file_scan.next()):
        (i, d, s) = handle.unpack_record(record.data)
        assert i==idx*2 and d==float(idx*20) and s==str(idx*200)
    # CompOp.EQ
    file_scan.open_scan(handle, comp_op=CompOp.EQ, field_value=str(N*50),
        field_type=cf.TYPE_STR, field_size=str_size, field_off=12)
    for record in file_scan.next():
        (i, d, s) = handle.unpack_record(record.data)
        assert i==N//2 and d==float(N*5) and s==str(N*50)
    # CompOp.LT
    file_scan.open_scan(handle, comp_op=CompOp.LT, field_value=float(N*5),
        field_type=cf.TYPE_FLOAT, field_size=8, field_off=4)
    records = list(file_scan.next())
    assert len(records) == N//4
    rm_manager.close_file(file_name)
    rm_manager.remove_file(file_name)
    print(f'test_file_scan passed!')
    
    
def test_file_scan_2():
    ''' Test dbtrain-lab-test lab1 00_setup.
    '''
    file_name = os.path.join(cf.TEST_ROOT, 'test_file_scan_2')
    str_size = 10
    meta = {
        'record_size': 16 + str_size,
        'column_number': 4,
        'columns': [ {
                'column_type': cf.TYPE_INT,
                'column_size': cf.SIZE_INT,
                'column_name_length': 2,
                'column_name': 'id',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_INT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_INT,
                'column_size': cf.SIZE_INT,
                'column_name_length': 2,
                'column_name': 'age',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_INT, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_STR,
                'column_size': str_size,
                'column_name_length': 4,
                'column_name': 'name',
                'column_default_en': False,
                'column_default': np.zeros(str_size, dtype=np.uint8),
            }, {
                'column_type': cf.TYPE_FLOAT,
                'column_size': cf.SIZE_FLOAT,
                'column_name_length': 5,
                'column_name': 'score',
                'column_default_en': False,
                'column_default': np.zeros(cf.SIZE_FLOAT, dtype=np.uint8),
            },
        ],
        'primary_key_size': 0, 'primary_keys': [],
        'foreign_key_number': 0, 'foreign_keys': [],
    }
    rm_manager.create_file(file_name)
    handle:RM_FileHandle = rm_manager.open_file(file_name)
    handle.init_meta(meta)
    N = 7
    # insert records
    for i in range(N):
        data = struct.pack(f'<ii{str_size}sd', i, i+20,
            bytes(chr(ord('a')+i).ljust(str_size, '\0'), encoding='utf-8'), i/10)
        handle.insert_record(np.frombuffer(data, dtype=np.uint8))
    # file scan: select *
    file_scan = RM_FileScan()
    file_scan.open_scan(handle)
    for idx, record in enumerate(file_scan.next()):
        (i, j, s, d) = handle.unpack_record(record.data)
        assert i==idx and j==idx+20 and s==chr(ord('a')+idx) and d==idx/10
    rm_manager.close_file(file_name)
    rm_manager.remove_file(file_name)
    print(f'test_file_scan_2 passed!')


def test():
    print(f'-------- Test record management --------')
    test_meta()
    test_pack_unpack_record()
    test_record()
    test_file_scan()
    test_file_scan_2()