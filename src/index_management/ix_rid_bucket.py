import os
import struct
import numpy as np
from typing import Tuple, List

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from utils.bitmap import Bitmap
from errors.err_index_management import *


# file_id -> Dict[Tuple[int,int], IX_RidBucket]
bucket_cache = {}


class IX_RidBucketHeader:
    
    
    def __init__(self, rid_cnt, next_page):
        ''' Init the rid bucket header.
        '''
        self.rid_cnt, self.next_page = rid_cnt, next_page
        
    
    @staticmethod
    def size() -> int:
        return 8
    
    
    @staticmethod
    def deserialize(data:np.ndarray):
        ''' Deserialize np.ndarray[>=IX_RidBucketHeader.size(), uint8] to rid bucket header.
        '''
        buffer = data[:IX_RidBucketHeader.size()].tobytes()
        return IX_RidBucketHeader(*struct.unpack(f'{cf.BYTE_ORDER}ii', buffer))
    

    def serialize(self) -> np.ndarray:
        ''' Serialize rid bucket header to np.ndarray[IX_RidBucketHeader.size(), uint8].
        '''
        buffer = struct.pack(f'{cf.BYTE_ORDER}ii', self.rid_cnt, self.next_page)
        return np.frombuffer(buffer, dtype=np.uint8)


class IX_RidBucket:
    
    
    def __init__(self) -> None:
        ''' Init an empty rid bucket
        '''
        header_size = IX_RidBucketHeader.size()
        capacity = (8*(cf.PAGE_SIZE-header_size)-7) // (8*12+1)
        self.header = IX_RidBucketHeader(rid_cnt=0, next_page=cf.INVALID)
        self.bitmap = Bitmap(capacity=capacity)
        bitmap_size = self.bitmap.size
        self.data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
        self.data[:header_size] = self.header.serialize()
        self.data[header_size:header_size+bitmap_size] = self.bitmap.serialize()
        self.data_modified = True
        
    
    def free_space(self):
        ''' Ruturn the free slot count in the page.
        '''
        return self.bitmap.capacity - self.header.rid_cnt
    
    
    def set_next_page(self, page_no:int) -> None:
        ''' Set next_page in header.
        '''
        self.header.next_page = page_no
        header_size = IX_RidBucketHeader.size()
        self.data[:header_size] = self.header.serialize()
        self.data_modified = True
        
        
    def insert_rid(self, rid:RM_Rid, verbose:int) -> None:
        ''' Insert a rid into the bucket.
        '''
        slot_no = self.bitmap.first_free()
        if slot_no == cf.INVALID:
            raise BucketInsertRidError(f'No free slot in the bucket.')
        if self.header.rid_cnt >= self.bitmap.capacity:
            raise BucketInsertRidError(f'Bucket overflow.')
        header_size = IX_RidBucketHeader.size()
        bitmap_size = self.bitmap.size
        entry_size = 12
        offset = header_size + bitmap_size + slot_no * entry_size
        self.header.rid_cnt += 1
        self.bitmap.set_bit(slot_no, True)
        self.data[:header_size] = self.header.serialize()
        self.data[header_size:header_size+bitmap_size] = self.bitmap.serialize()
        self.data[offset:offset+entry_size] = np.frombuffer(struct.pack(
            f'{cf.BYTE_ORDER}iii', rid.page_no, rid.slot_no, verbose), dtype=np.uint8)
        self.data_modified = True
        
    
    def remove_rid(self, slot_no:int) -> None:
        ''' Remove the rid at <slot_no>.
        '''
        if not self.bitmap.get_bit(slot_no):
            raise BucketRemoveRidError(f'Slot {slot_no} is not occupied.')
        if self.header.rid_cnt <= 0:
            raise BucketRemoveRidError(f'Bucket underflow')
        self.header.rid_cnt -= 1
        self.bitmap.set_bit(slot_no, False)
        header_size = IX_RidBucketHeader.size()
        bitmap_size = self.bitmap.size
        self.data[:header_size] = self.header.serialize()
        self.data[header_size:header_size+bitmap_size] = self.bitmap.serialize()
        self.data_modified = True
        
    
    def search_rid(self, page_no:int, slot_no:int) -> int:
        ''' Search the first slot position that contains the rid.
        return: int, the rid index, if not found, return INVALID.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        idx = cf.INVALID
        base_off = IX_RidBucketHeader.size() + self.bitmap.size
        data = self.data
        for i in self.bitmap.occupied_slots():
            off = base_off + (i * 12)
            page, slot = struct.unpack(f'{BYTE_ORDER}ii', data[off:off+8].tobytes())
            if page == page_no and slot == slot_no:
                idx = i
                break
        return idx
        
    
    def get_all_rids(self) -> List[Tuple[RM_Rid,int]]:
        ''' Return a list of all rids and verbose fields.
        '''
        BYTE_ORDER = cf.BYTE_ORDER
        bitmap = self.bitmap
        data = self.data
        base_off = IX_RidBucketHeader.size() + bitmap.size
        res = []
        for i in bitmap.occupied_slots():
            off = base_off + (i * 12)
            page_no, slot_no, verbose = struct.unpack(
                f'{BYTE_ORDER}iii', data[off:off+12].tobytes())
            res.append((RM_Rid(page_no, slot_no), verbose))
        return res
        
    
    @staticmethod
    def deserialize(data:np.ndarray):
        bucket = IX_RidBucket()
        header_size = IX_RidBucketHeader.size()
        capacity = (8*(cf.PAGE_SIZE-header_size)-7) // (8*12+1)
        bitmap_size = bucket.bitmap.size
        bucket.header = IX_RidBucketHeader.deserialize(data[:header_size])
        bucket.bitmap = Bitmap.deserialize(capacity, data[header_size:header_size+bitmap_size])
        bucket.data[:] = data[:cf.PAGE_SIZE]
        return bucket
        
    
    def serialize(self) -> np.ndarray:
        ''' Serialize rid bucket into np.ndarray[PAGE_SIZE, uint8].
        '''
        return self.data.copy()
    

    def sync(self, file_id:int, page_id:int) -> None:
        ''' Sync data to disk.
        '''
        if not self.data_modified: return
        self.data_modified = False
        bucket_cache[file_id][page_id] = self
        
    
def flush_bucket_cache(file_id:int):
    ''' Flush bucket pages to pf_manager.
    '''
    for page_id, bucket in bucket_cache[file_id].items():
        pf_manager.write_page(file_id, page_id, bucket.serialize())
    bucket_cache.pop(file_id, None)
    
if __name__ == '__main__':
    pass
