import struct
import numpy as np

import config as cf


class RM_PageHeader:
    ''' Stores the page header info.
    '''
    
    def __init__(self, page_lsn:int=cf.INVALID, next_free=cf.INVALID):
        self.page_lsn = page_lsn
        self.next_free = next_free
        
    
    def __str__(self):
        return f'{{page_lsn: {self.page_lsn}, next_free: {self.next_free}}}'
    

    @staticmethod
    def size() -> int:
        return 8
    
    
    def serialize(self) -> np.ndarray:
        ''' Serialize page header to np.ndarray[(RM_PageHeader.size(),), uint8].
        '''
        bytes = struct.pack(f'{cf.BYTE_ORDER}ii', self.page_lsn, self.next_free)
        return np.frombuffer(bytes, dtype=np.uint8).copy()
    
    
    def deserialize(self, data:np.ndarray):
        ''' Deserialize np.ndarray[(>=RM_PageHeader.size(),), uint8] to page header.
        '''
        bytes = data[:RM_PageHeader.size()].tobytes()
        (self.page_lsn, self.next_free) = struct.unpack(f'{cf.BYTE_ORDER}ii', bytes)
    

if __name__ == '__main__':
    header = RM_PageHeader(10, 20)
    print(header.size())
    data = header.serialize()
    print(data)
    data[0] = 30
    header.deserialize(data)
    print(header.page_lsn, header.next_free)
    