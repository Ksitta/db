import struct
import numpy as np

import config as cf


class RM_PageHeader:
    ''' Stores the page header info.
    '''
    
    def __init__(self, record_cnt:int=0, next_free=cf.INVALID):
        self.record_cnt = record_cnt
        self.next_free = next_free
        
    
    def __str__(self):
        return f'{{record_cnt: {self.record_cnt}, next_free: {self.next_free}}}'
    

    @staticmethod
    def size() -> int:
        return 8
    
    
    @staticmethod
    def deserialize(data:np.ndarray):
        ''' Deserialize np.ndarray[(>=RM_PageHeader.size(),), uint8] to page header.
        '''
        buffer = data[:RM_PageHeader.size()].tobytes()
        return RM_PageHeader(*struct.unpack(f'{cf.BYTE_ORDER}ii', buffer))
    
    
    def serialize(self) -> np.ndarray:
        ''' Serialize page header to np.ndarray[(RM_PageHeader.size(),), uint8].
        '''
        buffer = struct.pack(f'{cf.BYTE_ORDER}ii', self.record_cnt, self.next_free)
        return np.frombuffer(buffer, dtype=np.uint8)
    

if __name__ == '__main__':
    header = RM_PageHeader(10, 20)
    print(header.size())
    data = header.serialize()
    print(data)
    data[0] = 30
    header = RM_PageHeader.deserialize(data)
    print(header.record_cnt, header.next_free)
    