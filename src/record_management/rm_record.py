import numpy as np

from rm_rid import RM_Rid


class RM_Record:
    
    
    def __init__(self, rid:RM_Rid, data:np.ndarray):
        ''' Init the record identifier and data.
        args:
            rid: RM_Rid.
            data: np.ndarray[(N,), uint8].
        '''
        self.rid = rid
        self.data = data
        
    
    def __len__(self):
        return len(self.data)
    

    def __str__(self):
        return str(self.data.tobytes()) 
    

if __name__ == '__main__':
    rid = RM_Rid(10, 20)
    data = np.frombuffer(b'1234567890\x00\x01\x02\x03', dtype=np.uint8)
    record = RM_Record(rid, data)
    print(len(record))
    print(record)
    
    