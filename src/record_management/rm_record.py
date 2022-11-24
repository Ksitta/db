import numpy as np

from record_management.rm_rid import RM_Rid


class RM_Record:
    
    
    def __init__(self, rid:RM_Rid, data:np.ndarray):
        ''' Init the record identifier and data.
        args:
            rid: RM_Rid.
            data: np.ndarray[(N,), uint8].
        '''
        self.rid:RM_Rid = rid
        self.data:np.ndarray = data
        
    def __len__(self):
        return len(self.data)
    

    def __str__(self):
        return str(self.data.tobytes())
    

    def __hash__(self):
        return self.rid.__hash__()


    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.rid == other.rid
        return False
    

    def __ne__(self, other):
        return not self.__eq__(other)
    

if __name__ == '__main__':
    rid = RM_Rid(10, 20)
    rid2 = RM_Rid(10, 30)
    data = np.frombuffer(b'1234567890\x00\x01\x02\x03', dtype=np.uint8)
    record = RM_Record(rid, data)
    record2 = RM_Record(rid2, data)
    print(len(record))
    print(record)
    print(record == record2)
    
    