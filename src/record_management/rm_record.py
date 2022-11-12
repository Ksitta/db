from rm_rid import RM_Rid


class RM_Record:
    
    
    def __init__(self, rid:RM_Rid, data:bytes):
        self.rid = rid
        self.data = data
        
    
    def __len__(self):
        return len(self.data)
    

    def __str__(self):
        return str(self.data)    
    

if __name__ == '__main__':
    rid = RM_Rid(10, 20)
    record = RM_Record(rid, b'1234567890\x00\x01\x02\x03')
    print(len(record))
    print(record)
    
    