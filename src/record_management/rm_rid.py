class RM_Rid:
    ''' The record identifier, including page_no and slot_no.
    '''
    
    def __init__(self, page_no:int, slot_no:int):
        self.page_no = page_no
        self.slot_no = slot_no
        
    
    def __str__(self):
        return f'{{page_no: {self.page_no}, slot_no: {self.slot_no}}}'
    
    
    def __hash__(self) -> int:
        return hash((self.page_no, self.slot_no))
    

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return self.page_no == other.page_no and self.slot_no == other.slot_no
        return False
    

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)
    
    
if __name__ == '__main__':
    rid = RM_Rid(10, 20)
    print(rid)
