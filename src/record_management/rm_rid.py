class RM_Rid:
    ''' The record identifier, including page_no and slot_no.
    '''
    
    def __init__(self, page_no:int, slot_no:int):
        self.page_no = page_no
        self.slot_no = slot_no
        
    
    def __str__(self):
        return f'{{page_no: {self.page_no}, slot_no: {self.slot_no}}}'
    
    
if __name__ == '__main__':
    rid = RM_Rid(10, 20)
    print(rid)
