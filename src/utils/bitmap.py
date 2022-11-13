import numpy as np


class Bitmap:
    ''' The general bitmap data structure.
    '''
    
    def __init__(self, capacity:int=8):
        ''' Init the data structure.
        args:
            capacity: int, indicates how many bits the bitmap can represent.
        '''
        self.capacity = capacity            # capacity in bits
        self.size = (capacity + 7) // 8     # size in bytes
        self.data = np.zeros((self.size,), dtype=np.uint8)
        
    
    def __str__(self):
        ''' Print the bitmap for debug porposes.
        '''
        res = []
        data = self.data
        for byte_no in range(self.size - 1):
            byte = data[byte_no]
            for bit_no in range(8):
                res.append('1' if (byte & np.uint8(1<<bit_no)) > 0 else '0')
        byte = data[self.size - 1]
        length = self.capacity % 8
        for bit_no in range(length if length > 0 else 8):
            res.append('1' if (byte & np.uint8(1<<bit_no)) > 0 else '0')
        return f'{{capacity: {self.capacity}, data: [{" ".join(res)}]}}'
    
    
    def set_bit(self, idx:int, occupied:bool):
        ''' Set the occupation status of a bit.
        args:
            idx, int, the index in bit, in [0, capacity).
            occupied: bool, True for 1, False for 0.
        '''
        byte_no = idx // 8
        bit_no = idx % 8
        if occupied: self.data[byte_no] |= np.uint8(1 << bit_no)
        else: self.data[byte_no] &= ~np.uint8(1 << bit_no)
        
        
    def get_bit(self, idx:int) -> bool:
        ''' Get the occupation status of a bit.
        return: bool, True for 1, False for 0.
        '''
        return (self.data[idx//8] & np.uint8(1 << (idx%8))) > 0
    

    def clear(self):
        ''' Unset all bits.
        '''
        self.data[:] = 0
    
    
    def first_free(self) -> int:
        ''' Search the not unoccupied bit, return its index.
        return:
            int, the first free index, if not found, return -1.
        '''
        res = -1
        for byte_no, byte in enumerate(self.data):
            if res >= 0 or byte == 255: continue
            for bit_no in range(8):
                if (byte & np.uint8(1<<bit_no)) == 0:
                    res = byte_no * 8 + bit_no
                    break
        if res >= self.capacity: return -1
        return res
        
    
    def serialize(self) -> np.ndarray:
        ''' Serialize the bitmap to np.ndarray[(self.size,), uint8].
        '''
        return self.data.copy()
        
    
    def deserialize(self, capacity:int, data:np.ndarray) -> None:
        ''' Deserialize np.ndarray to the bitmap.
            This will reinit the whole bitmap.
        args:
            capacity: int, the capacity of the new bitmap.
            data: np.ndarray[(size,), uint8], size * 8 must >= capacity.
        '''
        self.capacity = capacity
        self.size = (capacity + 7) // 8
        self.data = data[:self.size].copy()


if __name__ == '__main__':
    bitmap = Bitmap(20)
    for idx in (2, 3, 5, 7, 9, 11, 13, 15, 17, 19):
        bitmap.set_bit(idx, True)
    for idx in (9, 15):
        bitmap.set_bit(idx, False)
    print(bitmap)
    data = bitmap.serialize()
    print(data)
    bitmap.deserialize(16, data)
    print(bitmap)
        