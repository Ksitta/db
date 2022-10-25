import os
import sys
import struct
import numpy as np

from global_vars import FILE_OPEN_MODE

if __name__ == '__main__':
    data = struct.pack('<IIII', 1, 2, 3, 4)
    file_id = os.open('test.bin', os.O_RDWR | os.O_CREAT)
    os.write(file_id, data)    
    size = os.path.getsize('test.bin')
    print(size)
    pos = os.lseek(file_id, 10, os.SEEK_SET)
    print(pos)
    data = os.read(file_id, 16)
    print(data)
    os.close(file_id)
    
