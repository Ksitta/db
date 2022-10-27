import os


# paged file system
BUFFER_CAPACITY = 16384 # 2**14
PAGE_SIZE = 4096        # 2**12
INVALID = -1
try: FILE_OPEN_MODE = os.O_RDWR | os.O_BINARY
except AttributeError as exception:
    FILE_OPEN_MODE = os.O_RDWR
