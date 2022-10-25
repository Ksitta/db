import os


# paged file system
BUFFER_EN = False       # whether to enable page buffer
BUFFER_CAPACITY = 16384 # 2**14
PAGE_SIZE = 4096        # 2**12
try: FILE_OPEN_MODE = os.O_RDWR | os.O_BINARY
except AttributeError as exception:
    FILE_OPEN_MODE = os.O_RDWR
