import os


# paged file system
# BUFFER_CAPACITY = 4     # for test only
# PAGE_SIZE = 64          # for test only
BUFFER_CAPACITY = 16384 # 2**14
PAGE_SIZE = 4096        # 2**12
INVALID = -1
try: FILE_OPEN_MODE = os.O_RDWR | os.O_BINARY
except AttributeError as exception:
    FILE_OPEN_MODE = os.O_RDWR
    
DATABASE_PATH = "./test_databases"
TABLE_META_SUFFIX = ".meta"
TABLE_DATA_SUFFIX = ".data"

# record management
BYTE_ORDER = '<'
TYPE_INT = 0        # field type int, size = 4
TYPE_FLOAT = 1      # field type float, size = 8
TYPE_STR = 2        # filed type str, size >= 0
SIZE_INT = 4        # size of int32, unchangable
SIZE_FLOAT = 8      # size of float64, unchangable

# index management
INDEX_META_SUFFIX = '.ixmeta'
INDEX_DATA_SUFFIX = '.ixdata'
NODE_TYPE_INTER = 0
NODE_TYPE_LEAF = 1
INDEX_ROOT_PAGE = 0

# unit test
TEST_ROOT = f'./test_root'
