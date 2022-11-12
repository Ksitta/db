from paged_file.pf_file_manager import pf_manager
from type.type import BaseType, FloatType, IntType, VarcharType


class Column():
    _name: str
    _type: BaseType
    _length: int

    def __init__(self, name: str, type: BaseType):
        self._name = name
        self._type = type
        self._length = type.get_length()


class Table():
    _name: str
    _meta_fd: int
    _data_fd: int
    _columns: list
    _record_len: int
    _record_cnt: int

    def __init__(self, name: str, meta_fd: int, data_fd: int) -> None:
        self._name = name
        self._meta_fd = meta_fd
        self._data_fd = data_fd
        self._load()
        
    def __init__(self, name : str, attrs : list, meta_fd : int, data_fd : int) -> None:
        self._name = name
        self._meta_fd = meta_fd
        self._data_fd = data_fd
        
    def __del__(self):
        self._store()
        

    def _load(self):
        metas: bytes = pf_manager.read_page(self._meta_fd, 0)
        self._record_len = int.from_bytes(
            metas[0:4], byteorder='little')
        self._record_cnt = int.from_bytes(
            metas[4:8], byteorder='little')
        col_cnt = int.from_bytes(metas[8:12], byteorder='little')
        page_num = int.from_bytes(metas[12:16], byteorder='little')

        metas = bytes()
        for i in range(page_num):
            metas += pf_manager.read_page(self._meta_fd, i + 1)

        pos : int = 0
        for i in range(col_cnt):
            type_num = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            type_len = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            name_len = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            name = metas[pos:pos + name_len].decode('utf-8')
            pos += name_len
            if (type_num == 0):
                type = IntType()
            if (type_num == 1):
                type = VarcharType(type_len)
            if (type_num == 2):
                type = FloatType()
            self._columns.append(Column(name, type))

    def _store(self):
        metas = bytes()

        for col in self._columns:
            metas += col._type.get_type().value.to_bytes(4, byteorder='little')
            metas += col._type.get_length().to_bytes(4, byteorder='little')
            metas += len(col._name).to_bytes(4, byteorder='little')
            metas += col._name.encode('utf-8')

        page_num: int = (len(metas) + 4095) // 4096
        for i in range(page_num):
            pf_manager.write_page(self._meta_fd, i + 1,
                                  metas[i * 4096:(i + 1) * 4096])

        metas = bytes()
        metas += self._record_len.to_bytes(4, byteorder='little')
        metas += self._record_cnt.to_bytes(4, byteorder='little')
        metas += len(self._columns).to_bytes(4, byteorder='little')
        metas += len(metas).to_bytes(4, byteorder='little')
