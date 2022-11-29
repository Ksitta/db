from errors.err_sm_manager import *
import os
from config import *
from table.table import Table
from typing import Dict, Set, List
from record_management.rm_manager import rm_manager
from record_management.rm_rid import RM_Rid
from typing import List, Union, Tuple
from functools import wraps
import shutil
from common.common import *
import pandas as pd
from operators.operators import TableScanNode


def require_using_db(func):
    @wraps(func)
    def wrapfunc(*args, **kwargs):
        if (sm_manager._using_db == ""):
            raise NoUsingDatabaseError("No database is using")
        return func(*args, **kwargs)
    return wrapfunc


class SM_Manager():
    def __init__(self):
        self._using_db: str = ""
        self._db_names: Set[str] = set()
        self._tables: Dict[str, Table] = dict()

        if (not os.path.exists(DATABASE_PATH)):
            os.mkdir(DATABASE_PATH)
        os.chdir(DATABASE_PATH)
        self._base_dir: str = os.path.abspath(".")
        files = os.listdir(".")
        for file in files:
            self._db_names.add(file)

    def open_db(self, db_name: str):
        if (db_name != self._using_db):
            if (self._using_db != ""):
                self.close_db()
            if (db_name not in self._db_names):
                raise DataBaseNotExistError(
                    f'Database {db_name} is not existed')
            self._tables.clear()
            self._using_db = db_name
            os.chdir(os.path.join(self._base_dir, db_name))
            files = os.listdir(".")
            for file in files:
                if (file.endswith(TABLE_DATA_SUFFIX)):
                    self._tables[file[:-len(TABLE_DATA_SUFFIX)]
                                 ] = Table(file[:-len(TABLE_DATA_SUFFIX)])

    def create_db(self, db_name: str):
        if (db_name in self._db_names):
            raise DataBaseExistError(f'Database {db_name} existed')
        os.mkdir(os.path.join(self._base_dir, db_name))
        self._db_names.add(db_name)

    def drop_db(self, db_name: str):
        if (db_name not in self._db_names):
            raise DataBaseNotExistError(
                ((f'Database {db_name} is not existed')))
        if (self._using_db == db_name):
            self.close_db()
        os.chdir(self._base_dir)
        shutil.rmtree(db_name, ignore_errors=True)
        self._db_names.remove(db_name)

    def close_db(self):
        if (self._using_db == ""):
            return
        self._using_db = ""
        for each in self._tables.values():
            del each
        self._tables.clear()
        os.chdir(self._base_dir)

    def show_dbs(self):
        return Result(["Databases"], [[each] for each in list(self._db_names)])

    @require_using_db
    def show_tables(self):
        return Result(["Tables"], [[each] for each in list(self._tables.keys())])

    @require_using_db
    def create_table(self, rel_name: str, columns: List[Column], pk: List[str], fk: List[Dict]):
        if (rel_name in self._tables):
            raise TableExistsError(rel_name)
        names = [each.name for each in columns]
        if (len(set(names)) != len(columns)):
            raise DuplicateColumnError()
        pk_idx = [names.index(each) for each in pk]
        if(len(set(pk_idx)) != len(pk_idx)):
            raise DuplicatePrimaryKeyError()
        for fk_dict in fk:
            local_idx = [names.index(each) for each in fk_dict["local_idents"]]
            target_idx = [names.index(each)
                          for each in fk_dict["target_idents"]]
            if(len(set(local_idx)) != len(local_idx)):
                raise DuplicateForeignKeyError()
            if(len(set(target_idx)) != len(target_idx)):
                raise DuplicateForeignKeyError()
            target_columns = self._tables[fk_dict["target_table_name"]].get_columns()
            for i in range(len(local_idx)):
                if (columns[local_idx[i]].type != target_columns[target_idx[i]].type or columns[local_idx[i]].size != target_columns[target_idx[i]].size):
                    raise ForeignKeyTypeError()
            fk_dict["foreign_key_pairs"] = list(zip(local_idx, target_idx))
            fk_dict.pop("local_idents")
            fk_dict.pop("target_idents")
        self._tables[rel_name] = Table(rel_name, columns, pk_idx, pk)

    @require_using_db
    def describe_table(self, rel_name: str):
        if (self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        return self._tables[rel_name].describe()

    @require_using_db
    def drop_table(self, rel_name: str):
        if (self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        self._tables.remove(rel_name)
        rm_manager.remove_file(rel_name)

    @require_using_db
    def insert(self, rel_name: str, values: List[List[Union[int, float, str]]]):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        for each in values:
            table.insert_record(each)

    @require_using_db
    def delete(self, rel_name: str, records: RecordList):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        for each in records.records:
            self._tables[rel_name].delete_record(each.rid)

    @require_using_db
    def update(self, rel_name: str, records: RecordList, set_clause: List):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        up_list: List[Tuple(int, Union[int, float, str, None])] = []
        table = self._tables[rel_name]
        for each in set_clause:
            idx = table.get_column_idx(each[0])
            up_list.append((idx, each[1]))
        for each in records.records:
            for idx, val in up_list:
                each.data[idx] = val
            table.update_record(each.rid, each)

    @require_using_db
    def create_index(self, rel_name: str, idents: list):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        raise NotImplementedError()

    @require_using_db
    def drop_index(self, rel_name: str, idents: list):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        raise NotImplementedError()

    @require_using_db
    def load(self, rel_name: str, file_name: str):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        dtypes = {}
        i = 0
        table = self._tables[rel_name]
        for each in table.get_columns():
            if (each.type == TYPE_INT):
                dtypes[i] = int
            if (each.type == TYPE_FLOAT):
                dtypes[i] = float
            if (each.type == TYPE_STR):
                dtypes[i] = str
            i += 1
        raw_datas = pd.read_csv(file_name, header=None, dtype=dtypes)
        values = raw_datas.values.tolist()
        for each in values:
            table.insert_record(each)

    @require_using_db
    def dump(self, rel_name: str, file_name: str):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        node = TableScanNode(self._tables[rel_name])
        records = node.process()
        values = [each.data for each in records.records]
        pd.DataFrame(values).to_csv(file_name, header=False, index=False)

    @require_using_db
    def add_pk(self, rel_name: str, idents: list):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        raise NotImplementedError()

    def help(self):
        pass

    def set(self, param_name: str, value: str):
        pass

    @require_using_db
    def get_table(self, table_name: str) -> Table:
        if(table_name not in self._tables):
            raise TableNotExistsError(table_name)
        return self._tables[table_name]

    @require_using_db
    def get_table_name(self, col_name: str, tables: List[str]) -> str:
        found: bool = False
        for each in tables:
            table = self._tables[each]
            names = table.get_column_names()
            if(col_name in names):
                if(found):
                    raise AmbiguousColumnError(col_name)
                found = True
                table_name: str = table.get_name()

        if(not found):
            raise NoSuchColumnError(col_name)

        return table_name

    def get_using_db(self) -> str:
        return self._using_db


sm_manager = SM_Manager()
