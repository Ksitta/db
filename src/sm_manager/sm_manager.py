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
from utils.bitwise import list_int_to_int, int_to_list_int


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
        return Result(["Databases"], [[each] for each in list(self._db_names)], [])

    @require_using_db
    def show_tables(self):
        return Result(["Tables"], [[each] for each in list(self._tables.keys())], [])

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
            target_table = self.get_table(fk_dict["target_table_name"])
            tnames = target_table.get_column_names()
            target_idx = [tnames.index(each)
                          for each in fk_dict["target_idents"]]
            if(len(set(local_idx)) != len(local_idx)):
                raise DuplicateForeignKeyError()
            if(len(set(target_idx)) != len(target_idx)):
                raise DuplicateForeignKeyError()
            target_columns = self._tables[fk_dict["target_table_name"]].get_columns(
            )
            for i in range(len(local_idx)):
                if (columns[local_idx[i]].type != target_columns[target_idx[i]].type or columns[local_idx[i]].size != target_columns[target_idx[i]].size):
                    raise ForeignKeyTypeError()
            fk_dict["foreign_key_pairs"] = list(zip(local_idx, target_idx))
            fk_dict.pop("local_idents")
            fk_dict.pop("target_idents")
        Table.create_table(rel_name, columns, pk_idx, fk)
        self._tables[rel_name] = Table(rel_name)

    @require_using_db
    def describe_table(self, rel_name: str):
        if (self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        res = table.describe()
        addition = []
        pk = table.get_pk()
        fk = table.get_fk()
        columns = table.get_columns()
        if (len(pk) != 0):
            names = [columns[i].name for i in pk]
            item = "PRIMARY KEY ("
            i = 0
            for each in names:
                if (i != 0):
                    item += ", "
                item += each
                i += 1
            item += ")"
            addition.append(item)
        for each in fk:
            local_names = [columns[i[0]].name for i in each['foreign_key_pairs']]
            target_table: Table = self._tables[each['target_table_name']]
            target_col = target_table.get_columns()
            target_names = [target_col[i[1]].name for i in each['foreign_key_pairs']]
            item = "FOREIGN KEY " + each['foreign_key_name'] + " ("
            i = 0
            for names in local_names:
                if (i != 0):
                    item += ", "
                item += names
                i += 1
            item += ") REFERENCES " + each['target_table_name'] + " ("
            i = 0
            for names in target_names:
                if (i != 0):
                    item += ", "
                item += names
                i += 1
            item += ")"
            addition.append(item)
        indexes = table.get_index()
        for each in indexes:
            item = "INDEX ("
            col_idx = int_to_list_int(each)
            names = [columns[i].name for i in col_idx]
            i = 0
            for name in names:
                if (i != 0):
                    item += ", "
                item += name
                i += 1
            item += ")"
            addition.append(item)
        res.set_addition(addition)
        return res

    @require_using_db
    def drop_table(self, rel_name: str):
        if (self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        table.drop()
        self._tables.pop(rel_name)
        rm_manager.remove_file(rel_name)

    def _check_insert(self, table: Table, values: np.ndarray):
        table.check_fields(values)
        table.check_primary_key(values)

    def _check_fk(self, fk: List[Dict], values: np.ndarray):
        for each in fk:
            target_table = self.get_table(each["target_table_name"])
            fk_pairs: List[Tuple] = each["foreign_key_pairs"]
            local_idx = [each[0] for each in fk_pairs]
            target_idx = [each[1] for each in fk_pairs]
            res = target_table.find_exist(target_idx, local_idx, values)
            if (len(res) == 0):
                raise ForeignKeyNotExistsError()

    def _modify_ref_cnt(self, fk: Dict, values: np.ndarray, delta: int):
        for each in fk:
            target_table = self.get_table(each["target_table_name"])
            fk_pairs: List[Tuple] = each["foreign_key_pairs"]
            local_idx = [each[0] for each in fk_pairs]
            val_list = values[:, local_idx]
            target_table.modify_ref_cnt(val_list, delta)

    @require_using_db
    def insert(self, rel_name: str, values: np.ndarray):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        fk = table.get_fk()

        self._check_fk(fk, values)

        self._check_insert(table, values)
        table.insert_records(values)

        self._modify_ref_cnt(fk, values, 1)

    @require_using_db
    def delete(self, rel_name: str, records: RecordList):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table: Table = self._tables[rel_name]

        for each in records.records:
            ref_cnt = table.get_ref_cnt(each.data)
            if(ref_cnt != 0):
                raise ReferenceCountNotZeroError()
            table.delete_record(each)

        fk = table.get_fk()
        vals = np.array([each.data for each in records.records], dtype=object)
        self._modify_ref_cnt(fk, vals, -1)

    @require_using_db
    def update(self, rel_name: str, records: RecordList, set_clause: List):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        up_list: List[Tuple(int, Union[int, float, str, None])] = []
        table = self._tables[rel_name]
        fk = table.get_fk()
        for each in set_clause:
            idx = table.get_column_idx(each[0])
            up_list.append((idx, each[1]))

        for each in records.records:
            ref_cnt = table.get_ref_cnt(each.data)
            if(ref_cnt != 0):
                raise ReferenceCountNotZeroError()
            old_vals = each.data.copy()
            for idx, val in up_list:
                each.data[idx] = val
            np_array = np.array([each.data], dtype=object)
            self._check_fk(table.get_fk(), np_array)
            self._check_insert(table, np_array)
            self._modify_ref_cnt(fk, np_array, -1)
            table.delete_entry(old_vals, each.rid)
            table.update_record(each.rid, each)

    @require_using_db
    def create_index(self, rel_name: str, idents: List[str]):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        column_idx = [table.get_column_idx(each) for each in idents]
        index_no = list_int_to_int(column_idx)
        if (table.index_exist(index_no)):
            raise IndexAlreadyExistsError()
        records = table.load_all_records()
        table.create_index(column_idx, records)

    @require_using_db
    def drop_index(self, rel_name: str, idents: List[str]):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        column_idx = [table.get_column_idx(each) for each in idents]
        index_no = list_int_to_int(column_idx)
        table.drop_index(index_no)

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
        table.insert_records(raw_datas.values)
        self._modify_ref_cnt(table.get_fk(), raw_datas.values, 1)

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
        pk_idx = [table.get_column_idx(each) for each in idents]
        if(len(set(pk_idx)) != len(pk_idx)):
            raise DuplicatePrimaryKeyError()
        table.add_pk(pk_idx)

    @require_using_db
    def drop_pk(self, rel_name: str):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        table.drop_pk()

    @require_using_db
    def add_fk(self, rel_name: str, fk_name: str, target_table_name: str, local_idents: List[str], target_idents: List[str]):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        if (target_table_name not in self._tables):
            raise TableNotExistsError(target_table)
        table = self._tables[rel_name]
        target_table = self._tables[target_table]
        fk: Dict = {}
        local_idents_idx = [table.get_column_idx(each) for each in local_idents]
        target_idents_idx = [target_table.get_column_idx(each) for each in target_idents]
        fk_pairs = list(zip(local_idents_idx, target_idents_idx))
        fk["foreign_key_name"] = fk_name
        fk["foreign_key_name_length"] = len(fk_name)
        fk["target_table_name"] = target_table_name
        fk["target_table_name_length"] = len(target_table_name)
        fk["foreign_key_size"] = len(local_idents_idx)
        fk["foreign_key_pairs"] = fk_pairs
        target_pks = target_table.get_pk().copy()
        target_pks.sort()
        target_idents_idx.sort()
        if (target_pks != target_idents_idx):
            raise Exception("Foreign key can only reference primary key")
        table.add_fk(fk)

    @require_using_db
    def drop_fk(self, rel_name: str, fk_name: str):
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        table = self._tables[rel_name]
        table.drop_fk(fk_name)

    def help(self):
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

    def show_indexes(self):
        raise NotImplementedError()


sm_manager = SM_Manager()
