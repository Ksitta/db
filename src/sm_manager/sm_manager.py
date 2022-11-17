from errors.err_sm_manager import *
import os
from config import *
from table.table import Table


class SM_Manager():
    def __init__(self):
        self._using_db: str = ""
        self._db_names: set = set()
        self._name2table: dict = dict()
        self._tables: dict = dict()

        if(not os.path.exists(DATABASE_PATH)):
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
                if(file.endswith(TABLE_DATA_SUFFIX)):
                    self._tables[file[:-len(TABLE_DATA_SUFFIX)]
                                 ] = Table(file[:-len(TABLE_DATA_SUFFIX)])

    def create_db(self, db_name: str):
        if (db_name in self._db_names):
            raise DataBaseExistError(f'Database {db_name} existed')
        os.mkdir(os.path.join(self._base_dir, db_name))
        self._db_names.add(db_name)

    def drop_db(self, db_name: str):
        if(db_name not in self._db_names):
            raise DataBaseNotExistError(
                ((f'Database {db_name} is not existed')))
        if(self._using_db == db_name):
            self.close_db()
        os.chdir(self._base_dir)
        os.rmdir(db_name)
        self._db_names.remove(db_name)

    def close_db(self):
        if (self._using_db == ""):
            return
        self._using_db = ""
        self._tables.clear()
        os.chdir(self._base_dir)

    def show_dbs(self):
        return self._db_names

    def show_tables(self):
        return self._tables.keys()

    def create_table(self, rel_name: str, columns: list, pk: list, fk: dict):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name in self._tables):
            raise TableExistsError(rel_name)

        self._tables[rel_name] = Table(rel_name, columns, pk, fk)

    def describe_table(self, rel_name: str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        self._tables[rel_name].describe()

    def drop_table(self, rel_name: str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        self._name2table[rel_name].drop()
        self._tables.remove(rel_name)

    def create_index(self, rel_name: str, attr_name: str):
        pass

    def drop_index(self, rel_name: str, attr_name: str):
        pass

    def load(self, rel_name: str, file_name: str):
        pass

    def help(self):
        pass

    def help(self, rel_name: str):
        pass

    def set(self, param_name: str, value: str):
        pass


sm_manager = SM_Manager()
