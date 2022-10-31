from errors.err_sm_manager import *
import os
from config.sm_config import *

class SM_Manager():
    def __init__(self):
        self._using_db : str = ""
        self._db_names : set = set()
        if(not os.path.exists(DATABASE_PATH)):
            os.mkdir(DATABASE_PATH)
        os.chdir(DATABASE_PATH)
        self._base_dir : str = os.path.abspath(".")
        files = os.listdir(".")
        for file in files:
            self._db_names.add(file)

    def open_db(self, db_name : str):
        if (self._using_db != ""):
            self.close_db()
        if (db_name not in self._db_names):
            raise NonExistDataBaseError(((f'Database {db_name} is not existed')))
        self._using_db = db_name
        os.chdir(db_name)
        pass

    def close_db(self):
        if (self._using_db == ""):
            return
        self._using_db = ""
        os.chdir(self._base_dir)
        pass

    def show_dbs():
        pass

    def create_table(self, rel_name: str, attributes):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))

    def describe_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))


    def drop_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))

    def create_index(self, rel_name : str, attr_name : str):
        pass
    
    def drop_index(self, rel_name : str, attr_name : str):
        pass
    
    def load(self, rel_name : str, file_name : str):
        pass
    
    def help(self):
        pass
    
    def help(self, rel_name : str):
        pass
    
    def set(self, param_name : str, value : str):
        pass


sm_manager = SM_Manager()