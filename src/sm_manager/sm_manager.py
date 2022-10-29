from errors.err_sm_manager import *
from utils.singleton import singleton
import os
from config.sm_config import *
from paged_file.pf_file_manager import PF_FileManager

@singleton
class SM_Manager():
    def __init__(self):
        self._using_db : str = ""
        self._db_names : set = set()
        self._fd2table : dict = dict()
        self._table2datafd : dict = dict()
        self._table2metafd : dict = dict()
        self._name2table : dict = dict()
        self._tables : set = set()
        
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
        if (rel_name in self._tables):
            raise TableExistsError(rel_name)
        
        pf_manager = PF_FileManager()
        pf_manager.create_file(rel_name + TABLE_DATA_SUFFIX)
        pf_manager.create_file(rel_name + TABLE_META_SUFFIX)
        
        data_fd : int = pf_manager.open_file(rel_name + TABLE_DATA_SUFFIX)
        meta_fd : int = pf_manager.open_file(rel_name + TABLE_META_SUFFIX)

        self._fd2table[meta_fd] = rel_name
        self._fd2table[data_fd] = rel_name
        
        self._table2datafd[rel_name] = data_fd
        self._table2metafd[rel_name] = meta_fd

        # self._name2table[rel_name] = Table()
        

    def describe_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        

    def drop_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError((f'No database is opened'))
        if (rel_name not in self._tables):
            raise TableNotExistsError(rel_name)
        pf_manager = PF_FileManager()
        pf_manager.close_file(self._table2datafd[rel_name])
        pf_manager.close_file(self._metafd2table[rel_name])
        pf_manager.remove_file(rel_name + TABLE_DATA_SUFFIX)
        pf_manager.remove_file(rel_name + TABLE_META_SUFFIX)
                
        self._tables.remove(rel_name)
        self._fd2table.remove(self._table2datafd[rel_name])
        self._fd2table.remove(self._table2metafd[rel_name])
        self._tables.remove(rel_name)
        self._table2metafd.remove(rel_name)
        self._table2datafd.remove(rel_name)
        

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
