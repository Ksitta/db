from errors.err_sm_manager import *
from singleton import singleton

@singleton
class SM_Manager():
    def __init__(self):
        self._using_db : str = ""
        self._db_names : list = list()

    def open_db(self, db_name : str):
        if (self._using_db != ""):
            self.close_db()
        self._using_db = db_name
        pass

    def close_db(self):
        if (self._using_db == ""):
            return
        self._using_db = ""
        pass

    def show_dbs():
        pass

    def create_table(self, rel_name: str, attributes):
        if(self._using_db == ""):
            raise NoUsingDatabaseError

    def describe_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError


    def drop_table(self, rel_name : str):
        if(self._using_db == ""):
            raise NoUsingDatabaseError

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
