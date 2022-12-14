import os
import re
import numpy as np
from glob import glob
from typing import List, Dict, Tuple, Union

import config as cf
from paged_file.pf_manager import pf_manager
from index_management.ix_index_handle import IX_IndexHandle
from index_management.ix_tree_node import node_cache, flush_node_cache
from index_management.ix_rid_bucket import bucket_cache, flush_bucket_cache


class IX_Manager:
    
    
    def __init__(self) -> None:
        self.opened_indicies: Dict[str, IX_IndexHandle] = dict()
        
    
    def create_index(self, file_name:str, index_no:int) -> None:
        ''' Create index meta and data files with a specific index number.
        args:
            file_name: str, the index file name with out extension.
            index_no: int, the index number of the above file, must be unique for each file.
        '''
        pf_manager.create_file(f'{file_name}.{index_no}{cf.INDEX_META_SUFFIX}')
        pf_manager.create_file(f'{file_name}.{index_no}{cf.INDEX_DATA_SUFFIX}')
        
    
    def remove_index(self, file_name:str, index_no:int) -> None:
        ''' Remove the index <file_name>.<index_no>.
        '''
        pf_manager.remove_file(f'{file_name}.{index_no}{cf.INDEX_META_SUFFIX}')
        pf_manager.remove_file(f'{file_name}.{index_no}{cf.INDEX_DATA_SUFFIX}')
    
    
    def open_index(self, file_name:str, index_no:int) -> IX_IndexHandle:
        ''' Open the index <file_name>.<index_no> and return its index handle.
        return: IX_IndexHandle, the index handle for further index operations.
        '''
        index_name = f'{file_name}.{index_no}'
        if index_name in self.opened_indicies:
            return self.opened_indicies[index_name]
        meta_file_id = pf_manager.open_file(index_name + cf.INDEX_META_SUFFIX)
        data_file_id = pf_manager.open_file(index_name + cf.INDEX_DATA_SUFFIX)
        node_cache[data_file_id] = dict()
        bucket_cache[data_file_id] = dict()
        index_handle = IX_IndexHandle(file_name, index_no, meta_file_id, data_file_id)
        self.opened_indicies[index_name] = index_handle
        return index_handle
    
    
    def close_index(self, file_name:str, index_no:int) -> None:
        ''' Close the index <file_name>.<index_no>
        '''
        index_name = f'{file_name}.{index_no}'
        data_file_id = pf_manager.file_name_to_id[index_name + cf.INDEX_DATA_SUFFIX]
        flush_node_cache(data_file_id)
        flush_bucket_cache(data_file_id)
        pf_manager.close_file(index_name + cf.INDEX_META_SUFFIX)
        pf_manager.close_file(index_name + cf.INDEX_DATA_SUFFIX)
        handle:IX_IndexHandle = self.opened_indicies.pop(index_name, None)
        if handle: handle.is_opened = False
        
    
    @staticmethod
    def query_index(dir:str, file_name:str) -> List[int]:
        ''' Query all created index_no by a file name.
        args:
            dir: str, the database dir path.
            file_name: str, the file name without extension.
        return: List[int], a list of index numbers created under the file_name.
            A returned list with len == 0 means no indicies have been created.
        '''
        paths = glob(f'{os.path.join(dir,file_name)}.*{cf.INDEX_META_SUFFIX}')
        index_numbers = []
        for path in paths:
            if re.match(f'{os.path.join(dir,file_name)}.\d+{cf.INDEX_META_SUFFIX}', path):
                index_numbers.append(int(path.split('.')[-2]))
        return sorted(index_numbers)
        
        
ix_manager = IX_Manager()


if __name__ == '__main__':
    pass
