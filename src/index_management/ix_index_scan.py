import os
import sys
import time
import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from utils.enums import CompOp
from record_management.rm_rid import RM_Rid
from index_management.ix_index_handle import IX_IndexHandle


class IX_IndexScan:
    
    
    def __init__(self):
        ''' Init the index scan.
        '''
    
    
    def open_scan(self, index_handle:IX_IndexHandle, comp_op:CompOp=CompOp.NO,
        field_value:Union[int, float, str]=None) -> None:
        ''' Open the index scan.
        args:
            index_handle: IX_IndexHandle, an opened index handle instance.
            comp_op: CompOp, the filter condition. If comp_op == CompOp.NO,
                field_value will be ignored.
            field_value: Union[int, float, str], the value to be compared with.
        '''
    
    
    def next(self) -> RM_Rid:
        ''' Yield the next scanned rid that satisfies the filtering condition.
        '''
    
    
    def close_scan(self) -> None:
        ''' Close the index scan.
        '''


if __name__ == '__main__':
    pass
