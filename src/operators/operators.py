from abc import abstractmethod
from records.record import Record
from typing import List
from table.table import Table

class OperatorBase:
    """ Base class for all operators.
        This class defines the
    """
    @abstractmethod
    def process(self):
        pass

    

class FilterOperator(OperatorBase):
    pass


class TableScanNode(OperatorBase):
    def __init__(self, table: Table):
        self._table = table
    
    def process(self):
        result: List[Record] = self._table.load_all_records()
        return result
    
class JoinNode(OperatorBase):
    def __init__(self, left: OperatorBase, right: OperatorBase, condition):
        self._left = left
        self._right = right
        self._condition = condition
        
    