from abc import abstractmethod
from records.record import Record
from typing import List
from table.table import Table
from operators.conditions import Condition

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
    def __init__(self, left: OperatorBase, right: OperatorBase, condition: Condition):
        self._left = left
        self._right = right
        self._condition = condition
        
    def process(self):
        left_result: List[Record] = self._left.process()
        right_result: List[Record] = self._right.process()
        result = []
        for left_record in left_result:
            for right_record in right_result:
                if self._condition.fit(left_record, right_record):
                    result.append(left_record + right_record)
        return result
    