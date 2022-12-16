from abc import abstractmethod
from typing import List
from table.table import Table
from operators.conditions import Condition, AlgebraCondition
from common.common import *
from operators.conditions import *
from utils.bitwise import *

class OperatorBase:
    """ Base class for all operators.
        This class defines the operator interface.
    """
    @abstractmethod
    def process(self) -> RecordList:
        pass

    def get_column_idx(self, col: Col) -> int:
        for i in range(len(self._columns)):
            each = self._columns[i]
            if each.col_name == col.col_name:
                if col.table_name:
                    if each.table_name == col.table_name:
                        return i
                else:
                    return i


class ProjectNode(OperatorBase):
    """ ProjectNode is a node that projects the input records.
        It takes a list of column names and returns a list of records
        with only the specified columns.
    """
    @staticmethod
    def _count(records: List[Record], col_idx: int) -> int:
        return len(records)

    @staticmethod
    def _sum(records: List[Record], col_idx: int) -> Union[int, float]:
        return sum([each.data[col_idx] for each in records])

    @staticmethod
    def _avg(records: List[Record], col_idx: int) -> Union[int, float]:
        return ProjectNode._sum(records, col_idx) / ProjectNode._count(records, col_idx)

    @staticmethod
    def _max(records: List[Record], col_idx: int) -> Union[int, float]:
        return max([each.data[col_idx] for each in records])

    @staticmethod
    def _min(records: List[Record], col_idx: int) -> Union[int, float]:
        return min([each.data[col_idx] for each in records])

    def __init__(self, child: OperatorBase, columns: List[Col]) -> None:
        self._child: OperatorBase = child
        self._columns: List[Col] = columns
        self._aggregators: List[function] = list()
        for each in self._columns:
            if each.aggregator:
                if each.aggregator == Aggregator.COUNT:
                    agg_func = self._count
                elif each.aggregator == Aggregator.SUM:
                    agg_func = self._sum
                elif each.aggregator == Aggregator.AVG:
                    agg_func = self._avg
                elif each.aggregator == Aggregator.MAX:
                    agg_func = self._max
                elif each.aggregator == Aggregator.MIN:
                    agg_func = self._min
                else:
                    raise Exception("Invalid aggregator")
                self._aggregators.append(agg_func)

        if (len(self._aggregators) != 0 and len(self._aggregators) != len(self._columns)):
            raise Exception("Columns should be either all aggregated or not aggregated")

    def process(self) -> RecordList:
        inlist = self._child.process()
        proj: List[int] = list()
        proj = [inlist.get_column_idx(col) for col in self._columns]
        records = []
        for each in inlist.records:
            one_rec = Record([])
            for i in proj:
                one_rec.append(each.data[i])
            records.append(one_rec)
        if (len(self._aggregators) != 0):
            records = [Record([agg(records, i) for i, agg in enumerate(self._aggregators)])]
        return RecordList(self._columns, records)


class FilterNode(OperatorBase):
    """ FilterNode is a node that filters the input records.
        It takes a list of conditions and returns a list of records
        that satisfy all conditions.
    """

    def __init__(self, child: OperatorBase, condition: Condition) -> None:
        self._child: OperatorBase = child
        self._condition: Condition = condition
        self._columns = child._columns

    def process(self):
        inlist: RecordList = self._child.process()
        records = []
        for each in inlist.records:
            if (self._condition.fit(each)):
                records.append(each)
        return RecordList(self._columns, records)


class TableScanNode(OperatorBase):
    """ TableScanNode is a node that scans a table.
        It takes a table and returns a list of records.
    """

    def __init__(self, table: Table):
        self._table = table
        tb_name = table.get_name()
        self._inline_condition = []
        self._condition = []
        self._columns = [Col(col_name, tb_name)
                         for col_name in table.get_column_names()]

    def process(self) -> RecordList:
        if (len(self._inline_condition) == 0):
            records: List[Record] = self._table.load_all_records()
        else:
            records: List[Record] = self._table.load_records_with_cond(self._inline_condition)
        
        # records: List[Record] = self._table.load_all_records()
        table_name = self._table.get_name()
        cols = [Col(each, table_name)
                for each in self._table.get_column_names()]
        results = []
        for each in records:
            for cond in self._condition:
                if (not cond.fit(each)):
                    break
            else:
                results.append(each)
        return RecordList(cols, results)

    def add_condition(self, cond: Condition):
        index_no = list_int_to_int([cond.get_col_idx()])
        if(self._table.index_exist(index_no)):
            self._inline_condition.append(cond)
        else:
            self._condition.append(cond)

        # self._condition.append(cond)


class JoinNode(OperatorBase):
    """ JoinNode is a node that joins two tables.
        It takes two tables and a condition and returns a list of records
        that satisfy the condition.
    """

    def __init__(self, left: OperatorBase, right: OperatorBase, condition: JoinCondition):
        self._left = left
        self._right = right
        self._condition: JoinCondition = condition
        self._columns = left._columns + right._columns

    def process(self) -> RecordList:
        left_result: RecordList = self._left.process()
        right_result: RecordList = self._right.process()
        result: List[Record] = []
        if(self._condition == None):
            for left_record in left_result.records:
                for right_record in right_result.records:
                    result.append(Record.concat(left_record, right_record))
            return RecordList(left_result.columns + right_result.columns, result)
        left_col = self._left.get_column_idx(self._condition._left_col)
        right_col = self._right.get_column_idx(self._condition._right_col)
        left_dict = {}
        for each in left_result.records:
            left_dict.setdefault(each.data[left_col], []).append(each)

        for right_record in right_result.records:
            if right_record.data[right_col] in left_dict:
                for each in left_dict[right_record.data[right_col]]:
                    result.append(Record.concat(each, right_record))
                    
        return RecordList(left_result.columns + right_result.columns, result)
