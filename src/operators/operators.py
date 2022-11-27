from abc import abstractmethod
from records.record import Record, RecordList
from typing import List
from table.table import Table
from operators.conditions import Condition
from common.common import *
from operators.conditions import *


class OperatorBase:
    """ Base class for all operators.
        This class defines the
    """
    @abstractmethod
    def process(self) -> RecordList:
        pass

    def get_column_idx(self, col: Col) -> int:
        for i in range(len(self._columns)):
            each = self._columns[i]
            if each.col_name == col.col_name and each.table_name == col.table_name:
                return i


class ProjectNode(OperatorBase):
    """ ProjectNode is a node that projects the input records.
        It takes a list of column names and returns a list of records
        with only the specified columns.
    """

    def __init__(self, child: OperatorBase, columns: List[Col]) -> None:
        self._child: OperatorBase = child
        self._columns: List[Col] = columns

    def process(self) -> RecordList:
        inlist = self._child.process()
        i = 0
        j = 0
        proj: List[int] = list()
        while i != len(self._columns):
            while j != len(inlist.columns):
                if self._columns[i].col_name == inlist.columns[j].col_name and self._columns[i].table_name == inlist.columns[j].table_name:
                    proj.append(j)
                    break
                j += 1
            i += 1
        outlist: RecordList = RecordList()
        for each in inlist.records:
            one_rec = Record()
            for i in proj:
                one_rec.append(each[i])
            outlist.append(one_rec)
        outlist.set_columns(self._columns)
        return outlist


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
        outlist: RecordList = RecordList(self._columns)
        for each in inlist.records:
            if (self._condition.fit(each)):
                outlist.append(each)
        return outlist


class TableScanNode(OperatorBase):
    """ TableScanNode is a node that scans a table.
        It takes a table and returns a list of records.
    """

    def __init__(self, table: Table):
        self._table = table
        tb_name = table.get_name()
        self._columns = [Col(col_name, tb_name)
                         for col_name in table.get_column_names()]

    def process(self) -> RecordList:
        result: List[Record] = self._table.load_all_records()
        table_name = self._table.get_name()
        cols = [Col(each, table_name)
                for each in self._table.get_column_names()]
        return RecordList(cols, result)


class JoinNode(OperatorBase):
    def __init__(self, left: OperatorBase, right: OperatorBase, condition: JoinCondition):
        self._left = left
        self._right = right
        self._condition: JoinCondition = condition
        self._columns = left._columns + right._columns

    def process(self) -> RecordList:
        left_result: RecordList = self._left.process()
        right_result: RecordList = self._right.process()
        result: List[Record] = []
        self._condition._left_col = self._left.get_column_idx(self._condition._left_col)
        self._condition._right_col = self._right.get_column_idx(self._condition._right_col)
        for left_record in left_result.records:
            for right_record in right_result.records:
                if self._condition.fit(left_record, right_record):
                    result.append(Record.concat(left_record, right_record))
        return RecordList(left_result.columns + right_result.columns, result)
