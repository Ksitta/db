from abc import abstractmethod
from enum import Enum
from records.record import Record
from common.common import *
from typing import Union

class Condition:
    """ Base class for all operators.
        This class defines the
    """
    @abstractmethod
    def fit(self, record):
        pass

    @abstractmethod
    def fit(self, left_record, right_record):
        pass


class AlgebraCondition(Condition):
    def __init__(self, operator: Operator, col_idx: int, value: Union[int, str, float]):
        self._operator = operator
        self._col_idx = col_idx
        self._value = value
        
    def fit(self, record):
        if(self._operator == Operator.OP_EQ):
            return record[self._col_idx] == self._value
        if(self._operator == Operator.OP_NE):
            return record[self._col_idx] != self._value
        if(self._operator == Operator.OP_GT):
            return record[self._col_idx] > self._value
        if(self._operator == Operator.OP_GE):
            return record[self._col_idx] >= self._value
        if(self._operator == Operator.OP_LT):
            return record[self._col_idx] < self._value
        if(self._operator == Operator.OP_LE):
            return record[self._col_idx] <= self._value
        return False

    def fit(self, left_record, right_record):
        return False


class JoinCondition(Condition):
    def __init__(self, left_col: int, right_col: int):
        self._left_col = left_col
        self._right_col = right_col

    def fit(self, record):
        return False

    def fit(self, left_record: Record, right_record: Record):
        if(left_record.get_field(self._left_col) == right_record.get_field(self._right_col)):
            return True
        return False