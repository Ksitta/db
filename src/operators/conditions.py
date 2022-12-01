from abc import abstractmethod
from enum import Enum
from common.common import *
from typing import Union, Set

class Condition:
    """ Base class for all operators.
        This class defines the
    """
    def get_col_idx(self) -> int:
        return -1


class InListCondition(Condition):
    def __init__(self, col_idx: int, values: Set[Union[int, float, str]]) -> None:
        self._col_idx: int = col_idx
        self._values = values

    def fit(self, record: Record) -> bool:
        return record.data[self._col_idx] in self._values


class AlgebraCondition(Condition):
    def _eq(self, left, right):
        return left == right
    
    def _neq(self, left, right):
        return left != right
    
    def _gt(self, left, right):
        return left > right

    def _ge(self, left, right):
        return left >= right

    def _lt(self, left, right):
        return left < right

    def _le(self, left, right):
        return left <= right

    def __init__(self, operator: CompOp, col_idx: int, value: Union[int, str, float]):
        self._col_idx = col_idx
        self._value = value
        self._operator = operator
        if (operator == CompOp.EQ):
            self._compare = self._eq
        elif (operator == CompOp.NE):
            self._compare = self._neq
        elif (operator == CompOp.GT):
            self._compare = self._gt
        elif (operator == CompOp.GE):
            self._compare = self._ge
        elif (operator == CompOp.LT):
            self._compare = self._lt
        elif (operator == CompOp.LE):
            self._compare = self._le
        else:
            raise Exception("Invalid operator")

    def fit(self, record: Record) -> bool:
        return self._compare(record.data[self._col_idx], self._value)

    def get_col_idx(self) -> int:
        return self._col_idx


class JoinCondition(Condition):
    def __init__(self, left_col: Col, right_col: Col):
        self._left_col = left_col
        self._right_col = right_col

    def fit(self, left_record: Record, right_record: Record) -> bool:
        if(left_record.data[self._left_col] == right_record.data[self._right_col]):
            return True
        return False