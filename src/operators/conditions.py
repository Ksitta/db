from abc import abstractmethod
from enum import Enum
from common.common import *
from typing import Union

class Condition:
    """ Base class for all operators.
        This class defines the
    """
    pass

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

    def __init__(self, operator: Operator, col_idx: int, value: Union[int, str, float]):
        self._col_idx = col_idx
        self._value = value
        if (operator == Operator.OP_EQ):
            self._compare = self._eq
        elif (operator == Operator.OP_NE):
            self._compare = self._neq
        elif (operator == Operator.OP_GT):
            self._compare = self._gt
        elif (operator == Operator.OP_GE):
            self._compare = self._ge
        elif (operator == Operator.OP_LT):
            self._compare = self._lt
        elif (operator == Operator.OP_LE):
            self._compare = self._le
        else:
            raise Exception("Invalid operator")

    def fit(self, record: Record):
        return self._compare(record.data[self._col_idx], self._value)


class JoinCondition(Condition):
    def __init__(self, left_col: Col, right_col: Col):
        self._left_col = left_col
        self._right_col = right_col

    def fit(self, left_record: Record, right_record: Record):
        if(left_record.data[self._left_col] == right_record.data[self._right_col]):
            return True
        return False