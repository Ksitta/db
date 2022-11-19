from abc import abstractmethod
from enum import Enum

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

class AlgebraOperator(Enum):
    """ Base class for all operators.
        This class defines the
    """
    EQUAL = 1
    LESS = 2
    LESS_EQUAL = 3
    GREATER = 4
    GREATER_EQUAL = 5
    NOT_EQUAL = 6

class AlgebraCondition(Condition):
    def __init__(self, operator: AlgebraOperator):
        self._operator = operator

    def fit(self, record):
        pass

    def fit(self, left_record, right_record):
        return False
        
