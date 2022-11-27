from enum import Enum

class Operator(Enum):
    OP_EQ = 0
    OP_LT = 1
    OP_LE = 2
    OP_GT = 3
    OP_GE = 4
    OP_NE = 5

class Aggregator(Enum):
    COUNT = 0
    AVG = 1
    MAX = 2
    MIN = 3
    SUM = 4

class Col:
    def __init__(self, col_name: str, table_name: str = None, aggregator: Aggregator = None):
        self.col_name = col_name
        self.table_name = table_name
        self.aggregator = aggregator
        

class Result():
    def __init__(self, header: list, results: list) -> None:
        self._header = header
        self._results = results
        