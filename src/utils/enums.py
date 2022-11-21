from enum import Enum, unique


@unique
class CompOp(Enum):
    EQ = 0  # equal
    LT = 1  # less than
    GT = 2  # greater than
    LE = 3  # less than or equal
    GE = 4  # greater than or equal
    NE = 5  # not equal
    NO = 6  # no comparison
    