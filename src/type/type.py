from abc import ABCMeta, abstractmethod
from enum import Enum


class TypeEnum(Enum):
    INT = 0
    VARCHAR = 1
    FLOAT = 2


class BaseType(ABCMeta):
    @abstractmethod
    def get_length() -> int:
        pass

    @abstractmethod
    def get_type() -> TypeEnum:
        pass


class IntType(BaseType):
    def __init__(self):
        return

    def get_length(self) -> int:
        return 4

    def get_type() -> TypeEnum:
        return TypeEnum.FLOAT


class VarcharType(BaseType):
    def __init__(self, length: int):
        self.length = length

    def get_length(self) -> int:
        return self.length

    def get_type(self) -> TypeEnum:
        return TypeEnum.VARCHAR


class FloatType(BaseType):
    def __init__(self):
        return

    def get_length(self) -> int:
        return 4

    def get_type(self) -> TypeEnum:
        return TypeEnum.FLOAT
