from common.common import *
from typing import Set
class Printer():
    def __init__(self, records, interval) -> None:
        self._records = records
        self._interval = interval

    def display(self):
        self.print_inner()
        print("time: %.3f s" % self._interval)

    def print_inner(self):
        if(self._records is None):
            print("SUCCESS")
            return
        if (type(self._records) is Result):
            for each in self._records._header:
                print(each, end="\t")
            print()
            for results in self._records._results:
                for each in results:
                    print(each, end="\t")
                print()
            return
        hidden_cols: Set[int] = set()
        if (type(self._records) is RecordList):
            for each in self._records.columns:
                if each.aggregator:
                    print(each.aggregator.name + "(" + each.col_name + ")", end="\t")
                else:
                    print(each.col_name, end='\t')
            for each in self._records.records:
                print()
                for i in range(len(each.data)):
                    if(i in hidden_cols):
                        continue
                    print(each.data[i], end='\t')
            print()