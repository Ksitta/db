from common.common import *
from typing import Set
class Printer():
    def __init__(self, records, interval) -> None:
        self._records = records
        self._interval = interval

    def display(self):
        self.print_inner()
        print("%d rows in set (%.3f sec)" % (self._rows, self._interval))

    def print_edge(self, width):
        print("+", end="")
        for each in width:
            print("-" * (each + 2), end="+")
        print()

    def print_inner(self):
        self._rows = 0
        width = []
        if(self._records is None):
            width.append(7)
            self.print_edge(width)
            print("| SUCCESS |")
            self.print_edge(width)
            return
        if (type(self._records) is Result):
            for each in self._records._header:
                width.append(len(each))
            for each in self._records._results:
                for i in range(len(each)):
                    width[i] = max(width[i], len(str(each[i])))
            
            self.print_edge(width)
            print("| ", end="")
            for i in range(len(self._records._header)):
                print("%-*s" % (width[i], self._records._header[i]), end=" | ")
            print()
            self.print_edge(width)
            if len(self._records._results) == 0:
                return
            self._rows = len(self._records._results)
            for each in self._records._results:
                print("| ", end="")
                for i in range(len(each)):
                    print("%-*s" % (width[i], str(each[i])), end=" | ")
                print()
            self.print_edge(width)
            return
        
        if (type(self._records) is RecordList):
            for each in self._records.columns:
                col_width = len(each.col_name)
                if each.aggregator:
                    col_width = len(each.aggregator.name + "(" + each.col_name + ")")
                width.append(col_width)
            for each in self._records.records:
                for i in range(len(each.data)):
                    width[i] = max(width[i], len(str(each.data[i])))
            
            self.print_edge(width)
            print("| ", end="")
            for i in range(len(self._records.columns)):
                if self._records.columns[i].aggregator:
                    print("%-*s" % (width[i], self._records.columns[i].aggregator.name + "(" + self._records.columns[i].col_name + ")"), end=" | ")
                else:
                    print("%-*s" % (width[i], self._records.columns[i].col_name), end=" | ")
            print()
            self.print_edge(width)
            if len(self._records.records) == 0:
                return
            self._rows = len(self._records.records)
            for each in self._records.records:
                print("| ", end="")
                for i in range(len(each.data)):
                    print("%-*s" % (width[i], str(each.data[i])), end=" | ")
                print()
            self.print_edge(width)