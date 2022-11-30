from common.common import *
class Printer():
    def __init__(self, records) -> None:
        self._records = records

    def display(self):
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
        if (type(self._records) is RecordList):
            for each in self._records.columns:
                if each.aggregator:
                    print(each.aggregator.name + "(" + each.col_name + ")", end="\t")
                else:
                    print(each.col_name, end='\t')
            for each in self._records.records:
                print()
                for each in each.data:
                    print(each, end='\t')
            print()