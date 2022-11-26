from records.record import Record, RecordList

class Printer():
    def __init__(self, records: RecordList) -> None:
        self._records: RecordList = records

    def display(self):
        if(self._records is None):
            print("SUCCESS")
            return
        
        for each in self._records.columns:
            print(each.col_name, end='\t')
        for each in self._records.records:
            print()
            for each in each.data:
                print(each, end='\t')
        print()