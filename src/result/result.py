class Result():
    def __init__(self):
        self._headers : list = list()
        self._datas   : list = list()
    
    def __init__(self, headers : list):
        self._headers : list = headers.copy()
        self._datas   : list = list()
        
    def __init__(self, headers : list, datas : list):
        self._headers : list = headers.copy()
        self._datas : list = datas.copy()
        
