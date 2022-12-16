from typing import List

def list_int_to_int(l: List[int]) -> int:
    ''' Convert a list of integers to a single integer.
    args:
        l: List[int], a list of integers.
    return: int, the single integer.
    '''
    ret = 0
    for i in l:
        if(i < 0):
            return -1
        ret = 1 << i | ret
    return ret

def int_to_list_int(i: int) -> List[int]:
    ''' Convert a single integer to a list of integers.
    args:
        i: int, the single integer.
    return: List[int], the list of integers.
    '''
    ret = []
    base = 0
    while i:
        if(i & 1):
            ret.append(base)
        base += 1
        i = i >> 1
    return ret