import operator as op
from functools import reduce

def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)
    
def ncr(self, n, r):
    r = min(r, n-r)
    numer = reduce(op.mul, range(n, n-r, -1), 1)
    denom = reduce(op.mul, range(1, r+1), 1)
    return numer / denom