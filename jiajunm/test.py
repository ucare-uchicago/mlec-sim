import operator as op
from functools import reduce

def ncr(n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        # print(range(n, n-r, -1))
        denom = reduce(op.mul, range(1, r+1), 1)
        # print(numer, denom)
        return numer / denom

def p31(s, d):
        first = 3*s/d
        second = (3*s*(s-1))/(d*(d-1))
        third = 2*(3*s*(s-1)*(s-2))/(d*(d-1)*(d-2))
        
        return first-second+third