import operator as op
from functools import reduce

def ncr(n, r):
        r = min(r, n-r)
        numer = reduce(op.mul, range(n, n-r, -1), 1)
        # print(range(n, n-r, -1))
        denom = reduce(op.mul, range(1, r+1), 1)
        # print(numer, denom)
        return numer / denom

if __name__ == "__main__":
    print(ncr(89,8)/ncr(90,9))
    print(ncr(90,9))