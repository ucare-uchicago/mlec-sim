from position import Position
import random
import argparse
import logging

from constants import debug

class Prob:
    def __init__(self, N, n, m):
        self.N = N
        self.m = m
        pos = Position(N, n)
        pos.generate_position_matrix()
        pos.generate_row_based_stripesets()
        pos.generate_column_based_stripesets()
        pos.generate_row_column_stripesets()
        pos.finalize_stripesets()
        self.solsets = pos.all_stripesets
        if debug:
            "-------N, n--------", len(pos.all_stripesets)

    def sol_decluster_simulate(self, failures):
        prob = 0
        for stripeset in self.solsets:
            fail_per_set = set(stripeset).intersection(set(failures))
            if len(fail_per_set) > self.m:
                prob = 1
                return prob
        return prob



if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', type=int, help="#failures", default=2)
    args = parser.parse_args()
    exp =  Prob(169,13, 2)
    num_iters = 100000
    solprob_sum = 0.0
    solFile = open('dat/sol.txt','a')
    for i in range(num_iters):
        failures = random.sample(range(1,exp.N+1), args.f)
        solprob_sum += exp.sol_decluster_simulate(failures)
    solprob = solprob_sum / num_iters
    solFile.write("%f, %f\n" %(args.f, solprob*100))
    solFile.close()
    logging.debug("----------", solprob)


