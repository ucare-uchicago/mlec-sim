from itertools import permutations 
import numpy as np

from constants import debug

class Prime:
    def __init__(self, N, n):
        self.N = N
        self.n = n
        self.scope = range(1,N+1)
        #------------------------------------
        self.rows = n
        self.columns = N/n
        self.all_stripesets = []
        #------------------------------------
        self.generate_stripesets()
        logging.debug("--------N, n---------",N, n, len(self.all_stripesets))
        for setx in self.all_stripesets:
            logging.debug(setx)


    def generate_disk_matrix(self):
        permu = np.array(self.scope)
        logging.debug(permu, self.rows, self.columns)
        matrix = permu.reshape(self.rows, self.columns)
        logging.debug(matrix)
        return matrix

    def generate_stripesets(self):
        matrix = self.generate_disk_matrix()
        #------------------------------------
        # generate row-based stripesets
        #------------------------------------
        for row in range(self.rows):
            self.add_into_stripesets(matrix[row,:])
        #------------------------------------
        # generate row-column stripesets
        #------------------------------------
        for item_num in range(self.columns):
            logging.debug("---------------\n", matrix)
            for column in range(self.columns):
                newset = matrix[:,column]
                self.add_into_stripesets(newset)
            matrix = self.shift_disk_matrix(matrix)

    def shift_disk_matrix(self, matrix):
        new_matrix = []
        for row in range(self.rows):
            new_matrix.append(np.roll(matrix[row],-row))
        return np.array(new_matrix)


    def add_into_stripesets(self, setx):
        sety = sorted(setx)
        if sety not in self.all_stripesets:
            self.all_stripesets.append(sety)


if __name__ == "__main__":
    #prime = Prime(49, 7)
    prime = Prime(25, 5)
