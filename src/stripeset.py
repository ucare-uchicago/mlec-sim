import numpy as np
import random
import logging

class Stripeset:
    #----------------------------------------------
    # initialize the stripesets with scope, k, m
    #----------------------------------------------
    def __init__(self, scope, k, m):
        self.scope = scope 
        self.k = k
        self.m = m
        #----------------------------------------
        # covert scope into (k+m)x(N/k+m) matrix
        #----------------------------------------
        self.rows = k+m
        self.columns = int(len(scope)/(k+m))
        if self.rows * self.columns != len(scope):
            self.scope = np.array(random.sample(self.scope, self.rows*self.columns))
        logging.debug("scope", scope, "k+m", k, m)
        #---------------------------------
        # collect all the stripesets
        #---------------------------------
        self.all_stripesets = []


    #-----------------------------------------
    # generate row_based stripesets
    #-----------------------------------------
    def generate_row_based_stripesets(self):
        #---------------------------------------
        # covert scope into (k+m)x(N/k+m) matrix
        #---------------------------------------
        matrix = np.array(self.scope).reshape(self.rows, self.columns)
        for rowId in range(self.rows):
            stripeset = matrix[rowId,:]
            self.all_stripesets.append(stripeset)
            logging.debug("rowId", rowId, stripeset)
        

    #----------------------------------------
    # generate column shift based stripesets
    #----------------------------------------
    def generate_column_based_stripesets(self):
        shift_matrix = self.generate_shift_matrix()
        all_stripesets = self.scope
        return all_stripesets

    #----------------------------------------
    # generate the shift matrix for each row
    #----------------------------------------
    def generate_shift_matrix(self):
        matrix = []
        logging.debug("* generate shift matrix *")
        return matrix






if __name__ == "__main__":
    sim = Stripeset(range(1, 101), 8, 2)
    sim.generate_row_based_stripesets()
    sim.generate_column_based_stripesets()
