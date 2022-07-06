import numpy as np
import random
from itertools import permutations

import logging

class Position:
    def __init__(self, N, n):
        self.N = N
        self.n = n
        self.scope = range(N)
        #self.scope = range(0, N)
        self.item_done = {}
        for each in self.scope:
            self.item_done[each] = []
        #----------------------------
        self.rows = n
        self.columns = N/n
        logging.debug("row, columns-", self.rows, self.columns)
        self.matrix = self.generate_disk_matrix()
        self.all_stripesets = []


    def generate_disk_matrix(self):
        newscope = self.scope
        if self.rows*self.columns != len(self.scope):
            newscope = self.scope[1:self.rows*self.columns+1]
        permu = np.array(newscope)
        matrix = permu.reshape(self.rows, self.columns)
        if self.rows != self.columns:
            for i in range(self.columns - self.rows):
                adds = np.ones(self.columns)*(-1)
                matrix= np.vstack([matrix, adds])
        self.rows = self.columns
        logging.debug(matrix)
        return matrix

    def generate_position_matrix(self):
        self.item_map = {}
        #---------------------------------------------
        # fix the first row for 1st disk in positions
        #---------------------------------------------
        firstP = range(1, self.columns+1)
        self.item_map[firstP[0]] = []
        positions = firstP[1:]
        for i in positions:
            self.item_map[firstP[0]].append(positions)
            logging.debug(firstP[0],">>>>>",positions)
            positions = [x for x in np.roll(positions,1)]
        #---------------------------------------------
        # create other rows positions, start with 2nd rows
        #---------------------------------------------
        positionScope = range(2, self.columns+1)
        logging.debug(positionScope)
        #----------------------------
        # look for the best positions
        #----------------------------
        bestPositions = []
        permu = list(permutations(range(3, self.columns+1)))
        #permu = []
        for each in permu:
            diff_list = []
            for i in range(1, len(each)):
                if each[i] > each[i-1]:
                    diff = each[i] - each[i-1]
                else:
                    diff = -(self.N-each[i-1]+each[i])
                    #diff = -(each[i] - each[i-1])
                if diff not in diff_list:
                    diff_list.append(diff)
            if len(diff_list) == self.columns-3:
                bestPositions = [i for i in each]
                break
        logging.debug("start2 --> N, n", self.N, self.n, bestPositions)
        logging.debug("2 >>>>>",bestPositions)
        #bestPositions = [7, 3, 6, 4, 5]
        #7bestPositions = [3,5,6,4,7]
        #bestPositions = [3, 4, 7, 6, 8, 5]
        #8bestPositions = [4, 6, 7, 3, 8, 5]
        #9bestPositions = [3, 8, 7, 9, 5, 6, 4]
        #bestPositions = [3, 8, 6, 9, 10, 5, 7, 4]
        #bestPositions = [3, 4, 6, 10, 7, 12, 11, 9, 5, 8]
        #bestPositions = [3, 4, 6, 10, 7, 12, 11, 9, 5, 8]
        #bestPositions = [10, 9, 6, 8, 4, 5, 3, 7]
        #bestPositions = [6,3,5,4]
        #---------------------------------------
        # position matrix initialization
        #---------------------------------------
        matrix =np.array([[0 for i in range(self.columns)] for j in range(self.rows-1)])
        matrix[:,0] = range(2, self.rows+1)
        matrix[:,1] = [1 for i in range(self.rows-1)]
        columnId = 2
        for item in bestPositions:
            index = positionScope.index(item)
            matrix[:,columnId] = np.roll(positionScope, -index)
            columnId = columnId + 1
        logging.debug("position matrix>>>>>>\n", matrix)
        #------------------------------
        for row in range(self.rows-1):
            row_items = matrix[row]
            start = row_items[0]
            end = [i for i in row_items if i!=start]
            logging.debug(start, end)
            self.item_map[start] = []
            positions = end
            for i in positions:
                self.item_map[start].append(positions)
                logging.debug(">>>>>",positions)
                positions = [x for x in np.roll(positions,1)]
        logging.debug(self.item_map)


    def generate_row_based_stripesets(self):
        matrix = self.matrix
        logging.debug(matrix)
        for row in range(self.rows):
            row_items = matrix[row,:]
            num_sets = len(row_items) / self.n
            logging.debug(row, "row-items", row_items, num_sets)
            for i in range(num_sets):
                newset = row_items[i*self.n: i*self.n+self.n]
                self.add_into_stripesets(newset)


    def generate_column_based_stripesets(self):
        matrix = self.matrix
        for column in range(self.columns):
            newset = matrix[:,column]
            self.add_into_stripesets(newset)


    def generate_row_column_stripesets(self):
        matrix = self.matrix
        first_row = matrix[0:,]
        logging.debug("----",first_row)
        for item in self.item_map:
            for positions in self.item_map[item]:
                newset = [item]
                logging.debug(item, positions)
                for i in range(len(positions)):
                    p = positions[i]
                    newset.append(matrix[i+1, p-1])
                logging.debug("newset-", newset)
                self.add_into_stripesets(newset)

    def finalize_stripesets(self):    
        for each in self.item_done:
            used = []
            for setx in self.item_done[each]:
                for item in setx:
                    if item not in used:
                        used.append(item)
            remain = np.setdiff1d(self.scope, used)
            logging.debug(">>>>>>>>", each, remain)
            if len(remain) == self.n -1:
                newset = [each]
                for x in remain:
                    newset.append(x)
                self.add_into_stripesets(newset)




    def add_into_stripesets(self, setx):
        sety = sorted(setx)
        if sety not in self.all_stripesets:
            if -1 in sety:
                sety = [x for x in sety if x!=-1]
            if len(sety) > self.n:
                sety = sety[:self.n]
            self.all_stripesets.append(sety)
            for each in sety:
                if each == -1:
                    break
                self.item_done[each].append(sety)
        else:
            logging.debug("************", sety)




if __name__ == "__main__":
    sim = Position(36, 6)
    sim.generate_position_matrix()
    sim.generate_row_based_stripesets()
    sim.generate_column_based_stripesets()
    sim.generate_row_column_stripesets()
    sim.finalize_stripesets()
    logging.debug("-------N, n--------", len(sim.all_stripesets))
    #for setx in sim.all_stripesets:
    #   print setx
    #for each in sim.item_done:
    #    print each, sim.item_done[each]
