import numpy as np
import random
import logging

class Brute:
    def __init__(self, N, n):
        
        self.scope = range(1, N+1)
        self.columns = N/n
        self.rows = n
        #---------------------------
        self.map = {}
        self.item_lack = {}
        self.all_sets = []
        self.all_todo = []

    def generate_matrix(self):
        permu = np.array(self.scope)
        matrix = permu.reshape(self.rows, self.columns)
        #---------------------------------
        # extract the row items
        #---------------------------------
        for row in range(self.rows):
            row_items = matrix[row]
            for item in row_items:
                self.item_lack[item] = [i for i in row_items if i!=item]
        xx = np.zeros((self.rows,1), dtype=int)
        matrix = np.append(matrix, xx, axis=1)
        #--------------------------------------------
        for iter_num in range(self.columns+1):
            matrix = self.shift_matrix(matrix)
            for column in range(self.columns+1):
                stripeset = matrix[:,column]
                self.all_sets.append(stripeset)
            logging.debug("++++++++++++++++++++++++++" + iter_num)
            logging.debug(matrix)
        
        logging.debug("---------------------------------")
        self.analyze_stripesets(self.all_sets)


    def shift_matrix(self, matrix):
        new_matrix = []
        for row in range(self.rows):
            new_matrix.append(np.roll(matrix[row], -row))
        return np.array(new_matrix)


    def analyze_stripesets(self, all_sets):
        for setx in all_sets:
            for item in self.scope:
                if item in setx:
                    if item not in self.map:
                         self.map[item] = [setx]
                    else:
                        self.map[item].append(setx)
        self.analyze_each_item()


    def analyze_each_item(self):
        self.item_done = {}
        self.item_todo = {}
        for item in self.map:
            self.item_done[item] = []
            self.item_todo[item] = []
        
            logging.debug(str(item) + " >>>>" + str(self.map[item]))
            res = []
            for setx in self.map[item]:
                if 0 not in setx:
                    self.item_done[item].append(setx)
                else:
                    for each in setx:
                        if each != 0 and each != item:
                            if each not in self.item_todo[item]:
                                self.item_todo[item].append(each)
        for done in self.item_done:
            logging.debug(str(done) + str(self.item_done[done]))
        for each in self.item_todo:
            #print "* ", self.item_done[each]
            item_comb = {}
            todo = self.item_todo[each]
            lack = self.item_lack[each]
            
            logging.debug(each, ">> todo", todo, ">> lack", lack, ">> ",done)
            for todo_item in todo:
                res = []
                for done_set in self.item_done[todo_item]:
                    for lack_item in lack:
                        if lack_item in done_set:
                            res.append(lack_item)
                item_comb[todo_item] = np.setdiff1d(np.array(lack), np.array(res))

            logging.debug("     >>>> ",item_comb)
            lacks = []
            for key in item_comb:
                lack = item_comb[key]
                
                logging.debug(key, lack)


if __name__ == "__main__":
    #brute = Brute(36, 6)
    brute = Brute(16, 4)
    #brute.generate_sets(1)
    brute.generate_matrix()
