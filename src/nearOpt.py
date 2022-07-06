import logging

class NearOpt:
    def __init__(self, N, n):
        logging.debug("-----N, n------", N, n)
        self.scope = range(1, N+1)
        self.allSets = []
        basePermu = []
        for i in range(n):
            basePermu.append(0)
        basePermu[0] = 1
        basePermu[1] = 2
        basePermu[2] = 50
        #basePermu[3] = 8
        #basePermu[4] = 13
        #basePermu[5] = 21
        #basePermu[6] = 34
        #basePermu[7] = 47
        #basePermu[8] = 61
        #basePermu[n-1] = 80
        #------------------------------
        self.allSets.append(basePermu)
        for i in range(1, N+1):
            newPermu = []
            for k in basePermu:
                if (k+i) <= N:
                    newPermu.append(k+i)
                else:
                    newPermu.append((k+i)%N)
                    if newPermu[0] == 1:
                        break
            if len(newPermu) == n:
                self.allSets.append(newPermu)
        #------------------------------
        self.item_done = {}
        self.item_status = {}


    def display_sets(self):
        for setx in self.allSets:
            logging.debug(setx)
            for item in setx:
                if item not in self.item_done:
                    self.item_done[item] = [setx]
                else:
                    self.item_done[item].append(setx)
        for item in self.scope:
           logging.debug(item, self.item_done[item])
           combine = []
           for sety in self.item_done[item]:
               for each in sety:
                   combine.append(each)
           self.item_status[item] = combine
           logging.debug("  => ", item, set(combine), "-",len(set(combine)))


    def get_diff_distances(self):
        logging.debug("------------")
        x1 = 1
        x6 = 18
        history = []


if __name__ == "__main__":
    sim = NearOpt(100, 3)
    sim.display_sets()
    #1, 4, 6, 10, 17, 18
