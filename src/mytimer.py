class Mytimer:
    def __init__(self):
        self.seedtime = 0
        self.copytime = 0
        self.genfailtime = 0
        self.resettime = 0
        self.resetStateInitTime = 0
        self.resetDiskInitTime = 0
        self.resetGenFailTime = 0
        self.resetHeapTime = 0
        self.getEventTime = 0
        self.updateClockTime = 0
        self.updateStateTime = 0
        self.updateServerStateTime = 0
        self.updatePriorityTime = 0
        self.updateServerPriorityTime = 0
        self.newFailTime = 0
        self.checkLossTime = 0
        self.updateRepairTime = 0
    
    def __str__(self):
        return (" seedtime\t\t{}\n copytime\t\t{}\n genfailtime\t\t{}\n" 
                " resettime\t\t{}\n   resetStateInitTime\t{}\n   resetDiskInitTime\t{}\n   resetGenFailTime\t{}\n   resetHeapTime\t{}\n"
                " getEventTime\t\t{}\n updateClockTime\t{}\n updateStateTime\t{}\n"
                " updateServerStateTime\t{}\n updatePriorityTime\t{}\n updateServerPriTime\t{}\n"
                " newFailTime\t\t{}\n checkLossTime\t\t{}\n updateRepairTime\t{}\n").format(
            self.seedtime,
            self.copytime,
            self.genfailtime,
            self.resettime,
            self.resetStateInitTime,
            self.resetDiskInitTime,
            self.resetGenFailTime,
            self.resetHeapTime,
            self.getEventTime,
            self.updateClockTime,
            self.updateStateTime,
            self.updateServerStateTime,
            self.updatePriorityTime,
            self.updateServerPriorityTime,
            self.newFailTime,
            self.checkLossTime,
            self.updateRepairTime
        )
