class Mytimer:
    def __init__(self):
        self.eventInitTime = 0.0
        self.simInitTime = 0.0
        self.seedtime = 0.0
        self.copytime = 0.0
        self.genfailtime = 0.0
        self.resettime = 0.0
        self.resetStateInitTime = 0.0
        self.resetDiskInitTime = 0.0
        self.resetGenFailTime = 0.0
        self.resetHeapTime = 0.0
        self.getEventTime = 0.0
        self.updateClockTime = 0.0
        self.updateStateTime = 0.0
        self.updateRackStateTime = 0.0

        self.updatePriorityTime = 0.0
        self.updateDiskRepairTime = 0.0
        self.updatePriorityFailTime = 0.0

        self.updateDiskgrpPriorityTime = 0.0
        self.updateRackPriorityTime = 0.0
        self.newFailTime = 0.0
        self.checkLossTime = 0.0
        self.updateRepairTime = 0.0
    
    def __str__(self):
        return (" simInitTime\t\t{}\n seedtime\t\t{}\n copytime\t\t{}\n genfailtime\t\t{}\n" 
                " resettime\t\t{}\n   resetStateInitTime\t{}\n   resetDiskInitTime\t{}\n   resetGenFailTime\t{}\n   resetHeapTime\t{}\n"
                " getEventTime\t\t{}\n updateClockTime\t{}\n updateStateTime\t{}\n"
                " updateRackStateTime\t{}\n"
                " updatePriorityTime\t{}\n   updateDiskRepairTime\t{}\n   updatePriorityFailTime\t{}\n"
                " updateRackPriTime\t{}\n"
                " newFailTime\t\t{}\n checkLossTime\t\t{}\n updateRepairTime\t{}\n").format(
            self.simInitTime,
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
            self.updateRackStateTime,
            
            self.updatePriorityTime,
            self.updateDiskRepairTime,
            self.updatePriorityFailTime,

            self.updateRackPriorityTime,
            self.newFailTime,
            self.checkLossTime,
            self.updateRepairTime
        )
