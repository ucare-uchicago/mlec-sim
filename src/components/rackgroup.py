# Rackgroup. Used for top-level CP including netraid, mlec_c_c, mlec_c_d
# A rack group contains top_n racks.

class Rackgroup:
    #----------------------------------
    # Initialize the Rackgroup
    #----------------------------------
    def __init__(self, rackgroupId):
        self.rackgroupId = rackgroupId
        self.mpoolIds = []
        self.spoolIds = []
        self.affected_mpools = {}