class Mpool:
    #----------------------------------
    # Initialize the MLEC pool
    #----------------------------------
    def __init__(self, mpoolId):
        self.mpoolId = mpoolId
        self.rackgroupId = -1
        self.failed_spools = {}
        self.spoolIds = []
        self.failed_spools_undetected = {}
        self.failed_spools_in_repair = {}
        self.repair_rate: float = 0.0
        self.affected_spools = {}
        