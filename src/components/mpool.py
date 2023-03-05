class Mpool:
    #----------------------------------
    # Initialize the MLEC pool
    #----------------------------------
    def __init__(self, mpoolId):
        self.mpoolId = mpoolId
        self.rackgroupId = -1
        self.failed_spools = {}
        self.spoolIds = []
        