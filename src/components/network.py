import numpy as np
from system import System

class Network:
    
    def __init__(self, sys: System, intrarack_bandwidth: int, interrack_bandwidth: int):
        # Define available bandwidth
        self.inter_rack_avail = interrack_bandwidth
        self.intra_rack_avail = []
        
        # Define total bandwidth
        self.inter_rack_total = interrack_bandwidth
        self.intra_rack_total = []
        
        # Populate intra rack
        self.intra_rack_avail = (np.ones(sys.num_racks) * interrack_bandwidth).tolist()
        self.intra_rack_avail = (np.ones(sys.num_racks) * interrack_bandwidth).tolist()
        