from __future__ import annotations
import numpy as np
import typing
from typing import Dict

if typing.TYPE_CHECKING:
    from system import System

class Network:
    
    EVENT_REPLENISH = "<network bandwidth replenish>"
    
    def __init__(self, sys: System, intrarack_bandwidth: int, interrack_bandwidth: int):
        # Define available bandwidth
        self.inter_rack_avail = float(interrack_bandwidth)
        self.intra_rack_avail = []
        
        # Define total bandwidth
        self.inter_rack_total = float(interrack_bandwidth)
        self.intra_rack_total = []
        
        # Populate intra rack
        self.intra_rack_avail = (np.ones(sys.num_racks) * intrarack_bandwidth).tolist()
        self.intra_rack_avail = (np.ones(sys.num_racks) * intrarack_bandwidth).tolist()
        
        # This is a FIFO queue of network bandwidth in use
        #  each time there is a EVENT_REPLENISH, the first element is popped and the corresponding
        #  network bandwidth is restored
        #  a list of NetworkUsage
        self.network_usage_queue = {}
        
    
    def print_queue(self) -> str:
        to_print = {}
        for diskid in self.network_usage_queue:
            to_print[diskid] = self.network_usage_queue[diskid].__dict__
        
        return str(to_print)

class NetworkUsage:
    
    def __init__(self, inter_rack: float, intra_rack: Dict[int, float]) -> None:
        self.inter_rack: float = inter_rack
        self.intra_rack: Dict[int, float] = intra_rack
        
    def __str__(self):
        return "<inter: {}| intra {}>".format(self.inter_rack, self.intra_rack)
        
