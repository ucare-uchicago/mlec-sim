from __future__ import annotations
import numpy as np
import typing
from typing import Dict

if typing.TYPE_CHECKING:
    from system import System

class Network:
    
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


class NetworkUsage:
    
    def __init__(self, inter_rack: float, intra_rack: Dict[int, float]) -> None:
        self.inter_rack: float = inter_rack
        self.intra_rack: Dict[int, float] = intra_rack
        
    def __str__(self):
        return "<inter: {}| intra {}>".format(self.inter_rack, self.intra_rack)
        
