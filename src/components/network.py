from __future__ import annotations
import numpy as np
import typing
import logging
import copy
from typing import Dict, Optional

if typing.TYPE_CHECKING:
    from system import System

class Network:
    
    def __init__(self, sys: System, intrarack_bandwidth: float, interrack_bandwidth: float):
        # Define available bandwidth
        self.inter_rack_avail = float(interrack_bandwidth)
        self.intra_rack_avail = []
        
        # Define total bandwidth
        self.inter_rack_total = float(interrack_bandwidth)
        self.intra_rack_total = []
        
        # Populate intra rack
        self.intra_rack_avail = (np.ones(sys.num_racks) * intrarack_bandwidth).tolist()
        self.intra_rack_avail = (np.ones(sys.num_racks) * intrarack_bandwidth).tolist()
        
    def replenish(self, usage: Optional[NetworkUsage]):
        if usage is None:
            return
        
        self.inter_rack_avail += usage.inter_rack
        for rackId in usage.intra_rack.keys():
            self.intra_rack_avail[rackId] += usage.intra_rack[rackId]
        
    def use(self, usage: Optional[NetworkUsage]):
        if usage is None:
            return
        
        logging.info("Original %s, usage %s", self.inter_rack_avail, usage.inter_rack)
        self.inter_rack_avail -= usage.inter_rack
        for rackId in usage.intra_rack.keys():
            self.intra_rack_avail[rackId] -= usage.intra_rack[rackId]
        
    def __str__(self):
        return "<inter: {}| intra {}>".format(self.inter_rack_avail, self.intra_rack_avail)

class NetworkUsage:
    
    def __init__(self, inter_rack: float, intra_rack: Dict[int, float]) -> None:
        self.inter_rack: float = inter_rack
        self.intra_rack: Dict[int, float] = intra_rack
        
    def join(self, usage: Optional[NetworkUsage]):
        if usage is None:
            return
        
        self.inter_rack += usage.inter_rack
        for rackId in usage.intra_rack.keys():
            rack_usage = self.intra_rack.get(rackId, 0) + usage.intra_rack[rackId]
            self.intra_rack[rackId] = rack_usage
            
    def split(self, portion: int) -> NetworkUsage:
        split_intra_rack = {}
        for rackId in self.intra_rack.keys():
            split_intra_rack[rackId] = self.intra_rack[rackId] / portion
            
        return NetworkUsage(self.inter_rack / portion, split_intra_rack)
    
    # The other should be a subset of base
    def subtract(self, other: NetworkUsage) -> NetworkUsage:
        base = copy.deepcopy(self)
        base.inter_rack -= other.inter_rack
        logging.info("Base %s", base)
        logging.info("Other %s", other)
        for rackId in base.intra_rack.keys():
            if base.intra_rack[rackId] >= other.intra_rack.get(rackId, 0):
                base.intra_rack[rackId] -= other.intra_rack.get(rackId, 0)
        return base
    
    def has_intra_rack(self):
        return len(self.intra_rack) != 0
        
    def __str__(self):
        return "<inter: {}| intra {}>".format(self.inter_rack, self.intra_rack)
        
