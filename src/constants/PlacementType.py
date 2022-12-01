from enum import Enum

class PlacementType(str, Enum):
    RAID = 'RAID'
    RAID_NET = 'RAID_NET'
    
    DP = "DP"
    DP_NET = 'DP_NET'
    
    MLEC = "MLEC"
    MLEC_DP = 'MLEC_DP'
    
    
def parse_placement(str: str) -> PlacementType:
    return PlacementType[str]