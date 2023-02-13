from enum import Enum

class PlacementType(str, Enum):
    RAID = 'RAID'
    RAID_NET = 'RAID_NET'
    
    DP = "DP"
    DP_NET = 'DP_NET'
    
    MLEC = "MLEC"
    MLEC_C_D = 'MLEC_C_D'
    MLEC_D_C = "MLEC_D_C"
    MLEC_D_D = "MLEC_D_D"
    
    
def parse_placement(str: str) -> PlacementType:
    return PlacementType[str]