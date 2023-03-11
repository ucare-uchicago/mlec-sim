from enum import Enum

class PlacementType(str, Enum):
    SLEC_LOCAL_CP = 'SLEC_LOCAL_CP'
    SLEC_LOCAL_DP = 'SLEC_LOCAL_DP'
    
    DP = "DP"
    DP_NET = 'DP_NET'
    
    MLEC_C_C = "MLEC_C_C"
    MLEC_C_D = 'MLEC_C_D'
    MLEC_D_C = "MLEC_D_C"
    MLEC_D_D = "MLEC_D_D"
    
    
def parse_placement(str: str) -> PlacementType:
    return PlacementType[str]