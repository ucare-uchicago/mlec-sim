from enum import Enum

class PlacementType(str, Enum):
    SLEC_LOCAL_CP = 'SLEC_LOCAL_CP'
    SLEC_LOCAL_DP = 'SLEC_LOCAL_DP'
    
    SLEC_NET_CP = "SLEC_NET_CP"
    SLEC_NET_DP = 'SLEC_NET_DP'
    LRC_DP = 'LRC_DP'
    
    MLEC_C_C = "MLEC_C_C"
    MLEC_C_D = 'MLEC_C_D'
    MLEC_D_C = "MLEC_D_C"
    MLEC_D_D = "MLEC_D_D"

    # This is Greedy SODP (G-SODP). We don't implement O-SDOP because it does NOT work for all k and p
    SLEC_LOCAL_SODP = 'SLEC_LOCAL_SODP'
    MLEC_C_SODP = 'MLEC_C_SODP'
    MLEC_D_SODP = 'MLEC_D_SODP'
    
    
def parse_placement(str: str) -> PlacementType:
    return PlacementType[str]