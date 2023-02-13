from .decluster.layout import flat_decluster_layout
from .raid.layout import flat_cluster_layout
from .netdp.layout import net_dp_layout
from .netraid.layout import net_raid_layout
from .mlec.layout import mlec_cluster_layout
from .mlec_c_d.layout import mlec_c_d_layout
from .mlec_d_c.layout import mlec_d_c_layout
from .mlec_d_d.layout import mlec_d_d_layout

from policies.raid.raid import RAID
from policies.netraid.netraid import NetRAID
from policies.decluster.decluster import Decluster
from policies.netdp.netdp import NetDP
from policies.mlec.mlec import MLEC
from policies.mlec_c_d.mlec_c_d import MLECDP


from constants.PlacementType import PlacementType

# Because system config happens before State initialization, cannot merge into Policy class
def config_system_layout(placement: PlacementType, system):
    if placement == PlacementType.RAID:
        flat_cluster_layout(system)
    elif placement == PlacementType.DP:
        flat_decluster_layout(system)
    elif placement == PlacementType.RAID_NET:
        net_raid_layout(system)
    elif placement == PlacementType.DP_NET:
        net_dp_layout(system)
    elif placement == PlacementType.MLEC:
        mlec_cluster_layout(system)
    elif placement == PlacementType.MLEC_DP:
        mlec_c_d_layout(system)
    elif placement == PlacementType.MLEC_D_C:
        mlec_d_c_layout(system)
    elif placement == PlacementType.MLEC_D_D:
        mlec_d_d_layout(system)
    else:
        print("???")
        raise NotImplementedError("Cannot recognize the placement type")
    
def get_policy(placement: PlacementType, state):
    if placement == PlacementType.RAID:
        return RAID(state)
    elif placement == PlacementType.DP:
        return Decluster(state)
    elif placement == PlacementType.MLEC:
        return MLEC(state)
    elif placement == PlacementType.RAID_NET:
        return NetRAID(state)
    elif placement == PlacementType.MLEC_DP:
        return MLECDP(state)
    elif placement == PlacementType.DP_NET:
        return NetDP(state)