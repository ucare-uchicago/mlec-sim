from .slec_local_dp.layout import slec_local_dp_layout
from .slec_local_cp.layout import slec_local_cp_layout
from .netdp.layout import net_dp_layout
from .slec_net_cp.layout import slec_net_cp_layout
from .mlec_c_c.layout import mlec_c_c_layout
from .mlec_c_d.layout import mlec_c_d_layout
from .mlec_d_c.layout import mlec_d_c_layout
from .mlec_d_d.layout import mlec_d_d_layout

from policies.slec_local_cp.slec_local_cp_rs0 import SLEC_LOCAL_CP_RS0
from policies.slec_local_cp.slec_local_cp_rs1 import SLEC_LOCAL_CP_RS1
from policies.slec_net_cp.slec_net_cp_rs0 import SLEC_NET_CP_RS0
from policies.slec_net_cp.slec_net_cp_rs1 import SLEC_NET_CP_RS1
from policies.slec_local_dp.slec_local_dp import SLEC_LOCAL_DP
from policies.netdp.netdp import NetDP
from policies.mlec_c_c.mlec_c_c_rs0 import MLEC_C_C_RS0
from policies.mlec_c_c.mlec_c_c_rs1 import MLEC_C_C_RS1
from policies.mlec_c_c.mlec_c_c_rs2 import MLEC_C_C_RS2
from policies.mlec_c_c.mlec_c_c_rs3 import MLEC_C_C_RS3
from policies.mlec_c_d.mlec_c_d import MLEC_C_D


from constants.PlacementType import PlacementType

# Because system config happens before State initialization, cannot merge into Policy class
def config_system_layout(placement: PlacementType, system):
    if placement == PlacementType.SLEC_LOCAL_CP:
        slec_local_cp_layout(system)
    elif placement == PlacementType.SLEC_LOCAL_DP:
        slec_local_dp_layout(system)
    elif placement == PlacementType.SLEC_NET_CP:
        slec_net_cp_layout(system)
    elif placement == PlacementType.DP_NET:
        net_dp_layout(system)
    elif placement == PlacementType.MLEC_C_C:
        mlec_c_c_layout(system)
    elif placement == PlacementType.MLEC_C_D:
        mlec_c_d_layout(system)
    elif placement == PlacementType.MLEC_D_C:
        mlec_d_c_layout(system)
    elif placement == PlacementType.MLEC_D_D:
        mlec_d_d_layout(system)
    else:
        print("???")
        raise NotImplementedError("Cannot recognize the placement type")
    
def get_policy(placement: PlacementType, state):
    if placement == PlacementType.SLEC_LOCAL_CP:
        if state.sys.repair_scheme == 0:
            return SLEC_LOCAL_CP_RS0(state)
        elif state.sys.repair_scheme == 1:
            return SLEC_LOCAL_CP_RS1(state)
    elif placement == PlacementType.SLEC_LOCAL_DP:
        return SLEC_LOCAL_DP(state)
    elif placement == PlacementType.MLEC_C_C:
        if state.sys.repair_scheme == 0:
            return MLEC_C_C_RS0(state)
        elif state.sys.repair_scheme == 1:
            return MLEC_C_C_RS1(state)
        elif state.sys.repair_scheme == 2:
            return MLEC_C_C_RS2(state)
        elif state.sys.repair_scheme == 3:
            return MLEC_C_C_RS3(state)
    elif placement == PlacementType.SLEC_NET_CP:
        if state.sys.repair_scheme == 0:
            return SLEC_NET_CP_RS0(state)
        elif state.sys.repair_scheme == 1:
            return SLEC_NET_CP_RS1(state)
    elif placement == PlacementType.MLEC_C_D:
        return MLEC_C_D(state)
    elif placement == PlacementType.DP_NET:
        return NetDP(state)