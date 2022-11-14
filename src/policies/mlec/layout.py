from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

# layout for mlec cluster raid
def mlec_cluster_layout(sys: System):
    # In network level, we form top_n diskgroups into a diskgroup_stripeset
    sys.top_n = sys.top_k + sys.top_m
    sys.num_diskgroups = sys.num_disks // sys.n
    sys.num_diskgroup_stripesets = sys.num_diskgroups // sys.top_n
    sys.diskgroup_stripesets = []
    # print(self.rack_stripesets)
    # print(self.stripesets_per_racks)