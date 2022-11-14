from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

def net_dp_layout(sys: System):
    # Same as flat decluster
    sys.flat_decluster_rack_layout = {}
    for rackId in sys.racks:
        disks_per_rack = sys.disks_per_rack[rackId]
        sys.flat_decluster_rack_layout[rackId] = disks_per_rack

    for diskId in sys.disks:
        sys.disks[diskId].diskId = diskId
        sys.disks[diskId].rackId = diskId // sys.num_disks_per_rack