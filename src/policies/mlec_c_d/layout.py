from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

def mlec_c_d_layout(sys: System):
    sys.flat_decluster_rack_layout = {}
    for rackId in sys.rackIds:
        disks_per_rack = sys.disks_per_rack[rackId]
        sys.flat_decluster_rack_layout[rackId] = disks_per_rack