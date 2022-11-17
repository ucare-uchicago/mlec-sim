from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from system import System

def net_raid_layout(sys: System):
    stripe_width = sys.top_k + sys.top_m
    # Non-overlapping stripe sets (each member is a rack)
    num_rack_group = sys.num_racks // stripe_width
    # How many stripesets in total can we have, non-overlapping disks
    num_stripesets = sys.num_disks_per_rack * num_rack_group
    
    sets = {}
    for i in range(num_stripesets):
        
        num_stripesets_per_rack_group = sys.num_disks_per_rack
        rackGroupId = i // num_stripesets_per_rack_group
        stripeset = []
        for rackId in range(rackGroupId * stripe_width, (rackGroupId + 1) * stripe_width):
            diskId = rackId * num_stripesets_per_rack_group + i % num_stripesets_per_rack_group
            disk = sys.disks[diskId]
            disk.rackId = rackId
            disk.stripesetId = i
            stripeset.append(diskId)
            # logging.info(" stripesetId: {} diskId: {}".format(i, diskId))
        sets[i] = stripeset
    # dictionary of stripeId to list of disks on disjoint racks
    sys.net_raid_stripesets_layout = sets
    # logging.info("* there are {} stripesets:\n{}".format(
    #         num_stripesets, sets))