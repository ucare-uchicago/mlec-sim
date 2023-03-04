from __future__ import annotations
import typing
import logging

if typing.TYPE_CHECKING:
    from system import System

def net_raid_layout(sys: System):
    stripe_width = sys.top_k + sys.top_m
    # Non-overlapping stripe sets (each member is a rack)
    num_rack_group = sys.num_racks // stripe_width
    # How many spools in total can we have, non-overlapping disks
    num_spools = sys.num_disks_per_rack * num_rack_group
    
    # logging.info("Stripe width: %s, num_racks: %s, num_rack_group: %s, num_spools: %s", stripe_width, sys.num_racks, num_rack_group, num_spools)
    
    sets = {}
    for i in range(num_spools):
        
        num_spools_per_rack_group = sys.num_disks_per_rack
        rackGroupId = i // num_spools_per_rack_group
        spool = []
        for rackId in range(rackGroupId * stripe_width, (rackGroupId + 1) * stripe_width):
            diskId = rackId * num_spools_per_rack_group + i % num_spools_per_rack_group
            disk = sys.disks[diskId]
            disk.rackId = rackId
            disk.spoolId = i
            spool.append(diskId)
            # logging.info(" spoolId: {} diskId: {}".format(i, diskId))
        sets[i] = spool
    # dictionary of stripeId to list of disks on disjoint racks
    sys.net_raid_spools_layout = sets
    # logging.info("* there are {} spools:\n{}".format(
    #         num_spools, sets))