from __future__ import annotations
import typing
import random

if typing.TYPE_CHECKING:
    from system import System

def lrc_dp_layout(sys: System):
    for diskId in sys.disks:
        sys.disks[diskId].diskId = diskId
        sys.disks[diskId].rackId = diskId // sys.num_disks_per_rack


def net_dp_layout_chunk(num_racks, num_disks_per_rack, num_chunks_per_disk, n_n):
    num_chunks_total = num_racks * num_disks_per_rack * num_chunks_per_disk
    num_stripes_total = num_chunks_total // n_n
    num_disks = num_racks * num_disks_per_rack

    all_racks = range(num_racks)
    stripeid_per_disk = [[] for i in range(num_disks)]
    stripes = []
    for stripeid in range(num_stripes_total):
        #  first pick n_n racks
        stripe = random.sample(all_racks, n_n)
        for i in range(n_n):
            diskid = stripe[i] * num_disks_per_rack + random.randint(0, num_disks_per_rack-1)
            stripe[i] = diskid
            stripeid_per_disk[diskid].append(stripeid)
        stripes.append(stripe)
    return stripeid_per_disk, stripes

if __name__ == "__main__":
    stripeid_per_disks, stripes = net_dp_layout_chunk(40,800,10,20)
    for stripe in stripes:
        print(stripe)
    for stripeid_per_disk in stripeid_per_disks:
        print(stripeid_per_disk)
    


        