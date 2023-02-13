from heapq import heappush
from components.disk import Disk
from components.rack import Rack

def mlec_c_d_repair(state, repair_queue):
    
    for rackId in state.get_failed_racks():
        repair_event = state.policy.get_rack_repair_event(rackId)
        if repair_event is not None:
            heappush(repair_queue, repair_event)
                
    #-----------------------------------------------------
    # FIFO reconstruct, utilize the hot spares
    #-----------------------------------------------------
    for diskId in state.get_failed_disks():
        rackId = diskId // state.sys.num_disks_per_rack
        if state.racks[rackId].state == Rack.STATE_NORMAL:
            disk = state.disks[diskId]
            priority = disk.priority
            estimate_time = disk.estimate_repair_time
            if priority > 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_FASTREBUILD, diskId))
                # print("push to repair queue  finish time{} {} {}".format(estimate_time, 
                #           Disk.EVENT_FASTREBUILD, diskId))
            if priority == 1:
                heappush(repair_queue, (estimate_time, Disk.EVENT_REPAIR, diskId))