from system import System

def mlec_dp_layout(sys: System):
    sys.flat_decluster_rack_layout = {}
    for rackId in sys.racks:
        disks_per_rack = sys.disks_per_rack[rackId]
        sys.flat_decluster_rack_layout[rackId] = disks_per_rack