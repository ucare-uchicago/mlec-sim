from typing import List

class RepairResult:
    
    def __init__(self, can_repair: bool, repair_speed: float, paused_bottom_disks: List[int] = []) -> None:
        self.can_repair = can_repair
        self.repair_speed = repair_speed
        
        # MLEC stuff
        self.paused_bottom_disks = paused_bottom_disks
        self.split_disk_io = -1