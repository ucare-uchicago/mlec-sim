from metrics import Metrics

class SimulationResult:
    
    def __init__(self, failed_iter: int, total_iter: int, metrics: Metrics) -> None:
        self.failed_iter = failed_iter
        self.total_iter = total_iter
        self.metrics = metrics