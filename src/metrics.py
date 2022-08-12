class Metrics:
    def __init__(self):
        self.total_rebuild_io_per_year = 0
        self.total_net_traffic = 0
        self.failure_count = 0
        self.iter_count = 0

    def __add__(self, otherMetrics):
        res = Metrics()
        res.total_rebuild_io_per_year = self.total_rebuild_io_per_year + otherMetrics.total_rebuild_io_per_year
        res.total_net_traffic = self.total_net_traffic + otherMetrics.total_net_traffic
        res.iter_count = self.iter_count + otherMetrics.iter_count
        res.failure_count = self.failure_count + otherMetrics.failure_count
        return res

    def __str__(self):
        return ("avg_rebuild_io_per_year:\t{}\n"
                "avg_net_traffic\t\t{}\n"
                "avg_failure_count:\t\t{}\n"
                "iter_count:\t\t{}\n").format(
            self.total_rebuild_io_per_year / (1024*1024) / self.iter_count,
            self.total_net_traffic / (1024*1024) / self.iter_count,
            self.failure_count / self.iter_count,
            self.iter_count
        )
    
    def getAverageRebuildIO(self):
        return self.total_rebuild_io_per_year / (1024*1024) / self.iter_count