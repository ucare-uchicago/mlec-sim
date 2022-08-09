class Metrics:
    def __init__(self):
        self.total_net_traffic_per_year = 0
        self.failure_count = 0
        self.iter_count = 0

    def __add__(self, otherMetrics):
        res = Metrics()
        res.total_net_traffic_per_year = self.total_net_traffic_per_year + otherMetrics.total_net_traffic_per_year
        res.iter_count = self.iter_count + otherMetrics.iter_count
        res.failure_count = self.failure_count + otherMetrics.failure_count
        return res

    def __str__(self):
        return ("avg_net_traffic_per_year:\t{}\n"
                "avg_failure_count:\t\t{}\n"
                "failure_count:\t\t{}\n").format(
            self.total_net_traffic_per_year / (1024*1024) / self.iter_count,
            self.failure_count / self.iter_count,
            self.iter_count
        )