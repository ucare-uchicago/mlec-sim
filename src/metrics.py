class Metrics:
    def __init__(self):
        self.total_net_traffic_per_year = 0
        self.iter_count = 0

    def __add__(self, otherMetrics):
        res = Metrics()
        res.total_net_traffic_per_year = self.total_net_traffic_per_year + otherMetrics.total_net_traffic_per_year
        res.iter_count = self.iter_count + otherMetrics.iter_count
        return res