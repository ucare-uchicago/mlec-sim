class DriveArgs:
    data_shards = 5
    parity_shards = 2
    total_shards = 7
    afr_in_pct = 4
    drive_cap = 20
    rec_speed = 50

    def __init__(self, d_shards, p_shards, afr, drive_cap, rec_speed):
        self.data_shards = d_shards
        self.parity_shards = p_shards
        self.total_shards = d_shards + p_shards
        self.afr_in_pct = afr
        self.drive_cap = drive_cap
        self.rec_speed = rec_speed

    def print(self):
        mystr = "**\n"
        mystr += "Data Shards: {} Parity Shards: {} Total Shards: {} afr: {}%\n".format(self.data_shards, self.parity_shards, self.total_shards, self.afr_in_pct)
        mystr += "**"
        print(mystr)