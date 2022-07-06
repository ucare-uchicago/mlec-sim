#---------------------------------------------------------------------
# 3-parameter Weibull distribution based on shape, scale, and location
#---------------------------------------------------------------------
class Weibull:
    def __init__(self, shape, scale, location):
        self.shape = float(shape)
        self.scale = float(scale)
        self.location = float(location)

    #----------------------------------------------
    # draw a random value from weibull distriution
    #----------------------------------------------
    def draw(self):
        return random.weibullvariate(self.scale, self.shape) + self.location

