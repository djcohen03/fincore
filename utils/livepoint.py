import datetime

class LivePoint(object):
    def __init__(self, inputs, timestamp, features, returns):
        '''
        '''
        self.inputs = inputs
        self.timestamp = timestamp
        self.features = features

        # Raw returns dataset for reference:
        self.returns = returns

    @property
    def timesince(self):
        ''' Time Since
        '''
        onehour = datetime.timedelta(hours=1) # adjust from CT to ET
        delta = datetime.datetime.now() + onehour - self.timestamp
        return delta.total_seconds()
