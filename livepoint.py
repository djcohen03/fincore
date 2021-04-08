import datetime

class LivePoint(object):
    def __init__(self, inputs, timestamp, features):
        '''
        '''
        self.inputs = inputs
        self.timestamp = timestamp
        self.features = features

    @property
    def timesince(self):
        ''' Time Since
        '''
        delta = datetime.datetime.now() - self.timestamp
        return delta.total_seconds()
