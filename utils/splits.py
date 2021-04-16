import random
import pandas as pd

class Splits(object):
    def __init__(self, inputs, outputs, features, split=0.65):
        ''' Data Split Abstraction
        '''
        self._inputs = inputs
        self._outputs = outputs
        self.features = features
        self.split = split
        self.count, _ = self._inputs.shape
        self._index = pd.RangeIndex(start=0, stop=self.count)
        self.shuffle()

    def shuffle(self, split=None):
        '''
        '''
        if split:
            self.split = split

        # Get the count of number of training items:
        count = int(self.count * self.split)
        print('Splitting: %s/%s Training/Testing Data Points' % (count, self.count - count))

        # Do Random Splitting:
        trainindex = sorted(random.sample(self._index, count))
        testindex  = sorted(set(self._index) - set(trainindex))

        # Create Train and Test Splits:
        self.train = DataSet('train', self.features, self._inputs[trainindex], self._outputs[trainindex])
        self.test = DataSet('test', self.features, self._inputs[testindex], self._outputs[testindex])


class DataSet(object):
    def __init__(self, name, features, inputs, outputs):
        ''' Single Input/Output DataSet Representation
        '''
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.features = features
        self.count, _ = self.inputs.shape

    def __repr__(self):
        '''
        '''
        return '<%s.DataSet [%s x %s]>' % (self.name.upper(), self.count, len(self.features))
