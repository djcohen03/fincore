import time
import random
import urllib
import traceback
import pandas as pd
import numpy as np
from db.models import *
from splits import Splits

# Set pandas dataframe column widths:
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', 25)

class XYData(object):
    def __init__(self, symbol, lookback=30, forecast=15):
        '''
        '''
        self.symbol = symbol
        self.lookback = lookback
        self.forecast = forecast

        self.tradable = session.query(Tradable).filter_by(name=symbol).first()
        self.vix = session.query(Tradable).filter_by(name='VIX').first()

        # Download the dataset:
        self.returns = self._loadreturns()
        self.features = list(self.returns.columns[:-1])

        # Split Returns Dataset into X/Y Inputs/Outputs:
        inputs = np.array(self.returns[self.returns.columns[:-1]].reset_index(drop=True))
        outputs = np.array(self.returns['forecast'].reset_index(drop=True))
        self.split = Splits(inputs, outputs, self.features, split=0.65)

    @property
    def train(self):
        ''' Alias For Training Dataset
        '''
        return self.split.train

    @property
    def test(self):
        ''' Alias For Testing Dataset
        '''
        return self.split.test

    def shuffle(self, split=None):
        ''' Alias For DataSet Shuffling Functionality
        '''
        self.split.shuffle(split=split)

    def _loadreturns(self):
        ''' Load in Formatted Lookback/Forecasted Returns Data
        '''
        returns = self.tradable.getreturns()

        # Add weekday, hour, minute dataset columns:
        mappers = [
            ('weekday', lambda row: row.date.weekday()),
            ('hour', lambda row: row.time.hour),
            ('minute', lambda row: row.time.minute),
        ]
        for key, mapper in mappers:
            returns[key] = returns.apply(mapper, axis=1)

        # Add in the high/low range
        returns['range'] = returns.high - returns.low

        # Add in the percent change with all n-period lags based on the
        # self.lookback parameter:
        for periods in range(2, self.lookback):
            returns['change.%s' % periods] = returns.price.pct_change(periods=periods)


        # MARK: If more informative columns are to be added, this is the place
        # where that should be done:
        # ...
        # ..


        # Drop the first N periods in the day based on our allowed lookback window:
        zerodays = datetime.timedelta(days=0)
        daydiffs = returns.date.diff(periods=self.lookback)
        returns = returns[~(daydiffs > zerodays)]

        # Add in the predictive variable:
        returns['forecast'] = returns.price.pct_change(periods=self.forecast).shift(periods=-self.forecast)

        # Drop the last N periods in the day based on our prediction window:
        daydiffs = returns.date.diff(periods=-self.forecast)
        returns = returns[~(daydiffs < zerodays)]

        # Drop NaN values that should apear at head & tail of dataframe:
        returns.dropna(inplace=True)

        # Drop any non-stationary columns:
        returns = returns.drop(['price', 'date', 'time'], axis=1)

        return returns


if __name__ == '__main__':
    data = XYData('AAPL')
