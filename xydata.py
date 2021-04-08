import time
import random
import urllib
import traceback
import pandas as pd
import numpy as np
from db.models import *
from splits import Splits
from tiingo import TiingoClient
from livepoint import LivePoint

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


class LiveXYData(object):
    def __init__(self, symbol, lookback=30):
        '''
        '''
        self.symbol = symbol
        self.lookback = lookback
        self._token = self._apikey()
        self.client = TiingoClient(self._token)

    def _apikey(self):
        '''
        '''
        try:
            from api_key import TIINGO
            return TIINGO
        except ImportError:
            raise Exception('No Tiingo API Key Provided')

    def livestream(self, buffer=0.0):
        '''
        '''
        while True:
            sleeptime = self._timetonext(buffer=buffer)
            time.sleep(sleeptime)
            try:
                print 'Fetching Live Data...'
                yield self.getlive()
            except Exception as e:
                print 'An Exception Occurred Getting Live Data Point: "%s" (Skipping...)' % e
                print traceback.format_exc()

    def _timetonext(self, buffer=0.0):
        ''' Time to the next minute marker
        '''
        now = datetime.datetime.now()
        seconds = 60 - now.second
        microseconds = (10e5 - now.microsecond) / 10e5
        return seconds + microseconds + buffer

    def getlive(self, attempt=1):
        '''
        '''
        returns = self.client.getlive(self.symbol)

        # Check to see if we got updated prices for the most recent minute. If
        # not, we try again, up to three times:
        if (returns[returns.columns[:4]].iloc[-1] == 0.0).all():
            maxtries = 5
            if attempt < maxtries:
                sleeptime = 2 ** attempt
                attempt += 1
                print 'Warning: Re-Fetching Live Data (Attempt %s, Sleeping %ss)...' % (attempt, sleeptime)
                time.sleep(sleeptime)
                return self.getlive(attempt=attempt)
            else:
                raise Exception('Failed To Get Current Data After %s Tries' % maxtries)

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


        # Drop the first N periods in the day based on our allowed lookback window:
        zerodays = datetime.timedelta(days=0)
        daydiffs = returns.date.diff(periods=self.lookback)
        returns = returns[~(daydiffs > zerodays)]

        # Drop NaN values that should apear at head & tail of dataframe:
        returns.dropna(inplace=True)

        # Drop any non-stationary columns:
        returns = returns.drop(['price', 'date', 'time'], axis=1)

        #
        features = list(returns.columns)
        inputs = np.array(returns.iloc[-1]).reshape((1, len(features)))
        timestamp = returns.index[-1].to_pydatetime()

        return LivePoint(
            inputs=inputs,
            timestamp=timestamp,
            features=features,
            returns=returns,
        )

if __name__ == '__main__':
    data = LiveXYData('AAPL')

    for point in data.livestream():
        print point.inputs
        print point.timesince
