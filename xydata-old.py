import time
import random
import urllib
import traceback
import pandas as pd
import numpy as np
from db.models import *


class FormatHelpers(object):
    ''' Helpers to format data, useful for both data from the database and data
        from live API requests
    '''

    # Dataset columns
    lookbackcols = [
        'volume.tradable',
        'closec.tradable',
        'highc.tradable',
        'lowc.tradable',
        'volumec.tradable',
        'open.vix',
        'close.vix',
        'low.vix',
        'high.vix',
        'closec.vix',
        'highc.vix',
        'lowc.vix',
    ]

    @classmethod
    def features(cls, lookback):
        ''' Creates an array of feature descriptions
        '''
        inputscols = ['%s.%s' % (col, i) for col in cls.lookbackcols for i in range(lookback)]
        inputscols += ['weekday', 'hour', 'minute']
        return inputscols

    @classmethod
    def flatteninput(cls, merged, index, lookback):
        ''' Flattens a prices dataframe into one large feature array using the
            given lookback period, and using the lookback columns defined above,
            along with the one-off columns defined above
        '''
        # Get a flattened list of all the lookback values:
        lookbacks = [merged.iloc[index - j] for j in range(lookback)]
        row = [item[col] for col in cls.lookbackcols for item in lookbacks]

        # Add in some extra singleton columns, and append to the overall
        # list of inputs:
        current = merged.iloc[index]
        row += [current[col] for col in ('weekday.tradable', 'hour.tradable', 'minute.tradable')]
        return row

    @classmethod
    def addtimes(cls, prices):
        ''' Formats a prices dataframe by using the 'time' column to add columns
            for date, weekday, hour, and minute
        '''
        mappers = [
            ('date', lambda row: row.time.date()),
            ('weekday', lambda row: row.time.weekday()),
            ('hour', lambda row: row.time.hour),
            ('minute', lambda row: row.time.minute),
        ]
        for key, mapper in mappers:
            prices[key] = prices.apply(mapper, axis=1)

    @classmethod
    def addchanges(cls, prices):
        ''' Formats the given prices dataframe by adding in pct change columns for
            high, low, and close values, as well as a difference column for the
            volume column
        '''

        # First, Create Empty columns for each new Field:
        prices['closec'] = None
        prices['highc'] = None
        prices['lowc'] = None
        prices['volumec'] = None

        # Get Integer Index Values for Each of the Columns Needed:
        columns = list(prices.columns)
        iclosec = columns.index('closec')
        ihighc = columns.index('highc')
        ilowc = columns.index('lowc')
        ivolumec = columns.index('volumec')
        iclose = columns.index('close')
        ihigh = columns.index('high')
        ilow = columns.index('low')
        ivolume = columns.index('volume')

        # Loop through the Session's Values, Updating Pct Change Values Along the Way:
        count, _ = prices.shape
        for i in range(1, count):
            close = prices.iat[i - 1, iclose]
            prices.iat[i, iclosec] = np.log(prices.iat[i, iclose] / close) * 100.
            prices.iat[i, ihighc] = np.log(prices.iat[i, ihigh] / close) * 100.
            prices.iat[i, ilowc] = np.log(prices.iat[i, ilow] / close) * 100.
            prices.iat[i, ivolumec] = prices.iat[i, ivolume] - prices.iat[i - 1, ivolume]

class LiveDatapoint(object):
    ''' This class aims to emulate the feature formatting done within the XYData
        class and the DaySession class, only with the data source coming directly
        from the AlphaVantage API, as opposed to from that which is stored in
        the database. Of interest are the following attributes:
            - time
            - price
            - inputs
    '''
    def __init__(self, symbol, key, lookback):
        self.symbol = symbol
        self._key = key
        self.lookback = lookback
        self.bought = False

        # Download latest data from AlphaVantage:
        start = time.time()
        prices = self.getlatest(self.symbol)
        vix = self.getlatest('VIX')

        # Merge VIX and tradable dataframes:
        merged = pd.merge(prices, vix, on='time', suffixes=('.tradable', '.vix'))

        # Use the centralized dataframe flattener to get a single feature array:
        row = FormatHelpers.flatteninput(merged, index=-1, lookback=self.lookback)

        # Save the timestamp, current price, and input feature array:
        self.time = prices.time[-1].to_pydatetime()
        self.price = prices.close[-1]
        self.inputs = np.array([row])
        self.features = FormatHelpers.features(self.lookback)
        print 'Live Data Fetch Took %.2fs' % (time.time() - start)

    def geturl(self, symbol):
        ''' Generates the full API url for getting live data
        '''
        args = urllib.urlencode({
            'function': 'TIME_SERIES_INTRADAY',
            'interval': '1min',
            'outputsize': 'compact',
            'symbol': symbol,
            'apikey': self._key,
        })
        return 'https://www.alphavantage.co/query?%s' % args

    def getlatest(self, symbol):
        ''' Get's a dataframe of the latest prices, times, and pct changes directly
            from AlphaVantage's API for the given symbol
        '''
        # Get Prices Dataframe from AlphaVantage:
        print 'Fetching Live %s data...' % symbol
        url = self.geturl(symbol)
        response = requests.get(url).json()

        # Convert the response into a pandas dataframe:
        timeseries = response['Time Series (1min)']
        prices = pd.DataFrame(timeseries).transpose()

        # Change column names to match the database format, and typecast as floats:
        prices.columns = ['open', 'high', 'low', 'close', 'volume']
        prices = prices.astype(float)

        # Add a 'time' column:
        format = '%Y-%m-%d %H:%M:%S'
        prices['time'] = [datetime.datetime.strptime(ts, format) for ts in prices.index]

        # Add time-based columns, and change-based dataframe columns:
        FormatHelpers.addtimes(prices)
        FormatHelpers.addchanges(prices)

        return prices

    def __repr__(self):
        return '<live.Datapoint %s>' % self.time

class DaySession(object):
    ''' Class for modeling inputs and outputs of one trading session,
        specifically with the given lookback and forecast periods for input and
        output construction:
            eg.
            day = DaySession(date, pricesdf, vixdf, 30, 10)
            inputs = day.inputs
            outputs = day.outputs
            features = day.features
            ...
        The inputs and outputs members are numpy array objects, which should be
        well-suited for machine learning model construction
    '''
    def __init__(self, date, prices, vix, lookback, forecast):
        self.date = date
        self.lookback = lookback
        self.forecast = forecast

        inputs = []
        outputs = []

        # Merge vix and tradable dataframes:
        merged = pd.merge(prices, vix, on='time', suffixes=('.tradable', '.vix'))
        count, _ = merged.shape
        for i in range(lookback, count - forecast):
            # Get a flattened list of all the lookback values:
            row = FormatHelpers.flatteninput(merged, i, self.lookback)
            inputs.append(row)

            # Get the output value here -- This is the value that we are trying
            # to predict, namely, the pct change in stock price of the
            # forecasting time horizon:
            current = merged.iloc[i]
            future = merged.iloc[i + forecast]
            result = np.log(future['close.tradable'] / current['close.tradable']) * 100.
            outputs.append(result)

        # Get a list of features:
        features = FormatHelpers.features(self.lookback)

        # Save the features, inputs, and outputs:
        self.features = features
        self.inputs = np.array(inputs)
        self.outputs = pd.DataFrame(outputs)

    def __repr__(self):
        return '<%s Session %s.lookback %s.forecast)>' % (
            self.date,
            self.lookback,
            self.forecast
        )

class XYData(object):
    ''' Constructs a data dictionary of date => DailySession objects, so that
        for the given tradable, we have n-minute lookback inputs, and m-minute
        forecast outputs for each day that the data is availble for both the
        given tradable, and for the VIX index.
        This data dictionary should be well-suited for random splitting into
        training and testing data, in the process of constructing a machine
        learning model
    '''
    def __init__(self, symbol, lookback=30, forecast=10):
        self.symbol = symbol
        self.lookback = lookback
        self.forecast = forecast

        self.tradable = session.query(Tradable).filter_by(name=symbol).first()
        self.vix = session.query(Tradable).filter_by(name='VIX').first()

        # Get data:
        self.data = self.getbuckets()
        self.dates = sorted(self.data.keys())

    @property
    def features(self):
        '''
        '''
        if self.dates:
            date = self.dates[0]
            return self.data[date].features
        else:
            raise Exception('No Feature Data Available')

    def getsplit(self, train=0.65):
        ''' Get's an aggregated train/test split of input and output data across
            a randomized selection of dates
            The return format is a 4-tuple of train inputs, train outputs, test
            inputs, and test outputs
        '''
        # Get the count of number of training items:
        count = int(len(self.dates) * train)

        # Aggregate the training data:
        traindates = random.sample(self.dates, count)
        traindays = [self.data[date] for date in traindates]
        intrain = np.concatenate([day.inputs for day in traindays])
        outtrain = np.concatenate([day.outputs for day in traindays])

        # Aggregate the testing data:
        testdates = sorted(set(self.dates) - set(traindates))
        testdays = [self.data[date] for date in testdates]
        intest = np.concatenate([day.inputs for day in testdays])
        outtest = np.concatenate([day.outputs for day in testdays])

        # Return the newly formatted 4-tuple:
        return intrain, np.ravel(outtrain), intest, np.ravel(outtest)

    def getbuckets(self):
        '''
        '''
        print 'Fetching %s, %s Data...' % (self.tradable, self.vix)
        start = time.time()
        prices = self.tradable.getprices()
        vprices = self.vix.getprices()
        print 'Loaded Data in %.2fs' % (time.time() - start,)

        # Add some new columns to the prices dataframes:
        print 'Adding Time-Based Features...'
        FormatHelpers.addtimes(prices)
        FormatHelpers.addtimes(vprices)

        # Bucket price dataframes in dates:
        dates = set(prices.date).intersection(vprices.date)
        print 'Bucketing Data Into %s Daily Sessions, ETC: %1.fm' % (
            len(dates),
            3.5 * len(dates) / 60.
        )
        buckets = {}
        for date in dates:
            try:
                # Splice Dataframe by the given date:
                dayprices = prices[prices.date == date]
                dayvprices = vprices[vprices.date == date]

                # Add in PCT Change Columns to the two Price Dataframes:
                FormatHelpers.addchanges(dayprices)
                FormatHelpers.addchanges(dayvprices)

                buckets[date] = DaySession(
                    date=date,
                    prices=dayprices,
                    vix=dayvprices,
                    lookback=self.lookback,
                    forecast=self.forecast,
                )

            except:
                print traceback.format_exc()

        return buckets

    def live(self):
        ''' Get a LiveDatapoint Based on the Current Instance's Symbol and
            Lookback Parameters. This is Useful for live making Live Predictions
        '''
        return LiveDatapoint(
            symbol=self.symbol,
            key=API_KEY,
            lookback=self.lookback
        )
