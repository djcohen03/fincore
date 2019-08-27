import time
import random
import traceback
import pandas as pd
import numpy as np
from db.models import *


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

        # Lookback Columns
        lookbackcols = [
            'open.tradable',
            'close.tradable',
            'low.tradable',
            'high.tradable',
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
        # Non-Lookback Columns:
        columns = [
            'weekday.tradable',
            'hour.tradable',
            'minute.tradable',
        ]

        inputs = []
        outputs = []

        # Merge vix and tradable dataframes:
        merged = pd.merge(prices, vix, on='time', suffixes=('.tradable', '.vix'))
        count, _ = merged.shape
        for i in range(lookback, count - forecast):
            # Get a flattened list of all the lookback values:
            lookbacks = [merged.iloc[i - j] for j in range(lookback)]
            row = [item[col] for col in lookbackcols for item in lookbacks]

            # Add in some extra singleton columns, and append to the overall
            # list of inputs:
            current = merged.iloc[i]
            future = merged.iloc[i + forecast]
            row += [current[col] for col in columns]
            inputs.append(row)

            # Get the output value here -- This is the value that we are trying
            # to predict, namely, the pct change in stock price of the
            # forecasting time horizon:
            result = np.log(future['close.tradable'] / current['close.tradable']) * 100.
            outputs.append(result)

        # Get a list of all input columns:
        inputscols = ['%s.%s' % (col, i) for col in lookbackcols for i in range(lookback)]
        inputscols += ['weekday', 'hour', 'minute']

        # # These DataFrames can be helpful for validating that the data is indeed
        # # what we Are expecting:
        # self.inputs = pd.DataFrame(inputs, columns=inputscols)
        # self.outputs = pd.DataFrame(outputs, columns=['forecast.%s' % forecast])

        # Save the features, inputs, and outputs:
        self.features = inputscols
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
        prices = self.tradable.prices()
        vprices = self.vix.prices()
        print 'Loaded Data in %.2fs' % (time.time() - start,)

        # Add some new columns to the prices dataframes:
        mappers = [
            ('date', lambda row: row.time.date()),
            ('weekday', lambda row: row.time.weekday()),
            ('hour', lambda row: row.time.hour),
            ('minute', lambda row: row.time.minute),
        ]
        for key, mapper in mappers:
            print 'Setting the "%s" Column...' % key
            prices[key] = prices.apply(mapper, axis=1)
            vprices[key] = vprices.apply(mapper, axis=1)


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
                self.addchanges(dayprices)
                self.addchanges(dayvprices)

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

    def addchanges(self, prices):
        ''' Adds the following columns to a session: close pct change, high pct
            change, and low pct change (does so in-place)
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
