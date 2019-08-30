import json
import time
import requests
import datetime
import pandas as pd
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, \
        Date, Numeric, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from dateutil.relativedelta import relativedelta
from base import Base
from session import session, engine

# Try to import the API Key:
try:
    from api_key import API_KEY
except ImportError:
    API_KEY = None
    print 'Warning: No AlphaVantage API Key Provided, Data Fetching Disabled...'


class Tradable(Base):
    ''' Class to Represent a Stock Market Equity
    '''
    __tablename__ = 'tradable'
    id = Column(Integer, primary_key=True)

    name = Column(String, unique=True)

    price_requests = relationship('PriceRequest')
    technical_requests = relationship('TechnicalRequest')

    def data(self):
        ''' Load a pandas DataFrame that contains a time series of feature data
            for the given tradable
        '''
        start = time.time()
        data = self.returns()

        # Add in some Historical Volatility Features:
        periods = [30, 60]
        vols = { p: [None] * len(data) for p in periods }
        for i in range(len(data)):
            for p in periods:
                if p <= i:
                    window = data[(i - p): i]
                    if not (window.time.iloc[-1] - window.time.iloc[0]).days:
                        vols[p][i] = window.close.std()
        for p in periods:
            key = "%sm Vol" % p
            data[key] = vols[p]

        # We should remove the rows with None values, for now, for data analysis:
        data = data.dropna()

        # Load in Daily Techincal Indicator Data:
        technicals = self.technicals()
        technicaldata = {key: [None] * len(data) for key in technicals.columns}
        for i, ts in enumerate(data.time):
            date = ts.date()
            if date in technicals.index:
                row = technicals.loc[date]
                for technical in technicaldata.keys():
                    technicaldata[technical][i] = row[technical]
        for technical, values in technicaldata.iteritems():
            data[technical] = values

        print "Loaded All Return and Feature Time Series Data For %s in %.2fs" % (self, time.time() - start)
        return data

    def returns(self, date=None):
        prices = self.prices(date=date)
        returns = []
        for i in range(1, len(prices)):
            close = prices.iloc[i - 1]['close']
            row = prices.iloc[i]
            returns.append({
                'open': row.open / close - 1.,
                'close': row.close / close - 1.,
                'high': row.high / close - 1.,
                'low': row.low / close - 1.,
                'time': row.time
            })
        return pd.DataFrame(returns)

    def prices(self, date=None):
        ''' Get a Pandas DataFrame of this tradable's price history
            Optionally filter by date
        '''
        if date:
            query = '''
                SELECT * FROM price WHERE request_id IN (
                    SELECT id FROM price_request WHERE tradable_id=%s
                ) AND (
                    price.time >= '%s' AND price.time < '%s'
                );
            ''' % (
                self.id,
                date.strftime('%Y-%m-%d 00:00:00.000000'),
                date.strftime('%Y-%m-%d 23:59:59.999999')
            )
        else:
            query = '''
                SELECT * FROM price WHERE request_id IN (
                    SELECT id FROM price_request WHERE tradable_id=%s
                );
            ''' % self.id

        prices = pd.read_sql(query, engine).sort_values('time')
        prices['time'] = pd.to_datetime(prices['time'])
        return prices


    def technicals(self):
        ''' Get a pandas DataFrame of daily techincal indicator values
        '''
        query = '''
            SELECT * FROM technical_indicator_value WHERE request_id IN (
                SELECT id FROM technical_request where tradable_id=%s
            );
        ''' % self.id
        rawdata = pd.read_sql(query, engine)

        data = {date: {} for date in set(rawdata.date)}
        for i in range(len(rawdata)):
            row = rawdata.iloc[i]
            for key, value in json.loads(row['values']).iteritems():
                # RMK: This overwrites other n-day metrics, since they are
                # all stored as 'SMA', as opposed to, eg. SMA-5
                data[row.date][key] = value

        return pd.DataFrame(data).transpose()

    def __repr__(self):
        return self.name


class Price(Base):
    __tablename__ = 'price'
    id = Column(Integer, primary_key=True)

    open = Column(Numeric)
    close = Column(Numeric, nullable=False)
    low = Column(Numeric)
    high = Column(Numeric)
    volume = Column(Integer)
    time = Column(DateTime, nullable=False)

    request_id = Column(Integer, ForeignKey('price_request.id'))
    request = relationship('PriceRequest')

    def __repr__(self):
        return "%s @ %s: %s" % (
            self.request.tradable.name,
            self.time,
            float(self.close)
        )

class TechnicalIndicator(Base):
    __tablename__ = 'technical_indicator'
    id = Column(Integer, primary_key=True)

    args = Column(String, nullable=False)
    description = Column(String)

    def serialized(self):
        return json.loads(self.args)

    def get_args(self):
        args = json.loads(self.args)
        arg_list = []
        for key in args.keys():
            arg_list.append("%s=%s" % (key, args[key]))
        return "&".join(arg_list)

    @property
    def name(self):
        return self.serialized().get('function')

    @property
    def time_period(self):
        return self.serialized().get('time_period')

    def __repr__(self):
        time_period = self.time_period
        if time_period:
            return "%s (%s-day)" % (self.name, time_period)
        else:
            return self.name

class TechnicalIndicatorValue(Base):
    __tablename__ = 'technical_indicator_value'
    id = Column(Integer, primary_key=True)

    values = Column(String, nullable=False)
    date = Column(Date, nullable=False)

    request_id = Column(Integer, ForeignKey('technical_request.id'))
    request = relationship('TechnicalRequest')

    def serialized(self):
        return {
            'name': str(self.request.technical_indicator),
            'values': json.loads(self.values)
        }

    def __repr__(self):
        return "%s %s @ %s: %s" % (
            self.request.tradable.name,
            str(self.request.technical_indicator),
            self.date,
            self.values
        )


class APIRequest():
    sent = Column(Boolean, default=False)
    time_sent = Column(DateTime)
    meta = Column(String)
    successful = Column(Boolean)

    def _send(self):
        if self.sent:
            return
        else:
            self.sent = True
            self.time_sent = datetime.datetime.now()
            session.commit()
            return requests.get(self.url).json()


class PriceRequest(Base, APIRequest):
    __tablename__ = 'price_request'
    id = Column(Integer, primary_key=True)

    prices = relationship('Price')

    # Price Requests need only a tradable
    tradable = relationship('Tradable')
    tradable_id = Column(Integer, ForeignKey('tradable.id'), nullable=False)

    @property
    def url(self, min=1):
        ''' Gets the API url for intraday (1m) prices for this request
        '''
        args = (self.tradable.name, min, API_KEY)
        return 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=%smin&outputsize=full&apikey=%s' % args

    def send(self, cutoff=None):
        ''' Sends this PriceRequest to the AlphaVantage API
        '''
        if self.sent:
            # Only send a request once
            print("Request %s already sent" % self.id)
            return

        # Send request:
        print("Sending Price Request %s" % self)
        result = self._send()

        # Read in result:
        if result.get('Information'):
            # An Error Occurred, Request Unscuccessful
            print("Price Request %s Unsuccessful: %s" % (self.id, result['Information']))
            self.meta = result['Information']
            self.successful = False
            session.commit()
            return

        # Save the Request Meta Data:
        self.meta = json.dumps(result.get('Meta Data'))
        self.successful = True
        session.commit()

        # Read in the data:
        self.readin_data(result.get('Time Series (1min)'))

    def readin_data(self, data):
        # Loop through data points
        prices = []
        for time in data.keys():
            # Create new Price record
            open = data[time].get('1. open')
            high = data[time].get('2. high')
            low = data[time].get('3. low')
            close = data[time].get('4. close')
            volume = data[time].get('5. volume')
            prices.append(Price(
                request_id=self.id,
                time=datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S'),
                open=float(open) if open else None,
                high=float(high) if high else None,
                low=float(low) if low else None,
                volume=int(volume) if volume else None,
                close=float(close),
            ))

        # Try bulk insert into database
        try:
            session.bulk_save_objects(prices)
        except:
            print("Couldn't save price request %s data:" % self.id)
            print(traceback.format_exc())
            session.rollback()

            # Mark as unsuccessful
            self.successful = False
            session.commit()

    def __repr__(self):
        return '<PriceRequest %s: %s>' % (self.id, self.tradable.name)



class TechnicalRequest(Base, APIRequest):
    __tablename__ = 'technical_request'
    id = Column(Integer, primary_key=True)

    values = relationship('TechnicalIndicatorValue')

    # Technical Indicator Requests need a tradable and a technical indicator:
    tradable = relationship('Tradable')
    tradable_id = Column(Integer, ForeignKey('tradable.id'), nullable=False)
    technical_indicator_id = Column(Integer, ForeignKey('technical_indicator.id'), nullable=True)
    technical_indicator = relationship('TechnicalIndicator')

    def last_successful_request(self):
        return session.query(TechnicalRequest) \
            .filter_by(tradable_id=self.tradable_id) \
            .filter_by(technical_indicator_id=self.technical_indicator_id) \
            .filter_by(sent=True) \
            .filter_by(successful=True) \
            .order_by(TechnicalRequest.time_sent.desc()) \
            .first()

    @property
    def url(self):
        indicator = self.technical_indicator
        args = indicator.get_args()
        return 'https://www.alphavantage.co/query?symbol=%s&apikey=%s&%s' % (
            self.tradable.name,
            API_KEY,
            args
        )

    def send(self, cutoff=None):
        if self.sent:
            # Only send a request once
            print("Request %s already sent" % self.id)
            return

        # Send request:
        print("Sending Technical Request %s..." % self)
        result = self._send()

        # Read in result:
        if result.get('Information'):
            # An Error Occurred, Request Unscuccessful
            print("Technical Request %s Unsuccessful: %s" % (self.id, result['Information']))
            self.meta = result['Information']
            self.successful = False
            session.commit()
            return
        elif result.get('Error Message'):
            # An Error Occurred, Request Unscuccessful
            print("Technical Request %s Unsuccessful: %s" % (self.id, result['Error Message']))
            self.meta = result['Error Message']
            self.successful = False
            session.commit()
            return

        # Request Seems to have been successful
        self.meta = json.dumps(result.get('Meta Data'))
        self.successful = True
        session.commit()

        # Read in the data:
        fn_name = self.technical_indicator.serialized().get('function')
        self.readin_data(result.get('Technical Analysis: ' + fn_name), cutoff=cutoff)

    def readin_data(self, data, cutoff=None):
        # Loop through data points
        values = []
        for timestamp, rawdata in data.iteritems():
            try:
                # We sometimes get a weird ' hh:mm:ss' appended to the string, so
                # here we make sure that that is removed from the parsed date string:
                timestamp = timestamp.split(' ')[0]
                date = datetime.datetime.strptime(timestamp, '%Y-%m-%d').date()
                if cutoff and date < cutoff:
                    continue
                value = TechnicalIndicatorValue(request_id=self.id, date=date, values=json.dumps(rawdata))
                values.append(value)

            except Exception as e:
                raise e
                print("Invalid Data Point, Skipping (%s: %s)" % (timestamp, rawdata))
                continue


        # Try bulk insert into database
        try:
            session.bulk_save_objects(values)
            session.commit()
        except:
            print("Couldn't save technical request %s data:" % self.id)
            print(traceback.format_exc())
            session.rollback()

            # Mark as unsuccessful
            self.successful = False
            session.commit()

    def __repr__(self):
        return '<TechnicalRequest %s: %s - %s>' % (self.id, self.tradable.name, self.technical_indicator)


if __name__ == '__main__':

    spy = session.query(Tradable).first()
    technical = session.query(TechnicalIndicator).first()
    request = TechnicalRequest(tradable=spy, technical_indicator=technical)
    session.add(request)
    session.commit()

    request.send(cutoff=datetime.date(2018, 8, 1))
    session.commit()
