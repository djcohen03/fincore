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

    def getreturns(self):
        '''
        '''
        # Get price history:
        prices = self.getprices()

        # Compute price returns based on previous close:
        prices['price'] = prices.close.copy()
        prices['prev'] = prices.close.shift(1)
        prices['open'] = (prices.open / prices.prev - 1.) * 100.
        prices['high'] = (prices.high / prices.prev - 1.) * 100.
        prices['close'] = (prices.close / prices.prev - 1.) * 100.
        prices['low'] = (prices.low / prices.prev - 1.) * 100.

        # Define returns dataframe:
        returns = prices[['open', 'high', 'low', 'close', 'time', 'date', 'volume', 'price']].fillna(0.)

        return returns

    def pricerange(self, start, end):
        ''' Get prices in the given date range
        '''
        prices = self.getprices()
        m1 = prices.date <= end
        m2 = prices.date >= start
        prices = prices[m1 & m2]

        return prices

    def pricedates(self):
        ''' Get available price dates
        '''
        query = '''
            SELECT DISTINCT date(time) FROM price
            WHERE request_id IN (
                SELECT price_request.id FROM tradable
                INNER JOIN price_request
                ON price_request.tradable_id = tradable.id
                WHERE tradable.id=%s
            ) ORDER BY date;
        ''' % self.id
        results = pd.read_sql(query, engine)
        return results.date.tolist()

    def getprices(self):
        ''' Get all prices
        '''
        print 'Downloading Prices For %s...' % self.name
        start = time.time()
        query = '''
            SELECT open, high, low, close, time, date(time), volume
            FROM price
            WHERE request_id IN (
                SELECT id FROM price_request WHERE tradable_id=%s
            );
        ''' % self.id
        prices = pd.read_sql(query, engine).sort_values('time')
        print 'Downloaded %s Prices For %s In %.2fs' % (prices.shape[0], self.name, time.time() - start)

        # Separate out date and time columns:
        prices.index = prices.time.copy()
        prices['date'] = prices.time.map(lambda x: x.date())
        prices['time'] = prices.time.map(lambda x: x.time())
        return prices

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
        '''
        '''
        return '<%s|%s|%s>' % (self.request.tradable, self.time, float(self.close))

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

class APIRequest(object):
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
