import urllib
import StringIO
import requests
import datetime
import pandas as pd

# Set pandas dataframe column widths:
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', 25)

class TiingoClient(object):
    def __init__(self, token):
        '''
        '''
        self._token = token
        self.headers = {'Content-Type': 'application/json'}

    def getlive(self, symbol):
        ''' Get Live Stock Price Data
        '''
        #
        today = str(datetime.date.today())
        params = urllib.urlencode({
            'startDate': today,
            'resampleFreq': '1min',
            'columns': 'open,high,low,close,volume',
            'forceFill': 'true',
            'token': self._token,
            'format': 'csv'
        })
        url = 'https://api.tiingo.com/iex/%s/prices?%s' % (symbol.lower(), params)

        # Do API Query:
        response = requests.get(url, headers=self.headers)

        # Convert the CSV-formatted response into a pandas dataframe:
        prices = pd.read_csv(StringIO.StringIO(response.text))
        prices['timestamp'] = prices.date.map(lambda x: datetime.datetime.strptime(x[:-7], '%Y-%m-%d %H:%M:%S'))
        prices.index = prices.timestamp
        prices.sort_index(inplace=True)

        # Filter out future data points:
        now = datetime.datetime.now() + datetime.timedelta(hours=1)
        infuture = prices.timestamp.map(lambda x: x > now)
        prices = prices[~infuture]

        # Convert to a returns dataframe:
        prices['date'] = prices.timestamp.map(lambda x: x.date())
        prices['time'] = prices.timestamp.map(lambda x: x.time())
        prices['price'] = prices.close.copy()
        prices['prev'] = prices.close.shift(1)
        prices['open'] = (prices.open / prices.prev - 1.) * 100.
        prices['high'] = (prices.high / prices.prev - 1.) * 100.
        prices['close'] = (prices.close / prices.prev - 1.) * 100.
        prices['low'] = (prices.low / prices.prev - 1.) * 100.

        # Define returns dataframe:
        returns = prices[['open', 'high', 'low', 'close', 'time', 'date', 'volume', 'price']].fillna(0.)

        return returns



if __name__ == '__main__':
    client = TiingoClient('d5b3bc41258b712303dcce5b484bee731facfdb6')
    print client.getlive('AAPL')
