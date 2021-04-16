import json
import requests
import datetime
import traceback
from time import sleep
from db.models import *


class CreateRequest(object):
    ''' Factory Class for creating Pricing and Technical Requests
    '''
    @classmethod
    def price(cls, tradable):
        ''' Create a pricing Request
        '''
        print("Creating price Request for %s..." % tradable.name)
        pricerequest = PriceRequest(tradable_id=tradable.id)
        session.add(pricerequest)
        return pricerequest

    @classmethod
    def technical(cls, tradable, technical):
        ''' Create a technical request for the given tradable/technical pair
        '''
        print("Creating %s technical Request for '%s'..." % (technical, tradable))

        techrequests = TechnicalRequest(tradable_id=tradable.id, technical_indicator_id=technical.id)
        session.add(techrequests)
        return techrequests

class Deduplicate(object):
    @classmethod
    def prices(cls):
        ''' Deduplicates all Prices for all Tradables in the system
        '''
        tradables = session.query(Tradable).all()
        for tradable in tradables:
            start = time.time()

            # When de-deuplicating, only check the past two weeks:
            _end = datetime.date.today()
            _start = _end - datetime.timedelta(days=14)
            prices = tradable.pricerange(_start, _end)

            times = prices[['id', 'time']]
            seen = set()
            duplicates = set()
            for i in range(len(times)):
                ts = times.iloc[i]['time']
                if ts in seen:
                    id = times.iloc[i]['id']
                    duplicates.add(id)
                else:
                    seen.add(ts)

            # Delete duplicate ids:
            if duplicates:
                session.execute('''
                        DELETE FROM price WHERE id IN (%s);
                    ''' % (
                        ','.join([str(id) for id in duplicates])
                    )
                )
                session.commit()

                print("Deleted %s Duplicates for %s in %.2fs" % (len(duplicates), tradable, time.time() - start))
            else:
                print("No Duplicates Found for %s (%.2fs)" % (tradable, time.time() - start))

    @classmethod
    def technicals(cls):
        ''' Deduplicates all Technical Indicators in the system
        '''
        tradables = session.query(Tradable).all()
        technicals = session.query(TechnicalIndicator).all()

        for tradable in tradables:
            for technical in technicals:
                print("Deduplicating %s %s..." % (tradable.name, str(technical)))

                # Collect the Indicator values for this Tradable/Techincal Pair:
                technical_requests = session.query(TechnicalRequest).filter_by(tradable_id=tradable.id, technical_indicator_id=technical.id).all()
                indicators = []
                for request in technical_requests:
                    indicators += request.values

                count = 0
                dates = {}
                for indicator in indicators:
                    if indicator.date in dates:
                        # Make sure this is indeed a duplicate:
                        oldval = json.loads(dates[indicator.date])
                        newval = json.loads(indicator.values)
                        if oldval == newval:
                            count += 1
                            session.delete(indicator)
                        else:
                            print('WARNING: %s != %s (%s)' % (oldval, newval, indicator.date))
                    else:
                        dates[indicator.date] = indicator.values
                session.commit()

                if count:
                    print("Found %s duplicates" % count)


class FetchData(object):

    @classmethod
    def create(cls):
        ''' Creates and Returns a list of Price & Technical Requests
            For now, we only generate technical requests for the SPY tradable
        '''
        pending = []

        # First Get All Tradables and Techincal Indiators:
        tradables = session.query(Tradable).all()
        technicals = session.query(TechnicalIndicator).all()

        # Create & Collect Tradable Requests:
        for tradable in tradables:
            request = CreateRequest.price(tradable=tradable)
            pending.append(request)

        # Create & Collect Technical Requests, Just for SPY for Now:
        spy = session.query(Tradable).filter_by(name='SPY').first()
        if spy:
            for technical in technicals:
                request = CreateRequest.technical(tradable=spy, technical=technical)
                pending.append(request)

        # Save all additions:
        session.commit()
        return pending

    @classmethod
    def send(cls, requests):
        ''' Send the given set of Price and Techincal Indicator Requests
        '''

        # Determine a cutoff for data history:
        cutoff = datetime.date(2018, 8, 1)

        # Loop through requests and send each:
        for request in requests:
            print('Sending %s...' % request)
            try:
                request.send(cutoff=cutoff)
                session.commit()

                prices = getattr(request, 'prices', [])
                if prices:
                    print('Found %s Prices for %s' % (len(prices), request))

                values = getattr(request, 'values', [])
                if values:
                    print('Found %s Values for %s' % (len(values), request))

            except:
                print("Exception occured for %s:" % request)
                print(traceback.format_exc())

            # Alpha Vantage limits the requests to 4 per minute for the free
            # account, so here we sleep
            print("Sleeping 15s...")
            sleep(15.1)





if __name__ == '__main__':
    pending = FetchData.create()
    FetchData.send(pending)
    Deduplicate.prices()
    Deduplicate.technicals()
