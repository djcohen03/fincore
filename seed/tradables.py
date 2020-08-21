from db.models import *

if __name__ == '__main__':
    tradables = [
        'SPY', 'TSLA', 'EEM','QQQ', 'TWTR', 'LULU', 'X',
        'AAPL', 'MSFT', 'AMZN', 'FB', 'BRK.B', 'JPM', 'GOOG', 'GOOGL', 'JNJ',
        'XOM', 'BAC', 'WFC', 'UNH', 'V', 'PFE', 'T', 'CVX', 'INTC', 'HD', 'VZ',
        'CSCO', 'PG', 'BA', 'MA', 'C', 'MRK', 'KO', 'DIS', 'CMCSA', 'PEP',
        'DWDP', 'NVDA', 'NFLX', 'ABBV', 'ORCL', 'PM', 'AMGN', 'WMT', 'MCD',
        'ADBE', 'MDT', 'IBM', 'MMM', 'UNP', 'HON', 'ABT', 'GE', 'MO', 'TXN',
        'NKE', 'ACN', 'CRM', 'GILD', 'UTX', 'LLY', 'BMY', 'PYPL', 'QCOM',
        'COST', 'TMO', 'BKNG', 'SLB', 'AVGO', 'COP', 'USB', 'UPS', 'CAT', 'GS',
        'LOW', 'NEE', 'LMT', 'AXP', 'BIIB', 'SBUX', 'EOG', 'CVS', 'PNC', 'MS',
        'BDX', 'ANTM', 'AMT', 'CELG', 'CB', 'AET', 'CSX', 'TJX', 'DHR', 'AGN',
        'MDLZ', 'ADP', 'SCHW', 'MU', 'FDX', 'BLK', 'OXY', 'ISRG', 'CL', 'DUK',
        'CHTR', 'WBA', 'RTN', 'CME', 'SPG', 'ATVI', 'SYK', 'GD', 'BK', 'INTU',
        'PSX', 'NOC', 'SPGI', 'VLO', 'AMAT', 'ILMN', 'NSC', 'FOXA', 'GM', 'COF',
        'SO', 'MET', 'BSX', 'AIG', 'EMR', 'D', 'CCI', 'ESRX', 'CI', 'DE', 'PX',
        'ZTS', 'CTSH', 'VRTX', 'HUM', 'TGT', 'MMC', 'ICE', 'ITW', 'PRU', 'EXC',
        'BBT', 'HPQ', 'EA', 'KMB', 'F', 'ECL', 'MPC', 'SHW', 'PGR', 'LYB',
        'HAL', 'ADI', 'AFL', 'MAR', 'STZ', 'WM', 'KHC', 'DAL'
    ]

    for t in tradables:
        if session.query(Tradable).filter_by(name=t).first():
            continue
        session.add(Tradable(name=t))

    session.commit()
