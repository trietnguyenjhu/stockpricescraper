import random
import datetime
import requests
from bs4 import BeautifulSoup

import yfinance as yf
import globalconsts



def run(database, tickers):
    database.cursor.fast_executemany = True

    # proxy handling
    page = requests.get('https://www.sslproxies.org/')
    soup = BeautifulSoup(page.text, 'lxml')
    proxyTable = pd.read_html(str(soup.find_all('table', {'id': 'proxylisttable'})))[0]
    proxyPool = []
    for row in (proxyTable[proxyTable['Https']=='yes'].iterrows()):
        proxyPool.append(f"https://{row[1]['IP Address']}:{int(row[1]['Port'])}")

    for ticker in tickers:
        # console log
        now = datetime.datetime.now()
        print(f'{now.month:02.0f}/{now.day:02.0f}/{now.year:02.0f} ' +
              f'{now.hour:02.0f}:{now.minute:02.0f}:{now.second:02.0f} - ' +
              f'Downloading {ticker.strip().upper()} - {count}/{len(tickers)}')

        data = yf.download(ticker, proxy={'https': random.choice(proxyPool)}).reset_index()
        selectQuery = \
            f"""
                select company_id
                from insiderTrading.Company
                where ticker = '{ticker.upper()}'
            """
        insertQuery = \
            """ 
            """ # boiler plate, will not insert if ticker isn't in database
        company_id = database.queryId(selectQuery, insertQuery)

        data['company_id'] = company_id

        baseInsert = \
            f"""
                insert into {globalconsts.SCHEMA}Price
                (company_id, day, openPrice, highPrice, lowPrice, closingPrice, adjClose, volume)
                values (?, ?, ?, ?, ?, ?, ?, ?)
            """
        data = data[[
            'company_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        params = list(data.itertuples(False, None))
        database.cursor.executemany(baseInsert, params)
        database.commit()

        database.runSQL('exec sp_deleteDuplicatePrices', verify=True)
        time.sleep(random.random()*10)
