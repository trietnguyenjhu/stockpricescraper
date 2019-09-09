import random
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

import yfinance as yf

from datsup import fileio

import exceptions
import globalconsts


def run(database, tickers, logger):
    database.cursor.fast_executemany = True
    proxyPool = getProxyPool()
    count = 0

    for ticker in tickers:
        # console log
        now = datetime.datetime.now()
        print(f'{now.month:02.0f}/{now.day:02.0f}/{now.year:02.0f} ' +
              f'{now.hour:02.0f}:{now.minute:02.0f}:{now.second:02.0f} - ' +
              f'Downloading {ticker.strip().upper()} - {count}/{len(tickers)}')

        flagIterTicker = True
        iterTickerCount = 0

        while flagIterTicker:
            proxy = {'https': random.choice(proxyPool)}
            try:
                update(database, proxy, ticker)
                flagIterTicker = False  # break out of while loop if download success
            except exceptions.ProxyError as e:
                logger.logError(e)
                iterTickerCount += 1
            time.sleep(random.random()*1) # iter delay

        count += 1
        if count%8 == 0: # batch delay
            time.sleep(random.random()*20)
            proxyPool = getProxyPool() # refresh proxy pool


def getProxyPool():
    """Generate a list of https proxies from https://www.sslproxies.org/"""
    # proxy handling
    page = requests.get('https://www.sslproxies.org/')
    soup = BeautifulSoup(page.text, 'lxml')
    proxyTable = pd.read_html(str(soup.find_all('table', {'id': 'proxylisttable'})))[0]
    proxyPool = []
    for row in (proxyTable[proxyTable['Https']=='yes'].iterrows()):
        proxyPool.append(f"https://{row[1]['IP Address']}:{int(row[1]['Port'])}")
    return proxyPool


def update(database, proxy, ticker):
    """Update data"""
        try:
            data = yf.download(ticker, progress=False, proxy=proxy).reset_index()
        except (requests.exceptions.SSLError, requests.exceptions.ProxyError) as e: # invalid proxy
            raise exceptions.ProxyError(f'Unable to use proxy - {proxy}')
        else:
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
