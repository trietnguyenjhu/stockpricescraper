import random
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import yfinance as yf
import pyodbc
import urllib3
import numpy as np

from datsup import fileio
from datsup import nanhandler
from datsup import datahandling
from datsup import log

import exceptions
import globalconsts


def run(database, tickers, logger):
    database.cursor.fast_executemany = True
    proxyPool = getProxyPool()

    random.shuffle(tickers)
    batchCount = 0
    batchSize = 100

    # split list of tickers in batches of 100s
    for tickerSubset in datahandling.splitIterableEvenly(tickers, batchSize):
        
        log.timestampPrintToConsole(
            f'Downloading batch {batchCount}/{int(len(tickers)/batchSize)+1}')
        tickerSubset = ' '.join(tickerSubset)
        flagIterTicker = True
        iterTickerCount = 0

        while flagIterTicker:
            # renew proxy if all popped from last iter
            if len(proxyPool) == 0: proxyPool = getProxyPool()
            proxyPath = random.choice(proxyPool)
            proxy = {'https': proxyPath}

            try:
                update(database, proxy, tickerSubset)
            except exceptions.ProxyError as e:
                proxyPool.pop(proxyPool.index(proxyPath)) # remove faulty proxy from pool
                logger.logError(e)
                # iterTickerCount += 1 # 10 retries
                # if iterTickerCount >=10: flagIterTicker = False
                if len(proxyPool) == 0: flagIterTicker = False
                continue
            else:
                flagIterTicker = False  # break out of while loop if download success
            
            batchCount += 1


def getProxyPool():
    """Generate a list of https proxies from https://www.sslproxies.org/"""
    # proxy handling
    page = requests.get('https://www.us-proxy.org/')
    soup = BeautifulSoup(page.text, 'lxml')
    proxyTable = pd.read_html(str(soup.find_all('table', {'id': 'proxylisttable'})))[0]
    proxyPool = []
    for row in (proxyTable[
        (proxyTable['Https']=='yes') & ((proxyTable['Anonymity']=='elite proxy') | (proxyTable['Anonymity']=='anonymous'))
        ].iterrows()):
        proxyPool.append(f"https://{row[1]['IP Address']}:{int(row[1]['Port'])}")
    return proxyPool


def update(database, proxy, tickers):
    """Update data"""
    try:
        bulkData = yf.download(tickers, progress=True, proxy=proxy)
    except (
        requests.exceptions.SSLError,
        requests.exceptions.ProxyError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ChunkedEncodingError,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        TimeoutError,
        ) as e: # invalid proxy
        raise exceptions.ProxyError(f'Unable to use proxy - {proxy}')
    else:
        bulkData = bulkData.swaplevel(axis=1)
        postDownloadTickers = np.unique([x[0] for x in bulkData.columns])
        
        count = 0
        for ticker in postDownloadTickers:

            now = datetime.datetime.now()
            print(f'{now.month:02.0f}/{now.day:02.0f}/{now.year:02.0f} ' +
                    f'{now.hour:02.0f}:{now.minute:02.0f}:{now.second:02.0f} - ' +
                    f'Updating {ticker.strip().upper()} - {count}/{len(postDownloadTickers)}')

            data = bulkData[ticker].reset_index()
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

            data = nanhandler.removeRows(data)        
            data.Open = data.Open.map(lambda x: float(x))
            data.High = data.High.map(lambda x: float(x))
            data.Low = data.Low.map(lambda x: float(x))
            data.Close = data.Close.map(lambda x: float(x))
            data['Adj Close'] = data['Adj Close'].map(lambda x: float(x))
            data.Volume = data.Volume.map(lambda x: int(x))

            # processing
            data = data[[
                'company_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]

            params = list(data.itertuples(False, None))
            try:
                database.cursor.executemany(baseInsert, params)
                database.commit()
            except pyodbc.ProgrammingError as e:
                print('Faulty params:')
                print(params)
                raise e

            database.runSQL('exec sp_deleteDuplicatePrices', verify=True)
            count += 1
