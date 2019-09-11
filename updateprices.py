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
import ssl
import http

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
    batch = 0
    batchSize = 50
    batches = int(len(tickers)/batchSize)

    # split list of tickers in batches of 100s
    for tickerSubset in datahandling.splitIterableEvenly(tickers, batchSize):
        
        log.timestampPrintToConsole(
            f'Downloading batch {batch}/{batches} - {sorted(tickerSubset)}')
        flagIterTicker = True
        iterTickerCount = 0

        while flagIterTicker:
            # renew proxy if all popped from last iter
            if len(proxyPool) == 0: proxyPool = getProxyPool()
            
            fArray = pd.read_csv('badproxies.csv').values
            proxyPool = datahandling.filterArray(proxyPool, fArray)
            proxyPool = list(proxyPool) # coerce to list for pop()

            proxyPath = random.choice(proxyPool)
            proxy = {'https': proxyPath}

            try:
                update(database, proxy, tickerSubset, batch, batches)
            except exceptions.ProxyError as e:
                proxyPool.pop(proxyPool.index(proxyPath)) # remove faulty proxy from pool
                logger.logError(e)
                fileio.appendLine('badproxies.csv', proxyPath)
                if len(proxyPool) == 0: flagIterTicker = False # give up on batch if every proxies fail
                continue
            else:
                flagIterTicker = False  # break out of while loop if download success
            batch += 1


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


def update(database, proxy, tickerSubset, batch: int, batches: int):
    """Update data"""
    try:
        tickers = tickerSubset if len(tickerSubset) == 1 else ' '.join(tickerSubset)
        bulkData = yf.download(tickers, progress=True, proxy=proxy)
    except (
        requests.exceptions.SSLError,
        requests.exceptions.ProxyError,
        urllib3.exceptions.MaxRetryError,
        requests.exceptions.ChunkedEncodingError,
        urllib3.exceptions.ProtocolError,
        urllib3.exceptions.NewConnectionError,
        TimeoutError,
        ssl.SSLCertVerificationError,
        http.client.RemoteDisconnected
        ) as e: # invalid proxy
        raise exceptions.ProxyError(f'Unable to use proxy - {proxy}')
    else:
        if len(tickers) > 1:
            bulkData = bulkData.swaplevel(axis=1)
            # downloaded tickers check
            postDownloadTickers = np.unique([x[0] for x in bulkData.columns])
        else:
            postDownloadTickers = tickerSubset
        
        count = 0
        for ticker in postDownloadTickers:

            now = datetime.datetime.now()
            log.timestampPrintToConsole(
                f'Batch {batch}/{batches} - Updating {ticker.strip().upper()} {count}/{len(postDownloadTickers)}')

            # data subsetting - bulk download handling
            if len(tickers) > 1:
                data = bulkData[ticker].reset_index()
            else:
                data = bulkData.reset_index()

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

