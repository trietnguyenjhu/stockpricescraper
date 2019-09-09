import sys
import os
sys.path.append(os.path.join('..'))

import yfinance as yf

from datsup import settings
from datsup import datahandling
from datsup import log
from dbadapter import adapter

import cli
import setupdatabase
import updateprices
import globalconsts


def main():

    args = cli.getArgs()
    logger = log.LogManager('error.log')
    credentials = settings.readConfig('settings.ini')['auth']

    with adapter.SQLServer(credentials) as database:
        if args.create_table and args.confirm_reset:  # setup mode
            setupdatabase.run(database)
        else:  # update modes
            if args.tickers:
                tickerList = args.tickers
            elif args.auto_update:
                tickerList = database.getData(
                    'select distinct ticker from insiderTrading.Company').ticker.values
                if args.filter_db:
                    fArray = database.getData(
                        f"""
                            select distinct c.ticker 
                            from {globalconsts.SCHEMA}Price p
                            inner join insiderTrading.Company c 
                                on p.company_id = c.company_id
                        """).ticker.values
                    tickerList = datahandling.filterArray(tickerList, fArray)
            else:
                raise exceptions.InvalidModeError()
            updateprices.run(database, tickerList)


if __name__ == "__main__":
    main()