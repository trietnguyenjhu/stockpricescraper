import sys
import os
sys.path.append(os.path.join('..'))

import yfinance as yf

from datsup import settings
from datsup import datahandling
from dbadapter import adapter

import setupdatabase
import updateprices


def main():

    args = cli.getArgs()
    logger = log.LogManager('insiderScraper.log')
    credentials = settings.readConfig('settings.ini')

    with adapter.SQLServer(credentials) as database:
        if args.create_table and args.confirm_reset:  # setup mode
            setupdatabase.run(database)
        else:  # update modes
            if args.tickers:
                tickerList = args.update_tickers
            elif args.auto_update:
                tickerList = database.getData(
                    'select distinct ticker from insiderTrading.Company').ticker.values
                if args.filter_db:
                    fArray = data.getData(
                        f"""
                            select distinct c.ticker 
                            from {globalconsts}Price p
                            inner join insiderTrading.Company c 
                                on p.company_id = c.company_id
                        """).ticker.values
                    tickerList = datahandling.filterArray(tickerList, fArray)
            updateprices.run(database, tickerList)
        else:
            raise exceptions.InvalidModeError()


if __name__ == "__main__":
    main()