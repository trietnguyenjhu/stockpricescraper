import yfinance as yf
import globalconsts


def run(database, tickers):
    database.cursor.fast_executemany = True
    for ticker in tickers:
        data = yf.download(ticker).reset_index()
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
