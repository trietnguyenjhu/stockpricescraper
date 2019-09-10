import globalconsts

def run(database):
    """Drop all tables and recreate"""

    # drop tables
    dropTables = ['Price']
    dropTables = [f'{globalconsts.SCHEMA}{x}' for x in dropTables]
    for table in dropTables:
        database.dropTableIfExists(table, verify=True)

    database.createTable(
        table=f'{globalconsts.SCHEMA}Price',
        dataVars=dict(
            day='date',
            openPrice='float',
            highPrice='float',
            lowPrice='float',
            closingPrice='float',
            adjClose='float',
            volume='int',
            ),
        foreignKeys=dict(
            company_id='insiderTrading.Company'
        )
    )
    createStoredProcedures(database)


def createStoredProcedures(database):
    sql =\
        f"""
            create or alter procedure sp_deleteDuplicatePrices as
            delete from {globalconsts.SCHEMA}Price 
            where price_id in (
                select max(price_id)
                from {globalconsts.SCHEMA}Price
                group by day, openPrice, highPrice, lowPrice, closingPrice,
                    adjClose, volume, company_id
                having count(1) > 1
            )
        """
    database.runSQL(sql, verify=True)