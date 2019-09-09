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
            date='date',
            open='float',
            high='float',
            low='float',
            close='float',
            adjClose='float',
            volume='float',
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
                group by date, open, high, low, close, adjClose, volume, company_id
            )
        """
    database.runSQL(sql, verify=True)