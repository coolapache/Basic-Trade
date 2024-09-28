import traceback
import sqlalchemy
import yahoo_fin.options as ops
from datetime import date, datetime, timedelta
import pandas as pd


def save_ticker_option_strike(ticker, strike_date):
    db_engine = sqlalchemy.create_engine('sqlite:///D:\\sqlite\\data\\finance.db')
    try:
        sdate_str = strike_date.strftime("%m/%d/%Y")
        today = date.today()
        option = ops.get_calls(ticker, sdate_str)
        # print(option)
        option['OPTION'] = 'CALL'
        option['DATE'] = today
        option['STRIKE_DATE'] = strike_date
        option['TICKER'] = ticker
        option.reset_index(inplace=True)
        option.to_sql('OPTION_DAILY', db_engine, if_exists='append')
        option = ops.get_puts(ticker, sdate_str)
        option['OPTION'] = 'PULL'
        option['DATE'] = today
        option['STRIKE_DATE'] = strike_date
        option['TICKER'] = ticker
        option.reset_index(inplace=True)
        option.to_sql('OPTION_DAILY', db_engine, if_exists='append')
        print('ticker option {} with strike date {} saved to db.'.format(ticker,strike_date))
        db_engine.dispose()
    except Exception as e:
        print("ticker {} at Date {} has error!!".format(ticker, strike_date))
        db_engine.dispose()
        print(traceback.format_exc())
        print(e)


def save_ticker_options(ticker):
    expiration_dates = ops.get_expiration_dates(ticker)
    expiration_dates = map(lambda x: datetime.strptime(x, '%B %d, %Y'), expiration_dates)
    for strike_date in expiration_dates:
        today = datetime.now()
        difference = strike_date - today
        if difference.days > 395:
            return
        else:
            save_ticker_option_strike(ticker, strike_date)


def main():
    tickers = ['PLTR', 'DUOL','CRWD','ON', 'SMCI', 'GOOG', 'AMZN','MSFT','SNPS','AMGN']
    sqlengine = sqlalchemy.create_engine('sqlite:///D:\\sqlite\\data\\finance.db')
    query = r"""select max(date) as preDate from option_daily"""
    result_set = pd.read_sql_query(query, sqlengine.connect())
    predate = result_set.iloc[0]["preDate"]
    today = date.today().isoformat()
    sqlengine.dispose()
    if predate == today:
        return
    for ticker in tickers:
        save_ticker_options(ticker)


if __name__ == '__main__':
    main()
