import yfinance as yf
import pandas_ta
import sqlalchemy
from datetime import date, datetime, timedelta


def dowloadTicker(ticker, start_date):
    stock_data = yf.download(ticker, start=start_date)
    stock_data["ticker"] = ticker

    stock_data.ta.macd(append=True)
    stock_data.ta.rsi(append=True)
    stock_data.ta.bbands(append=True)
    stock_data.ta.obv(append=True)
    stock_data.ta.sma(length=20, append=True)

    stock_data.ta.ema(length=50, append=True)
    stock_data.ta.stoch(append=True)
    stock_data.ta.adx(append=True)
    stock_data.ta.ad(append=True)
    stock_data.ta.stdev(append=True)

    return stock_data


def save_tickers(ticker_list, start_date):
    engine = sqlalchemy.create_engine('sqlite:///D:\\sqlite\\data\\finance.db')
    for ticker in ticker_list:
        stock_data = dowloadTicker(ticker, start_date)
        stock_data.reset_index(inplace=True)
        stock_data['Y_CLOSE5_MAX'] = stock_data['Adj Close'].rolling(5).max().shift(-5)
        stock_data['Y_CLOSE5_MIN'] = stock_data['Adj Close'].rolling(5).min().shift(-5)
        stock_data.to_sql('STOCK_DAILY', engine, if_exists='append')
        print('ticker {} saved to db.'.format(ticker))


def main():
    tickers = ['PLTR', 'DUOL', 'CRWD', 'ON', 'SMCI', 'GOOG', 'AMZN', 'MSFT']
    today = date.today()
    save_tickers(tickers, today.strftime('%Y-%m-%d'))


if __name__ == '__main__':
    main()
