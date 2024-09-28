from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import numpy as np
import sqlalchemy
import time
from datetime import date, datetime, timedelta


def save_monthly_weather(year, month, the_tag, table_name):
    # table = BeautifulSoup(table_text,"html.parser")
    try:
        data = []
        for sub_table in the_tag.findAll('table', attrs={'aria-labelledby': "Days data"}):
            sub_data = []
            for tr in sub_table.findAll('tr', attrs={'class': ['ng-star-inserted']}):
                tds = tr.findAll('td', attrs={'class': ['ng-star-inserted']})
                tds = [ele.text.strip() for ele in tds]
                sub_data.append(tds)
            data.append(sub_data)

        ## convert 3 dim raw data to 2 dim data
        df_raw = pd.DataFrame(data)
        dim2_data = []
        for series_name, series in df_raw.items():
            row = []
            for srow in series:
                for e in srow:
                    row.append(e)
            dim2_data.append(row)

        # hard coding header
        new_header = ['DATE', 'TEMPERATURE_MAX', 'TEMPERATURE_AVG', 'TEMPERATURE_MIN', 'DEW_POINT_MAX', 'DEW_POINT_AVG',
                      'DEW_POINT_MIN',
                      'HUMIDITY_MAX', 'HUMIDITY_AVG', 'HUMIDITY_MIN', 'WIND_SPEED_MAX', 'WIND_SPEED_AVG',
                      'WIND_SPEED_MIN',
                      'PRESSURE_MAX', 'PRESSURE_AVG', 'PRESSURE_MIN', 'PRECIPITATION']
        dim2_data[0] = new_header
        np_data = np.array(dim2_data)
        df = pd.DataFrame(data=np_data[1:, 0:], columns=np_data[0, 0:])

        # df['DATE'] = year +'-' +month +'-'+df['DATE']
        df['DATE'] = df['DATE'].apply(lambda v: datetime.strptime(year + '-' + month + '-' + v, "%Y-%m-%d"))
        # print(df['DATE'])
        engine = sqlalchemy.create_engine('sqlite:///D:\\sqlite\\data\\finance.db')
        df.to_sql(table_name, engine, if_exists='append')
        print("monthly weather saved for year {}, month {}", year, month)
    except Exception as e:
        print("monthly weather save failed: year {}, month {}", year, month)
        print(e)


def get_monthly_weather(year, month, baseurl):
    try:
        url = baseurl + year + '-' + month
        # url = 'https://www.wunderground.com/history/monthly/gb/london/EGLC/date/'+year+'-'+month
        print(url)
        driver = webdriver.Chrome()
        driver.get(url)

        html = driver.page_source
        soup = BeautifulSoup(html)
        table = soup.find(lambda tag: tag.name == 'table' and tag.has_attr('aria-labelledby') and tag[
            'aria-labelledby'] == 'History days')
        # print(type(table))
        # print(table['class'])
        # print(table)
        return table
    except Exception as e:
        print("unable get weather data: year {},month {}".format(year, month))
        print(e)


def get_and_save(year, month, baseurl, table_name):
    month_data = get_monthly_weather(year, month, baseurl)
    save_monthly_weather(year, month, month_data, table_name)


