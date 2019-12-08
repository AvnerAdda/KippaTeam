import pymysql
from pytrends.request import TrendReq
import config
import argparse
import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("driver_path", help="path to the chrome driver", type=str)
    parser.add_argument("password", help="password for mysql server", type=str)
    args = parser.parse_args()
    pytrends = TrendReq(hl='en-US', tz=360)
    cursorType = pymysql.cursors.DictCursor
    connectionInstance = pymysql.connect(host=config.databaseServerIP, user=config.databaseUserName,
                                         password=args.password,
                                         charset=config.charSet, cursorclass=cursorType)
    cursorInstance = connectionInstance.cursor()
    cursorInstance.execute("""select tags as c from toward_datascience.articles ;""")
    fetch = cursorInstance.fetchall()
    list_tags = []
    for i in range(len(fetch)):
        tag_list = fetch[i]['c'].split('; ')[1:6]
        list_tags.append(tag_list)
    cursorInstance.execute("""select date as d from toward_datascience.articles ;""")
    date = cursorInstance.fetchall()
    list_dates = []
    for i in range(len(fetch)):
        date_list = date[i]['d']
        list_dates.append(date_list)

    for i in range(len(list_dates)):
        df = pytrends.get_historical_interest(list_tags[i], year_start=list_dates[i].year,
                                              month_start=list_dates[i].month,
                                              day_start=list_dates[i].day, hour_start=0,
                                              year_end=list_dates[i].year, month_end=list_dates[i].month,
                                              day_end=list_dates[i].day, hour_end=0,
                                              cat=0, geo='', gprop='', sleep=0)
        print(int(df.sum(1)))
