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
    for i in range(len(fetch)):
        tag_list = fetch[i]['c'].split('; ')[1:6]
        print(tag_list)
    cursorInstance.execute("""select date as c from toward_datascience.articles ;""")
    date = cursorInstance.fetchall()
    kw_list = tag_list[1:6]
    pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo='', gprop='news')
    df = pytrends.get_historical_interest(kw_list, year_start=2019, month_start=12, day_start=1, hour_start=0,
                                           year_end=2019,
                                           month_end=12, day_end=2, hour_end=0, cat=0, geo='', gprop='', sleep=0)
    print(df.columns, df.sum())
