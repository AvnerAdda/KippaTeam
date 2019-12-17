from pytrends.request import TrendReq
from datetime import timedelta
import  mysql_config
from tqdm import tqdm


def api_trend(connectionInstance):
    """
    Thus functions enriches the database with data from an external api. The api provides the total amount of trend
    traffic for all the tags that were used per article on the day that they were posted. This is done by querying
    the database for article_id, date and tags. The total_trend column is added to the articles table.
    :param connectionInstance: 
    """
    cursorInstance = connectionInstance.cursor()
    try:
        cursorInstance.execute(mysql_config.add_trend)
        cursorInstance.fetchone()
        pytrends = TrendReq(hl='en-US', tz=360)
        sqlStatement = mysql_config.select_trend
        cursorInstance.execute(sqlStatement)
        rows = cursorInstance.fetchall()
        total_list = []
        tag_list = []
        for row in tqdm(rows):
            if row['total_trend'] is None:
                try:
                    id = row['id_article']
                    date = row['date']
                    kw_list = row['tags'].split(';')
                    date1 = date + timedelta(days=1)
                    date_range = date.strftime("%Y-%m-%d") + ' ' + date1.strftime("%Y-%m-%d")
                    tag_count = len(kw_list)
                    tag_list.append(tag_count)
                    pytrends.build_payload(kw_list, cat=0, timeframe=date_range, geo='', gprop='')
                    interest_over_time_df = pytrends.interest_over_time()
                    if not interest_over_time_df.empty:
                        total = 0
                        for i in range(tag_count):
                            total += interest_over_time_df.iloc[:, i].mean()
                    else:
                        total = 0
                    my_sql_update_trend = mysql_config.update_trend
                    recordTuple = (int(total), id)
                    cursorInstance.execute(my_sql_update_trend, recordTuple)
                    cursorInstance.fetchone()
                    connectionInstance.commit()
                except:
                    pass
            else:
                pass
    except:
        pass
