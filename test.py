"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""
import argparse

from bs4 import BeautifulSoup as bs, BeautifulSoup
import requests
import time
from selenium import webdriver
import re
import datetime
import pymysql
from tqdm import tqdm
import config


def export_data_topic(link):
    """
    This function gets the links to the five "data science" themed topics that are available on the towardsdatascience
    homepage. The input is a link to the towardsdatascience homepage and the output is a dictionary containing the names
    of each topic and their corresponding links.
    :param link: string
    :return: topic: list
    """
    html = requests.get(link)
    topic = {}
    soup_extraction = BeautifulSoup(html.text, 'html.parser')
    for i in tqdm(range(0, len(soup_extraction.findAll('li')) - 3)):
        topic[soup_extraction.findAll('li')[i].text] = soup_extraction.findAll('li')[i].findAll('a')[0]['href']
    return topic


def browser_scroll(browser):
    """
    This function defines automated browsing through the desired web page which allows for the access to raw html that
    is only available through scrolling.
    :param browser: browser object
    :return: Beautiful Soup object
    """
    last_height = browser.execute_script("return document.body.scrollHeight")
    while True:
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(config.SCROLL_PAUSE_TIME)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    return BeautifulSoup(browser.page_source, "html.parser")


def insert_mysql_article(id_article, title, subtitle, id_author, page, date, read_time, is_premium, curr):
    mySql_insert_query = """INSERT INTO Toward_DataScience.articles (id_article ,title ,subtitle ,\
     id_author, page , date , read_time , is_Premium) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    recordTuple = (id_article, title, subtitle, id_author, page, date, read_time, is_premium)
    curr.execute(mySql_insert_query, recordTuple)


def export_articles(link_dict, cur, path):
    """
    This function creates an articles datatable which contains the following information about an article:
    Title, Subtitle, Page, Author, Date, Read_time, is_Premium, Link_Author, Link_Article.
    The function takes a dictionary of topic links, iterates through the dictionary to extract raw html from each topic
    page (using the previously defined browser) and extracts the exact information to populate the dataframe.
    :param link_dict: dictionary, cur: cursor instance
    :return: data_frame: nothing
    """
    try:
        topic_list = []
        link_list_topic = []
        dict_author = {}
        dict_article = {}
        for topic, link in link_dict.items():
            topic_list.append(topic)
            link_list_topic.append(link)
        row = 1
        id_author = 1
        browser = webdriver.Chrome(path)
        for i in tqdm(range(len(topic_list))):
            browser.get(link_list_topic[i])
            soup2 = browser_scroll(browser)
            for j in range(50):
                try:
                    sub = soup2.findAll(class_=config.ARTICLE_CLASS)[j]
                    sub2 = soup2.findAll(class_=config.LINK_ARTICLE_LINK)[j]
                    sub_author = sub.findAll(config.BALISE_A)[0]
                    sub_time = sub.findAll('time')[0]
                    sub_min = sub.findAll(class_='readingTime')[0]
                    date_format = datetime.datetime.strptime(sub_time['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
                    if len(sub.findAll(class_='svgIcon-use')) != 0:
                        sub_premium = 1
                    else:
                        sub_premium = 0
                    if sub_author.text not in dict_author.keys():
                        dict_author[sub_author.text] = [id_author, sub_author[config.BALISE_HREF]]
                        id_author += 1
                    dict_article[row] = sub_author[config.BALISE_HREF]
                    insert_mysql_article(int(row),
                                         soup2.findAll(class_=config.TITLE_CLASS)[j].text,
                                         soup2.findAll(class_=config.SUB_TITLE_CLASS)[j].text,
                                         dict_author[sub_author.text][0],
                                         str(topic_list[i]),
                                         date_format.isoformat(),
                                         int(sub_min['title'].split(' ')[0]),
                                         sub_premium, cur)
                    row += 1
                    if row % 100 == 0:
                        connectionInstance.commit()
                except:
                    pass
        connectionInstance.commit()
        return dict_author, dict_article
    except Exception as e:
        print("Exception occured:{}".format(e))


def max_id_sql(curr, data):
    try:
        table = 'toward_datascience.' + data
        mySql_select_max_query = """SELECT MAX(id_article) as max FROM %s """
        recordTuple = table
        curr.execute(mySql_select_max_query, recordTuple)
        result = curr.fetchone()
        return result['max']
    except:
        return 0


def insert_mysql_author(id, name, member_since, description, following, followers, social_media, curr):
    mySql_insert_query = """INSERT INTO Toward_DataScience.authors (id ,name ,member_since ,\
                 description, following , followers , social_media) \
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    recordTuple = (id, name, member_since, description, following, followers, social_media)
    curr.execute(mySql_insert_query, recordTuple)


def export_authors(dict_author, curr):
    """
    This function creates an author dataframe which contains the following information about each author of an article
    in the article dataframe:
    Name, Member_Since, Description, Following, Followers, Social_Media.
    The function takes a dataframe of articles as its input and iterates through each "Link_Author" in the dataframe,
    extracts raw html from each author page, and extracts exact information to populate the dataframe.
    :param data_frame_article: dataframe
    :return: author: dataframe
    """
    row = max_id_sql(curr, 'author') + 1
    for key, value in tqdm(dict_author.items()):
        try:
            html = requests.get(value[1])
            soup_extraction = BeautifulSoup(html.text, 'html.parser')
            name_author = soup_extraction.findAll('h1')[0].text
            member_since = None
            if len(soup_extraction.findAll(class_=config.MEMBERSHIP)) != 0:
                member_since = soup_extraction.findAll(class_=config.MEMBERSHIP)[0].text
                member_since = str(datetime.datetime.strptime(member_since[20:], '%b %Y').date().isoformat())

            if len(soup_extraction.findAll(class_=config.DESCRIPTION)) != 0 :
                desc_author = soup_extraction.findAll(class_=config.DESCRIPTION)[0].text
            else:
                desc_author = ''

            info_author_plus = soup_extraction.findAll(class_=config.AUTHOR_PLUS)[0]
            if len(info_author_plus.findAll('a')) >= 2:
                social_media = True
            else:
                social_media = False

            if len(info_author_plus.findAll('a')) >= 2:
                following_author = info_author_plus.findAll('a')[0]['aria-label'].split(' ')[1]
            else:
                following_author = 0

            if len(info_author_plus.findAll('a')) >= 2:
                follower_author = info_author_plus.findAll(
                    'a')[1]['aria-label'].split(' ')[1]
            else:
                follower_author = 0

            insert_mysql_author(value[0], name_author, member_since, desc_author,
                                int(str(following_author).replace(',', '')), int(str(follower_author).replace(',', '')), social_media, curr)
            row += 1
            curr.fetchone()
            if row % 100 == 0:
                connectionInstance.commit()
        except Exception as e:
            print("Exception occured:{}".format(e))
    connectionInstance.commit()


def export_articles_details(dict_article, curr):
    """
    This function import more details about the articles of the first dataframe. We have the Title as the key, we have
    the number of claps (as like for facebook) and the different tags on the article>
    :param data_frame_article: the first dataframe
    :return: the dataframe of hte detail of each article
    """
    curr.execute("""ALTER TABLE Toward_DataScience.articles
                        ADD COLUMN claps INT NOT NULL AFTER `is_Premium`,
                        ADD COLUMN tags TEXT NOT NULL AFTER `claps`;""")
    for key, value in tqdm(dict_article.items()):
        html = requests.get(value)
        soup_extraction = BeautifulSoup(html.text, 'html.parser')
        text = soup_extraction.text
        clap = re.search(r"(?<=ClapCount\"\:)([0-9]+)", text)[0]
        tags = '; '.join(re.findall(r"(?<=\"\,\"name\"\:\").*?(?=\"\,)", text)[1:])
        mySql_update_query = """UPDATE Toward_DataScience.articles SET claps=%s, tags=%s WHERE id_article=%s;"""
        recordTuple = (clap, tags, key)
        curr.execute(mySql_update_query, recordTuple)
        curr.fetchone()
        if key % 100 == 0:
            connectionInstance.commit()
    connectionInstance.commit()


def database_definition(cursorInstance):
    try:
        sqlStatement = "CREATE DATABASE " + config.DB_NAME
        cursorInstance.execute(sqlStatement)
        cursorInstance.fetchall()
        cursorInstance.execute('''CREATE TABLE Toward_DataScience.authors (
                            id INT NOT NULL,
                            name TEXT,
                            member_since TEXT,
                            description TEXT,
                            following INT,
                            followers INT,
                            social_media TINYINT(1),
                            PRIMARY KEY (id))''')
        cursorInstance.execute('''CREATE TABLE Toward_DataScience.articles (
                            id_article INT NOT NULL,
                            title TEXT ,
                            subtitle TEXT ,
                            id_author INT NOT NULL,
                            page TEXT,
                            date DATETIME NULL DEFAULT NULL,
                            read_time INT,
                            is_Premium TINYINT(1),
                            PRIMARY KEY (id_article))''')
    except Exception as e:
        print("Exception occured:{}".format(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("driver_path", help="path to the chrome driver", type=str)
    parser.add_argument("password", help="password for mysql server", type=str)
    args = parser.parse_args()

    cursorType = pymysql.cursors.DictCursor
    connectionInstance = pymysql.connect(host=config.databaseServerIP, user=config.databaseUserName,
                                         password=args.password,
                                         charset=config.charSet, cursorclass=cursorType)
    print('Each step could take time, so no worry')
    print('Extract Topics')
    topic_link_dict = export_data_topic(config.LINK)
    print('Create Database')
    cursorInstance = connectionInstance.cursor()
    database_definition(cursorInstance)
    print('Extract Articles')
    dict_author, dict_article = export_articles(topic_link_dict, cursorInstance, args.driver_path)
    print('Extract Authors')
    export_authors(dict_author, cursorInstance)
    print('Extract Articles Details')
    export_articles_details(dict_article, cursorInstance)
    connectionInstance.close()
