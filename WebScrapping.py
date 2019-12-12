"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""

import argparse
import config
import datetime
import re
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm


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
    """
    This function takes in article information from the web scraper and creates article entries in the database
    :param id_article: int
    :param title: string
    :param subtitle: string
    :param id_author: int
    :param page: string
    :param date: datetime
    :param read_time: int
    :param is_premium: boolean
    :param curr: cursor
    """
    mySql_insert_query = """INSERT INTO Toward_DataScience.articles (id_article ,title ,subtitle ,\
     id_author, page , date , read_time , is_Premium) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    recordTuple = (id_article, title, subtitle, id_author, page, date, read_time, is_premium)
    curr.execute(mySql_insert_query, recordTuple)


def if_exist_article(curr, title, page):
    """
    This function is used to determine if an article already exists in the database.
    :param curr: cursor
    :param title: string
    :param author: string
    :return: not None/None
    """
    curr.execute("""SELECT DISTINCT title 
    FROM toward_datascience.articles 
    WHERE toward_datascience.articles.title = %s and toward_datascience.articles.page = %s""", (title, page))
    result = curr.fetchone()
    return result


def export_articles(link_dict, cur, path):
    """
    This function creates an articles datatable which contains the following information about an article:
    Title, Subtitle, Page, Author, Date, Read_time, is_Premium, Link_Author, Link_Article.
    The function takes a dictionary of topic links, iterates through the dictionary to extract raw html from each topic
    page (using the previously defined browser) and extracts the exact information to populate the dataframe.
    :param link_dict: dictionary, cur: cursor instance
    :return: data_frame: dict_author, dict_article
    """
    try:
        topic_list = []
        link_list_topic = []
        dict_author = {}
        dict_article = {}
        for topic, link in link_dict.items():
            topic_list.append(topic)
            link_list_topic.append(link)
        row = max_id_sql(cur)[1]
        id_author = max_id_sql(cur)[0]
        browser = webdriver.Chrome(path)
        for i in tqdm(range(len(topic_list))):
            browser.get(link_list_topic[i])
            soup2 = browser_scroll(browser)
            for j in range(50):
                try:
                    sub = soup2.findAll(class_=config.ARTICLE_CLASS)[j]
                    sub2 = soup2.findAll(class_=config.LINK_ARTICLE_LINK)[j]
                    title = soup2.findAll(class_=config.TITLE_CLASS)[j].text
                    page = str(topic_list[i])
                    sub_link_article = sub2.findAll('a')[0]
                    dict_article[row] = sub_link_article[config.DATA_ACTION]
                    if if_exist_article(cur, title, page) is None:
                        sub_time = sub.findAll('time')[0]
                        sub_min = sub.findAll(class_='readingTime')[0]
                        date_format = datetime.datetime.strptime(sub_time['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
                        if len(sub.findAll(class_='svgIcon-use')) != 0:
                            sub_premium = 1
                        else:
                            sub_premium = 0
                        sub_author = sub.findAll(config.BALISE_A)[0]
                        detect_author = if_exist_author(cur, sub_author.text)
                        if sub_author.text not in dict_author.keys():
                            dict_author[sub_author.text] = [id_author, sub_author[config.BALISE_HREF]]
                            id_author += 1
                            id_author_name = dict_author[sub_author.text][0]
                        else:
                            if detect_author is not None:
                                id_author_name = detect_author['id']
                            else:
                                id_author_name = dict_author[sub_author.text][0]

                        insert_mysql_article(int(row),
                                             title,
                                             soup2.findAll(class_=config.SUB_TITLE_CLASS)[j].text,
                                             id_author_name,
                                             str(topic_list[i]),
                                             date_format.isoformat(),
                                             int(sub_min['title'].split(' ')[0]),
                                             sub_premium, cur)
                        row += 1
                        if row % 100 == 0:
                            connectionInstance.commit()
                except Exception as e:
                    pass
        connectionInstance.commit()
        return dict_author, dict_article
    except Exception as e:
        print("Exception occured:{}".format(e))


def max_id_sql(curr):
    """
    This function finds the most max id of table. The max id will represent the most recently inserted row in the table
    :param curr: cursor
    :return: id: int
    """
    try:
        result = []
        curr.execute("""SELECT MAX(id) as max_author FROM toward_datascience.authors ;""")
        results = curr.fetchall()
        result.append(results[0]['max_author']+1)
        curr.execute("""SELECT MAX(id_article) as max_article FROM toward_datascience.articles ;""")
        results = curr.fetchall()
        result.append(results[0]['max_article']+1)
        return result
    except:
        return [1, 1]


def insert_mysql_author(id, name, member_since, description, following, followers, social_media, curr):
    """
    This function takes in author information from the web scraper and creates author entries in the database
    :param id: int
    :param name: string
    :param member_since: datetime
    :param description: string
    :param following: int
    :param followers: int
    :param social_media: boolean
    :param curr: cursor
    """
    mySql_insert_query = """INSERT INTO Toward_DataScience.authors (id ,name ,member_since ,\
                 description, following , followers , social_media) \
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    recordTuple = (id, name, member_since, description, following, followers, social_media)
    curr.execute(mySql_insert_query, recordTuple)


def if_exist_author(curr, name):
    """
    This function is used to determine if an author already exists in the database.
    :param curr: cursor
    :param name: string
    :return: not None/None
    """
    curr.execute("""SELECT DISTINCT toward_datascience.authors.name , toward_datascience.authors.id
    FROM toward_datascience.authors 
    WHERE toward_datascience.authors.name = %s""", name)
    return curr.fetchone()


def export_authors(dict_author, curr):
    """
    This function creates an author dataframe which contains the following information about each author of an article
    in the article dataframe:
    Name, Member_Since, Description, Following, Followers, Social_Media.
    The function takes a dataframe of articles as its input and iterates through each "Link_Author" in the dataframe,
    extracts raw html from each author page, and extracts exact information to populate the dataframe.
    :param dict_author: dict , curr
    :return: nothing
    """
    row = max_id_sql(curr)[0]
    for key, value in tqdm(dict_author.items()):
        try:
            html = requests.get(value[1])
            soup_extraction = BeautifulSoup(html.text, 'html.parser')
            name_author = soup_extraction.findAll('h1')[0].text
            if if_exist_author(curr, name_author) is None:
                member_since = None
                if len(soup_extraction.findAll(class_=config.MEMBERSHIP)) != 0:
                    member_since = soup_extraction.findAll(class_=config.MEMBERSHIP)[0].text
                    member_since = str(datetime.datetime.strptime(member_since[20:], '%b %Y').date().isoformat())

                if len(soup_extraction.findAll(class_=config.DESCRIPTION)) != 0:
                    desc_author = soup_extraction.findAll(class_=config.DESCRIPTION)[0].text
                else:
                    desc_author = ''

                info_author_plus = soup_extraction.findAll(class_=config.AUTHOR_PLUS)[0]
                if len(info_author_plus.findAll('a')) >= 2:
                    social_media = True
                else:
                    social_media = False

                following_author = 0
                if len(info_author_plus.findAll('a')) > 0:
                    if len(info_author_plus.findAll('a')[0]['aria-label'].split(' ')) > 0:
                        following_author = info_author_plus.findAll('a')[0]['aria-label'].split(' ')[1]

                follower_author = 0
                if len(info_author_plus.findAll('a')) > 1:
                    if len(info_author_plus.findAll('a')[1]['aria-label'].split(' ')) >= 2:
                        follower_author = info_author_plus.findAll(
                            'a')[1]['aria-label'].split(' ')[1]

                insert_mysql_author(value[0], name_author, member_since, desc_author,
                                    int(str(following_author).replace(',', '')),
                                    int(str(follower_author).replace(',', '')),
                                    social_media, curr)
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
    :param dict_article: dict
    :return: nothing
    """
    for key, value in tqdm(dict_article.items()):
        html = requests.get(value)
        soup_extraction = BeautifulSoup(html.text, 'html.parser')
        text = soup_extraction.text
        clap = re.search(r"(?<=clapCount\"\:)([0-9]+)", text)[0]
        tags = '; '.join(re.findall(r"(?<=\"\,\"Tag\:).*?(?=\"\,)", text))
        mySql_update_query = """UPDATE Toward_DataScience.articles SET claps=%s, tags=%s WHERE id_article=%s;"""
        recordTuple = (clap, tags, key)
        curr.execute(mySql_update_query, recordTuple)
        curr.fetchone()
        if key % 100 == 0:
            connectionInstance.commit()
    connectionInstance.commit()


def database_definition(cursorInstance):
    """
    This function defines the database tables and their columns
    :param cursorInstance:
    """
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
                            claps INT NULL DEFAULT NULL,
                            tags TEXT NULL DEFAULT NULL,
                            PRIMARY KEY (id_article))''')
    except Exception as e:
        print("Your database already exists, we implement it")


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