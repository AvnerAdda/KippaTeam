"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""

import argparse
import config
import mysql_config
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


def database_definition(cursorInstance):
    """
    This function defines the database tables and their columns
    :param cursorInstance:
    """
    try:
        sqlStatement = "CREATE DATABASE " + config.DB_NAME
        cursorInstance.execute(sqlStatement)
        cursorInstance.fetchall()
        cursorInstance.execute(mysql_config.create_authors)
        cursorInstance.execute(mysql_config.create_articles)
    except:
        print('Already exist')


def insert_mysql_author_base(name, curr):
    """
    This function takes in author information from the web scraper and creates author entries in the database
    :param name: string
    :param curr: cursor
    """
    mySql_insert_query = mysql_config.insert_mysql_author
    recordTuple = name
    curr.execute(mySql_insert_query, recordTuple)


def update_mysql_author(name, member, descrip, following, follower, social, curr):
    """
    This function takes in author information from the web scraper and creates author entries in the database
    :param name: string
    :param curr: cursor
    """
    mySql_update_query = mysql_config.update_mysql_author
    recordTuple = (member, descrip, following, follower, social, name)
    curr.execute(mySql_update_query, recordTuple)
    curr.fetchone()
    connectionInstance.commit()


def insert_mysql_article(title, subtitle, id_author, date, read_time, is_premium, claps, response, tags, curr):
    """
    This function takes in article information from the web scraper and creates article entries in the database
    :param title: string
    :param subtitle: string
    :param id_author: int
    :param page: string
    :param date: datetime
    :param read_time: int
    :param is_premium: boolean
    :param curr: cursor
    """
    mySql_insert_query = mysql_config.insert_mysql_article
    recordTuple = (title, subtitle, id_author, date, read_time, is_premium, claps, response, tags)
    curr.execute(mySql_insert_query, recordTuple)
    curr.fetchone()
    connectionInstance.commit()


def select_id(name, curr):
    """
    :param curr: cursor
    """
    mySql_insert_query = mysql_config.select_id
    recordTuple = name
    curr.execute(mySql_insert_query, recordTuple)
    result = curr.fetchone()
    return result['id']


def export_author_name(link_dict, cur, path):
    """
    This function creates an articles datatable which contains the following information about an article:
    Title, Subtitle, Page, Author, Date, Read_time, is_Premium, Link_Author, Link_Article.
    The function takes a dictionary of topic links, iterates through the dictionary to extract raw html from each topic
    page (using the previously defined browser) and extracts the exact information to populate the dataframe.
    :param link_dict: dictionary, cur: cursor instance
    :return: data_frame: dict_author, dict_article
    """
    topic_list = []
    link_list_topic = []
    dict_author = {}
    for topic, link in link_dict.items():
        topic_list.append(topic)
        link_list_topic.append(link)
    browser = webdriver.Chrome(path)
    for i in tqdm(range(len(topic_list))):
        browser.get(link_list_topic[i])
        soup2 = browser_scroll(browser)
        for j in range(50):
            try:
                sub = soup2.findAll(class_=config.ARTICLE_CLASS)[j]
                sub_author_link = sub.findAll(config.BALISE_A)[0]
                sub_author = sub_author_link.text
                if sub_author not in dict_author.keys():
                    dict_author[sub_author] = sub_author_link[config.BALISE_HREF]
                    insert_mysql_author_base(sub_author, cur)
                    connectionInstance.commit()
            except:
                pass
    return dict_author


def extract_article(dict_author, cur, path):
    browser = webdriver.Chrome(path)
    for key, value in dict_author.items():
        browser.get(value)
        soup2 = browser_scroll(browser)
        member_since = None
        if len(soup2.findAll(class_=config.MEMBERSHIP)) != 0:
            member_since = soup2.findAll(class_=config.MEMBERSHIP)[0].text
            member_since = str(datetime.datetime.strptime(member_since[20:], '%b %Y').date().isoformat())

        if len(soup2.findAll(class_=config.DESCRIPTION)) != 0:
            desc_author = soup2.findAll(class_=config.DESCRIPTION)[0].text
        else:
            desc_author = ''
        info_author_plus = soup2.findAll(class_=config.AUTHOR_PLUS)[0]
        if len(info_author_plus.findAll('a')) >= 2:
            social_media = True
        else:
            social_media = False
        following_author = 0
        if len(info_author_plus.findAll('a')) > 0:
            if len(info_author_plus.findAll('a')[0][config.ARIA_TAG].split(' ')) > 0:
                following_author = info_author_plus.findAll('a')[0][config.ARIA_TAG].split(' ')[1]
        follower_author = 0
        if len(info_author_plus.findAll('a')) > 1:
            if len(info_author_plus.findAll('a')[1][config.ARIA_TAG].split(' ')) >= 2:
                follower_author = info_author_plus.findAll(
                    'a')[1][config.ARIA_TAG].split(' ')[1]

        update_mysql_author(key, member_since, desc_author,
                            int(str(following_author).replace(',', '')),
                            int(str(follower_author).replace(',', '')),
                            social_media, cur)

        ## Insert Articles
        for i in range(20):
            try:
                article = soup2.findAll(class_=config.STREAM_ITEM_TAG)[i]
                date = article.findAll(class_=config.DARKEN_TAG)[0]
                link_article = date['href']
                date_time = datetime.datetime.strptime(date.findAll(attrs={config.DATETIME: True})[0][config.DATETIME],
                                                       '%Y-%m-%dT%H:%M:%S.%fZ')
                min_2_read = int(article.findAll(class_='readingTime')[0]['title'].split(' ')[0])
                is_prem = False
                if len(article.findAll(class_='u-paddingLeft4')) > 0:
                    is_prem = True
                clap = article.findAll(
                    class_=config.BUTTON_TAG)[
                    0].text
                if '.' in clap:
                    clap = clap.replace('.', '')
                if 'K' in clap:
                    clap = clap.replace('K', '000')
                clap = int(clap)
                ##
                response = 0
                if len(article.findAll(class_=config.BUTTON_CHROME)) > 0:
                    response = int(
                        article.findAll(class_=config.BUTTON_CHROME)[0].text.split(
                            ' ')[0])
                # print(link_article, date_time, min_2_read, is_prem, clap, response)
                html = requests.get(link_article)
                soup_extraction = BeautifulSoup(html.text, 'html.parser')
                complete = soup_extraction.findAll('article')[0]
                title = complete.findAll('h1', {'class': True})[0].text
                sub_title = complete.findAll('h2', {'class': True})[0].text
                text = soup_extraction.text
                tags = '; '.join(re.findall(r"(?<=\"\,\"Tag\:).*?(?=\"\,)", text))
                id_author = select_id(key, cur)

                insert_mysql_article(title, sub_title, id_author, date_time, min_2_read, is_prem, clap, response, tags,
                                     cur)
            except:
                pass
    return 0


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
    dict_author = export_author_name(topic_link_dict, cursorInstance, args.driver_path)
    dict_article = extract_article(dict_author, cursorInstance, args.driver_path)
    # print('Extract Authors')
    # export_authors(dict_author, cursorInstance)
    # print('Extract Articles Details')
    # export_articles_details(dict_article, cursorInstance)
    # connectionInstance.close()
