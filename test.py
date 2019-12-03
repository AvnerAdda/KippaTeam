"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""


# C:/Users/Nathan/Desktop/chromedriver_win32/chromedriver.exe redboxb,b500 nathan_db


from bs4 import BeautifulSoup as bs, BeautifulSoup
import requests
import urllib.request
import os
import argparse
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
import datetime
import pymysql
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
    for i in range(0, len(soup_extraction.findAll('li')) - 3):
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


def export_sql_articles(link_dict, cur, driver, connectionInstance):
    """
    This function creates an articles datatable which contains the following information about an article:
    Title, Subtitle, Page, Author, Date, Read_time, is_Premium, Link_Author, Link_Article.
    The function takes a dictionary of topic links, iterates through the dictionary to extract raw html from each topic
    page (using the previously defined browser) and extracts the exact information to populate the dataframe.
    :param link_dict: dictionary, cur: cursor instance
    :param cur
    :param driver
    :return: data_frame: nothing
    """
    try:
        topic_list = []
        link_list_topic = []
        for topic, link in link_dict.items():
            topic_list.append(topic)
            link_list_topic.append(link)
        row = 0
        browser = webdriver.Chrome(driver)
        for i in range(len(topic_list)):
            browser.get(link_list_topic[i])
            soup2 = browser_scroll(browser)
            for j in range(50):
                try:
                    sub = soup2.findAll(class_=config.ARTICLE_CLASS)[j]
                    sub2 = soup2.findAll(class_=config.LINK_ARTICLE_LINK)[j]
                    sub_link_article = sub2.findAll('a')[0]
                    sub_author = sub.findAll(config.BALISE_A)[0]
                    sub_time = sub.findAll('time')[0]
                    sub_min = sub.findAll(class_='readingTime')[0]
                    date_format = datetime.datetime.strptime(sub_time['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
                    if len(sub.findAll(class_='svgIcon-use')) != 0:
                        sub_premium = 1
                    else:
                        sub_premium = 0
                    mySql_insert_query = """INSERT INTO TowardDataScience.articles (id_article ,title ,subtitle ,\
                     author, page , date , read_time , is_Premium , link_author ,link_article) \
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    recordTuple = (int(row),
                                   soup2.findAll(class_=config.TITLE_CLASS)[j].text,
                                   soup2.findAll(class_=config.SUB_TITLE_CLASS)[j].text,
                                   str(sub_author.text),
                                   str(topic_list[i]),
                                   date_format.isoformat(),
                                   int(sub_min['title'].split(' ')[0]),
                                   sub_premium,
                                   str(sub_author[config.BALISE_HREF]),
                                   str(sub_link_article['data-action-value']))
                    cur.execute(mySql_insert_query, recordTuple)
                    if row % 100 == 0:
                        connectionInstance.commit()
                    row += 1
                    print(row)
                except:
                    pass
            connectionInstance.commit()
    except Exception as e:
        print("Exception occured:{}".format(e))


def sql_creator(connectionInstance, driver, db_name):
    print('Each step could take time, so no worry')
    print('Extract Topics')
    topic_link_dict = export_data_topic(config.LINK)
    print('Create Database')
    cursorInstance = connectionInstance.cursor()
    sqlStatement = "CREATE DATABASE " + db_name
    cursorInstance.execute(sqlStatement)
    cursorInstance.fetchall()
    cursorInstance.execute('''CREATE TABLE TowardDataScience.articles (
                        id_article INT,
                        title TEXT ,
                        subtitle TEXT ,
                        author TEXT,
                        page TEXT,
                        date TEXT,
                        read_time INT,
                        is_Premium TINYINT(1),
                        link_author TEXT,
                        link_article TEXT,
                        PRIMARY KEY (id_article))''')
    cursorInstance.execute('''CREATE TABLE TowardDataScience.authors (
                        id INT,
                        name TEXT,
                        member_since DATETIME,
                        description TEXT,
                        following INT,
                        followers INT,
                        social_media TINYINT(1),
                        PRIMARY KEY (id),
                        FOREIGN KEY (id) REFERENCES TowardDataScience.articles(id_article))''')
    print('Extract Articles')
    export_sql_articles(topic_link_dict, cursorInstance, driver, connectionInstance)
    print('Extract Authors')
    link_author = []
    cursorInstance.execute('''SELECT link_author FROM TowardDataScience.articles''')
    list_dict_author = cursorInstance.fetchall()
    for dict_author in list_dict_author:
        link_author.append(dict_author['link_author'])


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("driver_path", help="path to the chrome driver", type=str)
    parser.add_argument("password", help="password for mysql server", type=str)
    args = parser.parse_args()
    cursorType = pymysql.cursors.DictCursor
    connectionInstance = pymysql.connect(host=config.databaseServerIP, user=config.databaseUserName,
                                         password=args.password, charset=config.charSet, cursorclass=cursorType)
    sql_creator(connectionInstance, args.driver_path, config.DB_NAME)


if __name__ == '__main__':
    main()