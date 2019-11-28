"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""

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
import pymysql

SCROLL_PAUSE_TIME = 2
LINK = "https://towardsdatascience.com/"
ARTICLE_CLASS = 'postMetaInline postMetaInline-authorLockup ui-captionStrong u-flex1 u-noWrapWithEllipsis'
TITLE_CLASS = 'u-letterSpacingTight u-lineHeightTighter u-breakWord u-textOverflowEllipsis u-lineClamp3 u-fontSize24'
SUB_TITLE_CLASS = 'u-fontSize18 u-letterSpacingTight u-lineHeightTight u-marginTop7 u-textColorNormal u-baseColor--textNormal'

DRIVER = "C:/Users/avner/Downloads/chromedriver_win32/chromedriver.exe"

LINK_ARTICLE = 'col u-xs-marginBottom10 u-paddingLeft0 u-paddingRight0 u-paddingTop15 u-marginBottom30'
LINK_ARTICLE_LINK = 'u-lineHeightBase postItem'
BALISE_A = 'a'
BALISE_HREF = 'href'

AUTHOR_NAME = 'ui-h2 hero-title'
MEMBERSHIP = 'ui-caption u-textColorGreenNormal u-fontSize13 u-tintSpectrum u-accentColor--textNormal u-marginBottom20'
DESCRIPTION = 'ui-body hero-description'
FOLLOWERS = "button button--chromeless u-baseColor--buttonNormal is-touched"
AUTHOR_PLUS = 'buttonSet u-noWrap u-marginVertical10'

# Create a connection object
databaseServerIP = "127.0.0.1"  # IP address of the MySQL database server
databaseUserName = "root"  # User name of the database server
databaseUserPassword = "avner"  # Password for the database user
newDatabaseName = "TowardDataScience"  # Name of the database that is to be created
charSet = "utf8mb4"  # Character set
cursorType = pymysql.cursors.DictCursor
connectionInstance = pymysql.connect(host=databaseServerIP, user=databaseUserName, password=databaseUserPassword,
                                     charset=charSet, cursorclass=cursorType)


def export_data_topic(link):
    """
    Export Toward_Data_Science
    """
    html = requests.get(link)
    topic = {}
    soup_extraction = BeautifulSoup(html.text, 'html.parser')
    for i in range(0, len(soup_extraction.findAll('li')) - 3):
        topic[soup_extraction.findAll('li')[i].text] = soup_extraction.findAll('li')[i].findAll('a')[0]['href']
    return topic


def browser_scroll(browser):
    last_height = browser.execute_script("return document.body.scrollHeight")
    while True:
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    return BeautifulSoup(browser.page_source, "html.parser")


def export_sql_articles(link_dict):
    try:
        cursorInstance = connectionInstance.cursor()
        sqlStatement = "CREATE DATABASE " + newDatabaseName
        cursorInstance.execute(sqlStatement)
        databaseList = cursorInstance.fetchall()
        cursorInstance.execute('''CREATE TABLE TowardDataScience.authors (
                            id_author INT,
                            name CHAR,
                            member_since DATETIME,
                            description CHAR,
                            following INT,
                            followers INT,
                            social_media BIT,
                            PRIMARY KEY (id_author))''')
        cursorInstance.execute('''CREATE TABLE TowardDataScience.articles (
                            id_article INT,
                            title CHAR,
                            subtitle CHAR,
                            page CHAR,
                            id_author CHAR,
                            date CHAR,
                            read_time CHAR,
                            is_Premium CHAR,
                            link_author CHAR,
                            link_article CHAR,
                            PRIMARY KEY (id_article))''') #,
                            #FOREIGN KEY (id_author) REFERENCES TowardDataScience.authors(id_author))''')
        topic_list = []
        link_list_topic = []
        for topic, link in link_dict.items():
            topic_list.append(topic)
            link_list_topic.append(link)
        row = 0
        browser = webdriver.Chrome(DRIVER)
        for i in range(len(topic_list)):
            browser.get(link_list_topic[i])
            soup2 = browser_scroll(browser)
            for j in range(50):
                try:
                    sub = soup2.findAll(class_=ARTICLE_CLASS)[j]
                    sub2 = soup2.findAll(class_=LINK_ARTICLE_LINK)[j]
                    sub_link_article = sub2.findAll('a')[0]
                    sub_author = sub.findAll(BALISE_A)[0]
                    sub_time = sub.findAll('time')[0]
                    sub_min = sub.findAll(class_='readingTime')[0]
                    try:
                        sub_premium = True if sub.findAll(class_='svgIcon-use')[0] != '' else ''
                    except:
                        sub_premium = False
                    cursorInstance.execute("INSERT INTO TowardDataScience.articles (id_article ,title ,subtitle ,\
                    page , id_author , date , read_time , is_Premium , link_author ,link_article) \
                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                           # [0,'title','sub','page',1,'123','1',1,'bb','bx'])
                                           [int(row),
                                            str(soup2.findAll(class_=TITLE_CLASS)[j].text),
                                            str(soup2.findAll(class_=SUB_TITLE_CLASS)[j].text),
                                            str(sub_author.text),
                                            str(topic_list[i]),

                                            str(sub_time['datetime']),
                                            str(sub_min['title']),
                                            str(sub_premium),
                                            str(sub_author[BALISE_HREF]),
                                            str(sub_link_article['data-action-value'])])
                    row += 1
                    print(row)
                    connectionInstance.commit()
                except:
                    pass
    except Exception as e:
        print("Exception occured:{}".format(e))
    finally:
        connectionInstance.close()

def main():
    print('Each step could take time, so no worry')
    print('Extract Topics')
    topic_link_dict = export_data_topic(LINK)
    print('Extract Articles')
    data_frame_article = export_sql_articles(topic_link_dict)
    # print('Extract Authors')
    # data_frame_author = export_authors(data_frame_article)
    # print(data_frame_author.head())
    # print('Extract Articles Details')
    # data_frame_articles_detail = export_articles_details(data_frame_article)
    # print(data_frame_articles_detail.head())


if __name__ == '__main__':
    main()
