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
import datetime
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
newDatabaseName = "Toward_DataScience"  # Name of the database that is to be created
charSet = "utf8mb4"  # Character set
cursorType = pymysql.cursors.DictCursor
connectionInstance = pymysql.connect(host=databaseServerIP, user=databaseUserName, password=databaseUserPassword,
                                     charset=charSet, cursorclass=cursorType)


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
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    return BeautifulSoup(browser.page_source, "html.parser")


def export_articles(link_dict, cur):
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
        browser = webdriver.Chrome(DRIVER)
        for i in range(len(topic_list)):
            browser.get(link_list_topic[i])
            soup2 = browser_scroll(browser)
            for j in range(50):
                try:
                    sub = soup2.findAll(class_=ARTICLE_CLASS)[j]
                    sub2 = soup2.findAll(class_=LINK_ARTICLE_LINK)[j]
                    sub_author = sub.findAll(BALISE_A)[0]
                    sub_time = sub.findAll('time')[0]
                    sub_min = sub.findAll(class_='readingTime')[0]
                    date_format = datetime.datetime.strptime(sub_time['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
                    if len(sub.findAll(class_='svgIcon-use')) != 0:
                        sub_premium = 1
                    else:
                        sub_premium = 0
                    if sub_author.text not in dict_author.keys():
                        dict_author[sub_author.text] = [id_author, sub_author[BALISE_HREF]]
                        id_author += 1
                    dict_article[row] = sub_author[BALISE_HREF]
                    mySql_insert_query = """INSERT INTO Toward_DataScience.articles (id_article ,title ,subtitle ,\
                     id_author, page , date , read_time , is_Premium) \
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    recordTuple = (int(row),
                                   soup2.findAll(class_=TITLE_CLASS)[j].text,
                                   soup2.findAll(class_=SUB_TITLE_CLASS)[j].text,
                                   dict_author[sub_author.text][0],
                                   str(topic_list[i]),
                                   date_format.isoformat(),
                                   int(sub_min['title'].split(' ')[0]),
                                   sub_premium)
                    cur.execute(mySql_insert_query, recordTuple)
                    row += 1
                    if row % 100 == 0:
                        connectionInstance.commit()
                    print(row)
                except:
                    pass
        connectionInstance.commit()
        return dict_author, dict_article
    except Exception as e:
        print("Exception occured:{}".format(e))


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
    row = 1
    for key, value in dict_author.items():
        print(key)
        try:
            html = requests.get(value[1])
            soup_extraction = BeautifulSoup(html.text, 'html.parser')
            name_author = soup_extraction.findAll('h1')[0].text
            try:
                member_since = soup_extraction.findAll(class_=MEMBERSHIP)[0].text
                member_since = str(datetime.datetime.strptime(member_since[20:], '%b %Y').date().isoformat())
            except:
                member_since = ''
            try:
                desc_author = soup_extraction.findAll(class_=DESCRIPTION)[0].text
            except:
                desc_author = ''

            info_author_plus = soup_extraction.findAll(class_=AUTHOR_PLUS)[0]

            try:
                test = info_author_plus.findAll('a')[2]['aria-label']
                social_media = True
            except:
                social_media = False

            try:
                following_author = info_author_plus.findAll('a')[0]['aria-label'].split(' ')[1]
            except:
                following_author = ''

            try:
                follower_author = info_author_plus.findAll(
                    'a')[1]['aria-label'].split(' ')[1]
            except:
                follower_author = ''

            mySql_insert_query = """INSERT INTO Toward_DataScience.authors (id ,name ,member_since ,\
                         description, following , followers , social_media) \
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            recordTuple = (value[0], name_author, member_since, desc_author,
                           following_author.replace(',', ''), follower_author.replace(',', ''), social_media)
            curr.execute(mySql_insert_query, recordTuple)
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
    for key, value in dict_article.items():
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
        sqlStatement = "CREATE DATABASE " + newDatabaseName
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
                            date DATE,
                            read_time INT,
                            is_Premium TINYINT(1),
                            PRIMARY KEY (id_article))''')
    except Exception as e:
        print("Exception occured:{}".format(e))


if __name__ == '__main__':
    print('Each step could take time, so no worry')
    print('Extract Topics')
    topic_link_dict = export_data_topic(LINK)
    print('Create Database')
    cursorInstance = connectionInstance.cursor()
    database_definition(cursorInstance)
    print('Extract Articles')
    dict_author, dict_article = export_articles(topic_link_dict, cursorInstance)
    print('Extract Authors')
    export_authors(dict_author, cursorInstance)
    print('Extract Articles Details')
    export_articles_details(dict_article, cursorInstance)
    connectionInstance.close()
