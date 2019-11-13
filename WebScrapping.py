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
# from tqdm import tnrange, tqdm_notebook
import time
from selenium import webdriver

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


def export_articles(link_dict):
    topic_list = []
    link_list_topic = []
    for topic, link in link_dict.items():
        topic_list.append(topic)
        link_list_topic.append(link)
    data_frame = {'Title': [], 'Subtitle': [], 'Page': [], 'Author': [], 'Date': [],
                  'Read_time': [], 'is_Premium': [], 'Link_Author': [], 'Link_Article': []}
    data_frame = pd.DataFrame(data=data_frame)
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
                data_frame.loc[row, 'Title'] = soup2.findAll(class_=TITLE_CLASS)[j].text
                data_frame.loc[row, 'Subtitle'] = soup2.findAll(class_=SUB_TITLE_CLASS)[j].text
                data_frame.loc[row, 'Author'] = sub_author.text
                data_frame.loc[row, 'Page'] = topic_list[i]
                data_frame.loc[row, 'Date'] = sub_time['datetime']
                data_frame.loc[row, 'Read_time'] = sub_min['title']
                data_frame.loc[row, 'is_Premium'] = sub_premium
                data_frame.loc[row, 'Link_Author'] = sub_author[BALISE_HREF]
                data_frame.loc[row, 'Link_Article'] = sub_link_article['data-action-value']
                row += 1
            except:
                pass
    return data_frame


def main():
    topic_link_dict = export_data_topic(LINK)
    data_frame = export_articles(topic_link_dict)
    print(data_frame)


if __name__ == '__main__':
    main()
