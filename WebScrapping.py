"""
Project of data-mining on Towards Data Science
By : KippaTeam
"""
import csv

from bs4 import BeautifulSoup
import requests
import os
import pandas as pd
import time
from selenium import webdriver
import re

SCROLL_PAUSE_TIME = 2
LINK = "https://towardsdatascience.com/"
ARTICLE_CLASS = 'postMetaInline postMetaInline-authorLockup ui-captionStrong u-flex1 u-noWrapWithEllipsis'
TITLE_CLASS = 'u-letterSpacingTight u-lineHeightTighter u-breakWord u-textOverflowEllipsis u-lineClamp3 u-fontSize24'
SUB_TITLE_CLASS = 'u-fontSize18 u-letterSpacingTight u-lineHeightTight u-marginTop7 u-textColorNormal u-baseColor--textNormal'
DRIVER = "C:/Users/avner/Downloads/chromedriver_win32/chromedriver.exe"  # To change
LINK_ARTICLE = 'col u-xs-marginBottom10 u-paddingLeft0 u-paddingRight0 u-paddingTop15 u-marginBottom30'
LINK_ARTICLE_LINK = 'u-lineHeightBase postItem'
BALISE_A = 'a'
BALISE_HREF = 'href'
AUTHOR_NAME = 'ui-h2 hero-title'
MEMBERSHIP = 'ui-caption u-textColorGreenNormal u-fontSize13 u-tintSpectrum u-accentColor--textNormal u-marginBottom20'
DESCRIPTION = 'ui-body hero-description'
FOLLOWERS = "button button--chromeless u-baseColor--buttonNormal is-touched"
AUTHOR_PLUS = 'buttonSet u-noWrap u-marginVertical10'


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


def export_articles(link_dict):
    """
    This function creates an articles dataframe which contains the following information about an article:
    Title, Subtitle, Page, Author, Date, Read_time, is_Premium, Link_Author, Link_Article.
    The function takes a dictionary of topic links, iterates through the dictionary to extract raw html from each topic
    page (using the previously defined browser) and extracts the exact information to populate the dataframe.
    :param link_dict: dictionary
    :return: data_frame: dataframe
    """
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


def export_authors(data_frame_article):
    """
    This function creates an author dataframe which contains the following information about each author of an article
    in the article dataframe:
    Name, Member_Since, Description, Following, Followers, Social_Media.
    The function takes a dataframe of articles as its input and iterates through each "Link_Author" in the dataframe,
    extracts raw html from each author page, and extracts exact information to populate the dataframe.
    :param data_frame_article: dataframe
    :return: author: dataframe
    """
    author = {'Name': [], 'Member_Since': [], 'Description': [], 'Following': [], 'Followers': [], 'Social_Media': []}
    author = pd.DataFrame(data=author)
    row = 0
    for i in range(len(data_frame_article['Link_Author'])):
        html = requests.get(data_frame_article.iloc[i, 7])
        soup_extraction = BeautifulSoup(html.text, 'html.parser')
        name_author = soup_extraction.findAll('h1')[0].text
        try:
            member_since = soup_extraction.findAll(class_=MEMBERSHIP)[0].text
        except:
            member_since = 'NULL'
        try:
            desc_author = soup_extraction.findAll(class_=DESCRIPTION)[0].text
        except:
            desc_author = 'NULL'

        info_author_plus = soup_extraction.findAll(class_=AUTHOR_PLUS)[0]

        try:
            test = info_author_plus.findAll('a')[2]['aria-label']
            social_media = True
        except:
            social_media = False

        try:
            following_author = info_author_plus.findAll('a')[0]['aria-label'].split(' ')[1]
        except:
            following_author = 'NULL'

        try:
            follower_author = info_author_plus.findAll('a')[1]['aria-label'].split(' ')[1]
        except:
            follower_author = 'NULL'

        author.loc[row, 'Name'] = name_author
        author.loc[row, 'Member_Since'] = member_since[19:]
        author.loc[row, 'Description'] = desc_author
        author.loc[row, 'Following'] = following_author
        author.loc[row, 'Followers'] = follower_author
        author.loc[row, 'Social_Media'] = social_media
        row += 1
    return author


def export_articles_details(data_frame_article):
    """
    This function import more details about the articles of the first dataframe. We have the Title as the key, we have
    the number of claps (as like for facebook) and the different tags on the article>
    :param data_frame_article: the first dataframe
    :return: the dataframe of hte detail of each article
    """
    details_articles = {'Title': [], 'Claps': [], 'Tags': []}
    details_articles = pd.DataFrame(data=details_articles)
    row = 0
    for i in range(len(data_frame_article['Link_Article'])):
        html = requests.get(data_frame_article.iloc[i, 8])
        soup_extraction = BeautifulSoup(html.text, 'html.parser')
        text = soup_extraction.text
        clap = re.search(r"(?<=clapCount\"\:)([0-9]+)", text)[0]
        tags = '; '.join(re.findall(r"(?<=\"\,\"Tag\:).*?(?=\"\,)", text))
        details_articles.loc[row, 'Title'] = data_frame_article.iloc[i, 0]
        details_articles.loc[row, 'Claps'] = clap
        details_articles.loc[row, 'Tags'] = tags
        row += 1
    return details_articles


def compare_two_file(file1, file2):
    """
    Compare 2 files (this function could permit to see how many data could we have by day)
    The files must be csv
    """
    with open(file1, encoding="utf-8") as t1, open(file2, encoding="utf-8") as t2:
        file_one = csv.reader(t1, delimiter=',')
        file_two = csv.reader(t2, delimiter=',')
        title1 = []
        title2 = []
        for line in file_two:
            title1.append(line[0])
        for line in file_one:
            title2.append(line[0])
        diff_doc1 = 0
        for i in title1:
            if i not in title2:
                diff_doc1 += 1
        print('There is ' + str(diff_doc1) + ' differences between these 2 versions.')


def export_csv(data_frame):
    """
    Export to csv the files
    """
    file_name = os.getcwd() + '\\TDS_' + time.strftime("%d_%b_%Y_%H%M", time.gmtime()) + '.csv'
    data_frame.to_csv(file_name, index=None, header=True)


def main():
    print('Each step could take time, so no worry')
    print('Extract Topics')
    topic_link_dict = export_data_topic(LINK)
    print('Extract Articles')
    data_frame_article = export_articles(topic_link_dict)
    print(data_frame_article.head())
    print('Extract Authors')
    data_frame_author = export_authors(data_frame_article)
    print(data_frame_author.head())
    print('Extract Articles Details')
    data_frame_articles_detail = export_articles_details(data_frame_article)
    print(data_frame_articles_detail.head())


if __name__ == '__main__':
    main()
