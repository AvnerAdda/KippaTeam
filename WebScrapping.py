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
from tqdm import tnrange, tqdm_notebook

LINK = "https://towardsdatascience.com/"


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


def export_articles(link_dict):
    topic_list = []
    link_list = []
    for topic, link in link_dict.items():
        topic_list.append(topic)
        link_list.append(link)
    data_frame = {'Title': [], 'Subtitle': [], 'Page': [], 'Author': [], 'Date': [], 'Read_time': [], 'is_Premium': []}
    data_frame = pd.DataFrame(data=data_frame)
    indice_table = 0
    for j in tqdm_notebook(range(3)):
        for i in tqdm_notebook(range(len(topic_list))):
            response2 = requests.get(link_list[i])
            soup2 = BeautifulSoup(response2.text, "html.parser")
            sub = soup2.findAll(
                class_='postMetaInline postMetaInline-authorLockup ui-captionStrong u-flex1 u-noWrapWithEllipsis')[j]
            sub_autor = sub.findAll('a')[0].text
            sub_time = sub.findAll('time')[0]
            sub_min = sub.findAll(class_='readingTime')[0]
            try:
                sub_prem = sub.findAll(class_='svgIcon-use')[0]
                sub_prem = True
            except:
                sub_prem = False
            data_frame.loc[indice_table, 'Title'] = soup2.findAll(
                class_='u-letterSpacingTight u-lineHeightTighter u-breakWord u-textOverflowEllipsis u-lineClamp3 '
                       'u-fontSize24')[
                j].text
            data_frame.loc[indice_table, 'Subtitle'] = soup2.findAll(
                class_='u-fontSize18 u-letterSpacingTight u-lineHeightTight u-marginTop7 u-textColorNormal '
                       'u-baseColor--textNormal')[
                j].text
            data_frame.loc[indice_table, 'Author'] = sub_autor
            data_frame.loc[indice_table, 'Page'] = topic_list[i]
            data_frame.loc[indice_table, 'Date'] = sub_time['datetime']
            data_frame.loc[indice_table, 'Read_time'] = sub_min['title']
            data_frame.loc[indice_table, 'is_Premium'] = sub_prem
            indice_table += 1
    return data_frame


def main():
    topic_link_dict = export_data_topic(LINK)
    #     for key,value in topic_link_dict.items() :
    #         print (key, value)
    data_frame = export_articles(topic_link_dict)
    print(data_frame)


if __name__ == '__main__':
    main()
