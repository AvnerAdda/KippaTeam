"""
Project of data-mining on POKEMON
By : KippaTeam
"""

from bs4 import BeautifulSoup as bs, BeautifulSoup
import requests
import urllib.request
import os
import argparse

CLASS_NAME = "data-table block-wide"
LINK_POKEMON = "https://pokemondb.net/pokedex/all"

def export_data(link):
    """
    Export nationnal pokedex
    """
    html = requests.get(link)
    html.encoding = 'ISO-8859-1'
    soup_extraction = BeautifulSoup(html.text, 'html.parser')
    pokemon_list = soup_extraction.find_all('table', attrs={'class': CLASS_NAME})
    for pokemon in pokemon_list:  # Print all occurrences
        print(pokemon.get_text())
    return pokemon_list


def main():
    export_data(LINK_POKEMON)


if __name__ == '__main__':
    main()
