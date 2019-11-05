"""
Project of data-mining on POKEMON
By : KippaTeam
"""

from bs4 import BeautifulSoup as bs, BeautifulSoup
import requests
import urllib.request
import os
import argparse

CLASS_NAME = "infocard-list infocard-list-pkmn-lg"


def export_data(link):
    """
    Export nationnal pokedex
    """
    html = requests.get(link)
    html.encoding = 'ISO-8859-1'
    soup = BeautifulSoup(html.text, 'html.parser')
    pokemon_list = soup.find_all('div', attrs={'class': CLASS_NAME})
    for pokemon in pokemon_list:  # Print all occurrences
        print(pokemon.get_text())
    return pokemon_list


def main():
    export_data('https://pokemondb.net/pokedex/national')


if __name__ == '__main__':
    main()
