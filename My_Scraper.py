from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import *
import pandas as pd
import os


class TransfermarktSpider():
    """
    A Spider for Transfermarkt.com
    """

    def __init__(self):
        self.DBPATH = os.getcwd() + '\\'
        self.DBFILE = 'TransferDB_0'
        self.setting = requests.session()
        self.setting.headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
        self.sites = [('league','http://www.transfermarkt.com')]
        self.host = 'http://www.transfermarkt.com'
        print("Scraper Initialized")

    def __create_db(self):
        """
        :return:
        """
        self.__check_db()
        self.engine = create_engine('sqlite:///' + self.DBPATH + self.DBFILE)
        self.connection = self.engine.connect()
        self.metadata = MetaData()

        leagues = Table('leagues', self.metadata,
                        Column('league_id', INTEGER, primary_key=True),
                        Column('league_name', String),
                        Column('league_link', String),
                        Column('percentage_foreign', REAL),
                        Column('country', String),
                        Column('number_clubs', INTEGER),
                        Column('number_players', INTEGER),
                        Column('age_average', REAL),
                        Column('total_value', REAL),
                        Column('year', INTEGER)
                        )
        teams = Table('teams', self.metadata,
                      Column('team_id', INTEGER, primary_key=True),
                      Column('club_name', String),
                      Column('league_name', String, ForeignKey('leagues.league_name')),
                      Column('club_link', String),
                      Column('number_players', INTEGER),
                      Column('market_value', REAL),
                      Column('age_average', REAL),
                      Column('number_foreign', INTEGER),
                      Column('position_league', String),
                      Column('year', INTEGER)
                      )
        players = Table('players', self.metadata,
                        Column('player_id', INTEGER, primary_key=True),
                        Column('player_name', String),
                        Column('club_name', String, ForeignKey('teams.club_name')),
                        Column('player_link', String),
                        Column('birth_date', String),
                        Column('market_value', INTEGER),
                        Column('nationality', String),
                        Column('year', INTEGER)
                        )

        self.metadata.create_all(self.engine)
        print("Tables Created")

    def __check_db(self):
        if os.path.isfile(self.DBPATH + self.DBFILE):
            files = os.listdir(self.DBPATH)

            self.DBFILE = 'TransferDB_{}'.format(num + 1)

    def __store_db(self, table, data):
        """
        :param table:
        :param data:
        :return:
        """
        if table == 'leagues':
            statement = text(
                "INSERT INTO leagues (league_name, league_link,"
                "percentage_foreign, country, number_clubs,"
                "number_players, age_average, total_value, year)"
                "VALUES (:league_name, :league_link, :percentage_foreign,"
                ":country, :number_clubs, :number_players, :age_average,"
                ":total_value, :year)"
            )
        elif table == 'teams':
            statement = text(
                "INSERT INTO teams (club_name, club_link,"
                "number_foreign, number_players,"
                "age_average, market_value, year,"
                "position_league, league_name)"
                "VALUES (:club_name, :club_link, :number_foreign,"
                ":number_players, :age_average, :market_value,"
                ":year, :position_league, :league_name)"
            )
        elif table == 'players':
            statement = text(
                "INSERT INTO players (player_name, player_link,"
                "birth_date, market_value, nationality, club_name,"
                "year)"
                "VALUES (:player_name, :player_link, :birth_date,"
                ":market_value, :nationality, :club_name, :year)"
            )

        self.connection.execute(statement, data)

    def __getPage(self, site):
        """
        :param site:
        :return:
        """
        page = self.setting.get(site)
        html = page.content
        return BeautifulSoup(html, 'lxml')

    def parse_structure(self):
        """
        :return:
        """
        while len(self.sites)>0:
            site = self.sites.pop()
            if site[0] != None:
                try:
                    soup = self.getPage(site[1])
                except (RuntimeError, TypeError, NameError):
                    soup = self.getPage(site[1])
                links = self.parse_manager(soup,site[0])
                self.sites.extend(links) if links else None
        print('End')
        self.connection.close()

    def parse_manager(self,soup,page_type):
        """
        :param soup:
        :param page_type:
        :return:
        """
        if page_type == 'league':
            return self.competition_parse(soup)
        elif page_type == 'club':
            return self.club_parse(soup)
        elif page_type == 'player':
            return self.player_parse(soup)

    def competition_parse(self, soup):
        """
        :param soup:
        :return:
        """
        soup2 = soup.find_all("td", string="1. league")
        soup3 = soup2[0].find_parent("tr")
        league_soup = soup3.find_next_siblings()
        links = []
        for league in league_soup:
            Leagues = {}
            Leagues['league_name'] = league.find_all("a")[1].string
            Leagues["league_link"] = self.host + league.find_all("a")[1]["href"]
            Leagues["percentage_foreign"] = float('.'.join(re.findall('\d+', league.find_all("a")[2].string)))
            Leagues["country"] = league.contents[1].img["title"]
            Leagues["number_clubs"] = int(league.contents[2].string)
            Leagues["number_players"] = int(league.contents[3].string)
            Leagues["age_average"] = float('.'.join(re.findall('\d+', league.contents[4].string)))
            Leagues["total_value"] = float('.'.join(re.findall('\d+', league.contents[4].string)))
            Leagues['year'] = 2016
            links.append(('club',Leagues["league_link"]))
            self.store_db('leagues',Leagues)
            print(Leagues['league_name'] + ' Loadded')

        return links

    def club_parse(self,soup):
        """
        :param team_soup:
        :return:
        """
        soup2 = soup.find_all("div", class_="spielername-profil")
        league_name = soup2[0].string.replace("\r","").replace("\n","").replace("\t","")

        soup2 = soup.find_all("table", class_="items")
        soup3 = soup2[0].tbody
        team_soup = soup3.find_all("tr")
        links = []
        for team in team_soup:
            Teams = {}
            Teams["club_name"] = team.find_all("a")[1].string
            Teams["club_link"] = self.host + team.find_all("a")[0]["href"]
            Teams["league_name"] = league_name
            Teams["number_players"] = int(team.contents[4].a.string)
            Teams["market_value"] = float('.'.join(re.findall('\d+',
                                                              team.contents[7].a.string)))
            Teams["age_average"] = float('.'.join(re.findall('\d+', team.contents[5].string)))
            Teams["number_foreign"] = int(team.contents[6].string)
            Teams["year"] = 2016
            Teams["position_league"] = None
            links.append(('player',Teams["club_link"]))
            self.store_db('teams', Teams)
            print(Teams['league_name'] + ' ' + Teams["club_name"] + ' Loadded')

        soup2 = soup.find_all("div", class_="spielername-profil")
        Teams["league_name"] = soup2[0].string.replace("\r","").replace("\n","").replace("\t","")
        return links

    def player_parse(self, soup):
        """
        :param soup:
        :return:
        """
        soup2 = soup.find_all("div", class_="spielername-profil")
        club_name = soup2[0].h1.string.replace("\r", "").replace("\n", "").replace("\t", "")

        soup2 = soup.find_all("table", class_="items")
        soup3 = soup2[0].tbody
        player_soup = [item for item in soup3.contents if item != '\n']
        links = []
        for player in player_soup:
            Players = {}
            Players["player_name"] = player.find_all("td")[5].string
            Players["player_link"] = self.host + player.find_all("a", class_="spielprofil_tooltip")[0]["href"]
            Players["club_name"] = club_name
            Players["birth_date"] = player.find_all("td")[6].string
            Players["market_value"] = float(".".join(re.findall('\d+', player.find_all("td")[8].contents[0]))) \
                if re.findall('\d+', player.find_all("td")[8].contents[0]) else 0.0
            Players["nationality"] = player.find_all("td")[7].img["alt"]
            Players["year"] = 2016
            links.append((None,Players["player_link"]))
            self.store_db('players', Players)
            print(Players["club_name"] + ' ' + Players["player_name"] + ' Loadded')

        return links


#Getting links from starting page
#soup2 = soup.find_all("a",itemprop = "url", class_=" flex-box navi-item")
#links = {tag.string.strip(): site + tag['href'] for tag in soup2 if tag.string}

