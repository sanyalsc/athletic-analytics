import sqlite3
import os

import pandas as pd

ABB_CONV = {'ARI':'Arizona Diamondbacks',
    'ATL':'Atlanta Braves',
    'BAL':'Baltimore Orioles',
    'BOS':'Boston Red Sox',
    'BRS':'Boston Red Sox',
    'CUB':'Chicago Cubs',
    'CHC':None,
    'CWS':'Chicago White Sox',
    'CIN':'Cincinnati Reds',
    '':'Cleveland Guardians',
    'CLE':'Cleveland Indians',
    'COL':'Colorado Rockies',
    'DET':'Detroit Tigers',
    'HOU':'Houston Astros',
    'KAN':'Kansas City Royals',
    'LAA':'Los Angeles Angels',
    'LAD':'Los Angeles Dodgers',
    'LOS':'Los Angeles Dodgers',
    'MIA':'Miami Marlins',
    'MIL':'Milwaukee Brewers',
    'MIN':'Minnesota Twins',
    'NYM':'New York Mets',
    'NYY':'New York Yankees',
    'OAK':'Oakland Athletics',
    'PHI':'Philadelphia Phillies',
    'PIT':'Pittsburgh Pirates',
    'SDG':'San Diego Padres',
    'SFO':'San Francisco Giants',
    'SFG':'San Francisco Giants',
    'SEA':'Seattle Mariners',
    'STL':'St. Louis Cardinals',
    'TAM':'Tampa Bay Rays',
    'TEX':'Texas Rangers',
    'TOR':'Toronto Blue Jays',
    'WAS':'Washington Nationals'}


def parse_odds_file(file):
    fname = file.split('.')
    year = fname[0][-4:]
    data = pd.read_csv(file, header=0)
    data = data[['Team','Date'] +list(data.columns[-8:])]
    data['Team'] = data['Team'].apply(lambda x: ABB_CONV[x])
    data.dropna(axis=0)
    def date_transform(date, year):
        date = str(date)
        day = date[-2:]
        month = date[:-2]
        if len(month)<2:
            month = '0'+month
        return f'{year}-{month}-{day}'
    data['Date'] = data['Date'].apply(date_transform,year=year)

    data.columns = ['team',
            'date',
            'open',
            'close',
            'run_line',
            'run_odds',
            'ou_open',
            'ou_open_odds',
            'ou_close',
            'ou_close_odds']
    with sqlite3.connect('team.db') as conn:
        data.to_sql('odds',conn,if_exists='append',index=False)

    



if __name__ == '__main__':
    odds = 'C:\\Users\\Alekche\\Documents\\UVA\\MLB_data\\odds_data\\'
    files = os.listdir(odds)
    for file in files:
        if os.path.isdir(os.path.join(odds,file)):
            continue
        parse_odds_file(os.path.join(odds,file))

