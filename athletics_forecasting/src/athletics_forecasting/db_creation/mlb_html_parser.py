import sqlite3
import traceback

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
from html_table_parser.parser import HTMLTableParser
import urllib.request

def parse_tbc_data(html_results):
    """
    Parses data from TheBaseballCube
    
    ex: https://www.thebaseballcube.com/content/box/WAS202204070~r/
    """
    parser = HTMLTableParser()
    parser.feed(html_results)

    output = [pd.DataFrame(t[1:], columns=t[0]) for t in parser.tables]
    #output[0].drop(output[0].columns[4], axis=1, inplace=True)
    #output[0].drop(output[0].columns[-1], axis=1, inplace=True)
    return output


def clean_parsed_tbc_dataframes(df_list, date):
    team_df = df_list[0][['team','r','h','e','record','streak','div rk','lg rk','mlb rk']]
    team_df.loc[0,['record','streak']], team_df.loc[1,['record','streak']] = team_df.loc[1,['record','streak']], team_df.loc[0,['record','streak']]
    team_df['wins'] = team_df.apply(lambda d: d['record'].split('-')[0], axis=1)
    team_df['losses'] = team_df.apply(lambda d: d['record'].split('-')[1], axis=1)
    team_df['games_played'] = team_df['wins']+ team_df['losses']
    team_df.drop('record', axis=1, inplace=True)
    team_df['year'] = date[:4]


    def split_streak(df):
        """Helper function"""
        result, val = df['streak'].split(' ')
        if 'lost'==result.lower():
            return -1 * int(val)
        else:
            return int(val)

    team_df['streak'] = team_df.apply(split_streak, axis=1)
    team_df[team_df.columns[1:]] = team_df[team_df.columns[1:]].apply(pd.to_numeric, errors='coerce')
    team_df['win'] = team_df['r'] == team_df['r'].max()
    team_df['opponent'] = team_df.loc[::-1,'team'].values
    team_df['date'] = date

    visit_bat = df_list[1].replace(r'^\s*$', np.nan, regex=True)
    home_bat = df_list[2].replace(r'^\s*$', np.nan, regex=True)
    visit_bat['team'] = team_df['team'][0]
    home_bat['team'] = team_df['team'][1]
    visit_bat['opponent'] = team_df['team'][1]
    home_bat['opponent'] = team_df['team'][0]

    batting = pd.concat([visit_bat[:-1], home_bat[:-1]]).drop('ln',axis=1).dropna(axis=0)
    batting['age'] = batting['age'].astype(float)
    batting[batting.columns[3:20]] = batting[batting.columns[3:20]].apply(pd.to_numeric, errors='coerce')
    batting['date'] = date

    def clean_pitcher_name(pname):
        return ' '.join(pname.split(' ')[:2])

    visit_pitch = df_list[3].replace(r'^\s*$', np.nan, regex=True)
    visit_pitch['player'] = visit_pitch['player'].apply(clean_pitcher_name)
    home_pitch = df_list[4].replace(r'^\s*$', np.nan, regex=True)
    home_pitch['player'] = home_pitch['player'].apply(clean_pitcher_name)
    visit_pitch['team'] = team_df['team'][0]
    home_pitch['team'] = team_df['team'][1]
    visit_pitch['opponent'] = team_df['team'][1]
    home_pitch['opponent'] = team_df['team'][0]

    pitchers = pd.concat([visit_pitch, home_pitch]).dropna(axis=0).drop('seq', axis=1)
    pitchers['date'] = date
    pitchers[pitchers.columns[1:20]] = pitchers[pitchers.columns[1:20]].apply(pd.to_numeric, errors='coerce')


    pitchers['pct_strike'] = pitchers.apply(lambda d: d['pit'] and d['str']/d['pit'] or 0, axis=1)
    pitchers['pct_ball'] = pitchers.apply(lambda d: d['pit'] and d['bal']/d['pit'] or 0, axis=1)
    pitchers['hpi'] = pitchers.apply(lambda d: d['ip'] and d['h']/d['ip'] or 0, axis=1)
    pitchers['rpi'] = pitchers.apply(lambda d: d['ip'] and d['r']/d['ip'] or 0, axis=1)
    
    return team_df, batting, pitchers


def fetch_next_page(home,date):
    """Get html of game log."""
    url = f'https://www.thebaseballcube.com/content/box/{home}{date}0~r/'

    with urllib.request.urlopen(url) as url_page:
        bytes = url_page.read()
    
    return bytes.decode("utf8")


def scrape_year_df(df_year, db_file):

    failures = []
    with sqlite3.connect(db_file) as conn:

        for i, row in df_year.iterrows():
            date = row['box date']
            games = set(row['list of games'].split(' ... '))
            for game in games:
                try:
                    print(f'Parsing {game} on {date}')
                    home = game.split('-')[1]
                    html = fetch_next_page(home, date.replace('-',''))
                    df_list = parse_tbc_data(html)
                    team, bat, pitch = clean_parsed_tbc_dataframes(df_list, date)
                except Exception as e:
                    print(e)
                    failures.append([f'{date}, {game}', e])
                    continue
                
                team.to_sql('teams',conn,if_exists='append',index=False)
                bat.to_sql('batters',conn,if_exists='append',index=False)
                pitch.to_sql('pitchers',conn,if_exists='append',index=False)
        print(f'Failures:')
        print(failures)
        fail = pd.DataFrame(failures)
        fail.to_csv('failures.csv')

if __name__ =='__main__':
    data = pd.read_csv('C:\\Users\\Alekche\\Documents\\UVA\\MLB_data\\cur_year.csv',header=0)
    scrape_year_df(data,'team_data.db')
    
    #df_list = parse_tbc_data(txt)
    #team, batting, pitchers = clean_parsed_tbc_dataframes(df_list,'2021-04-31')

    