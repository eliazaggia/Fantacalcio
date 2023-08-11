import requests
import json
import pandas as pd
import time
import os

RAPID_API_KEY = os.environ.get("RAPID_API_KEY")

headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-RapidAPI-Key': RAPID_API_KEY
}

def get_leagues_from(country):
    url = f"https://api-football-v1.p.rapidapi.com/v2/leagues/search/{country}"
    response = requests.request("GET", url, headers=headers)
    leagues_dict = response.json()['api']['leagues']
    leagues_df = pd.DataFrame.from_dict(leagues_dict)
    return leagues_df

def get_fixtures_from_league(league_id):
    url = f"https://api-football-v1.p.rapidapi.com/v2/fixtures/league/{league_id}"
    response = requests.request("GET", url, headers=headers)
    fixtures_dict = response.json()['api']['fixtures']
    fixtures_df = pd.DataFrame.from_dict(fixtures_dict)
    return fixtures_df

def get_list_fixtures_id(league_id):
    fixtures_df = get_fixtures_from_league(league_id)
    fixtures_id = fixtures_df['fixture_id']
    fixtures_id_list = fixtures_id.tolist()
    return fixtures_id_list

def get_stats_per_game_for_league(league_id):
    requests_per_minute = 30
    request_delay_seconds = 60 / requests_per_minute

    fixtures_id_list = get_list_fixtures_id(league_id)

    stats_per_game_list = []
    for fixture in fixtures_id_list:
        url = f"https://api-football-v1.p.rapidapi.com/v2/players/fixture/{fixture}"
        response = requests.request("GET", url, headers=headers)
        time.sleep(request_delay_seconds)
        stats_per_game_list.append(response.json())

    league_list=[]
    for i in stats_per_game_list:
        players_data = i['api']['players']
        league_list.extend(players_data)


    df = pd.DataFrame(league_list)

    return df

def normalizing_columns_with_dictionaries(df):
    columns_with_dicts = df.applymap(lambda x: isinstance(x, dict)).any()

    columns_with_dicts_list = []
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, dict)).any() == True:
            columns_with_dicts_list.append(c)

    df_league_norm = df.copy()

    for col in columns_with_dicts_list:
        df_norm = pd.json_normalize(df_league_norm[col], sep='.')
        df_norm.columns = [f'{col}_{colname}' for colname in df_norm.columns]
        df_league_norm = pd.concat([df_league_norm.drop(col, axis=1), df_norm], axis=1)

    return df_league_norm


def merge_fixture_norm_df_with_df_league_norm(league_id,df):
    fixtures_league_df = get_fixtures_from_league(league_id)
    fixtures_league_df.rename(columns={'fixture_id': 'event_id'}, inplace=True)

    fixtures_league_norm = normalizing_columns_with_dictionaries(fixtures_league_df)
    df_league_norm = normalizing_columns_with_dictionaries(df)

    df_league_fin = pd.merge(df_league_norm, fixtures_league_norm, on='event_id', how='outer')

    return df_league_fin


def preprocessing(df):

    #splitting date and time and transforming them as datetime type
    df['event_date'] = pd.to_datetime(df['event_date'])
    df['match_date'] = df['event_date'].dt.date
    df['match_time'] = df['event_date'].dt.time
    df.drop(columns=['event_date'], inplace=True)

    #filling up NaN
    df['captain'].fillna(False, inplace=True)

    df['offsides'].fillna(0, inplace=True)

    df['rating'].fillna(0, inplace=True)
    df['rating'] = df['rating'].astype(float)

    df['minutes_played'].fillna(0, inplace=True)

    #accessing rounds numbers
    df['round'] = pd.to_numeric(df['round'].str.extract(r'(\d+)')[0], errors='ignore')
    df['round'].fillna(39, inplace=True)

    #encoding
    df = pd.get_dummies(df, columns=['position'], prefix=['position'])
    df['substitute'] = df['substitute'].astype(int)

    return df.to_csv(f'league.csv', index=False)
