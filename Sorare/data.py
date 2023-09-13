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


def own_goals_serie_a_2020(df):
    df['own_goals'] = 0
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Lorenzo Venuti'), 'own_goals'] = 1
    df.loc[(df['round'] == 4) & (df['player_name'] == 'Takehiro Tomiyasu'), 'own_goals'] = 1
    df.loc[(df['round'] == 25) & (df['player_name'] == 'Leonardo Spinazzola'), 'own_goals'] = 1
    df.loc[(df['round'] == 21) & (df['player_name'] == 'Marco Silvestri'), 'own_goals'] = 1
    df.loc[(df['round'] == 31) & (df['player_name'] == 'Gianluca Scamacca'), 'own_goals'] = 1
    df.loc[(df['round'] == 14) & (df['player_name'] == 'Alex Sandro'), 'own_goals'] = 1
    df.loc[(df['round'] == 8) & (df['player_name'] == 'Vasco Regini'), 'own_goals'] = 1
    df.loc[(df['round'] == 11) & (df['player_name'] == 'Andrea Poli'), 'own_goals'] = 1
    df.loc[(df['round'] == 3) & (df['player_name'] == 'Luca Pellegrini'), 'own_goals'] = 1
    df.loc[(df['round'] == 4) & (df['player_name'] == 'Lorenzo Montipò'), 'own_goals'] = 1
    df.loc[(df['round'] == 36) & (df['player_name'] == 'Salvatore Molina'), 'own_goals'] = 1
    df.loc[(df['round'] == 34) & (df['player_name'] == 'Adam Marušić'), 'own_goals'] = 1
    df.loc[(df['round'] == 15) & (df['player_name'] == 'Luca Marrone'), 'own_goals'] = 1
    df.loc[(df['round'] == 25) & (df['player_name'] == 'Nikola Maksimović'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Giangiacomo Magnani'), 'own_goals'] = 1
    df.loc[(df['round'] == 11) & (df['player_name'] == 'Manuel Lazzari'), 'own_goals'] = 1
    df.loc[(df['round'] == 20) & (df['player_name'] == 'Riccardo Improta'), 'own_goals'] = 1
    df.loc[(df['round'] == 21) & (df['player_name'] == 'Roger Ibañez'), 'own_goals'] = 1
    df.loc[(df['round'] == 31) & (df['player_name'] == 'Samir Handanovič'), 'own_goals'] = 1
    df.loc[(df['round'] == 22) & (df['player_name'] == 'Alberto Grassi'), 'own_goals'] = 1
    df.loc[(df['round'] == 23) & (df['player_name'] == 'Robin Gosens'), 'own_goals'] = 1
    df.loc[(df['round'] == 18) & (df['player_name'] == 'Kamil Glik'), 'own_goals'] = 1
    df.loc[(df['round'] == 25) & (df['player_name'] == 'Daam Foulon'), 'own_goals'] = 1
    df.loc[(df['round'] == 31) & (df['player_name'] == 'Fabio Depaoli'), 'own_goals'] = 1
    df.loc[(df['round'] == 11) & (df['player_name'] == 'Bryan Cristante'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Vladi Chiricheş'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Giorgio Chiellini'), 'own_goals'] = 1
    df.loc[(df['round'] == 2) & (df['player_name'] == 'Federico Ceccherini'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Davide Calabria'), 'own_goals'] = 1
    df.loc[(df['round'] == 30) & (df['player_name'] == 'Federico Barba'), 'own_goals'] = 1
    df.loc[(df['round'] == 4) & (df['player_name'] == 'Simone Iacoponi'), 'own_goals'] = 1
    df.loc[(df['round'] == 26) & (df['player_name'] == 'Simone Iacoponi'), 'own_goals'] = 1

    df['own_goals'] = df['own_goals'].astype(int)

    return df

def own_goals_serie_a_2021(df):
    df['own_goals'] = 0
    df.loc[(df['round'] == 29) & (df['player_name'] == 'Maya Yoshida'), 'own_goals'] = 1
    df.loc[(df['round'] == 12) & (df['player_name'] == 'Stefan de Vrij'), 'own_goals'] = 1
    df.loc[(df['round'] == 8) & (df['player_name'] == 'Mattia Viti'), 'own_goals'] = 1
    df.loc[(df['round'] == 8) & (df['player_name'] == 'Lorenzo Venuti'), 'own_goals'] = 1
    df.loc[(df['round'] == 17) & (df['player_name'] == 'Zinho Vanheusden'), 'own_goals'] = 1
    df.loc[(df['round'] == 11) & (df['player_name'] == 'Lorenzo Tonelli'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Jens Stryger Larsen'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Stefan Strandberg'), 'own_goals'] = 1
    df.loc[(df['round'] == 17) & (df['player_name'] == 'Adama Soumaoro'), 'own_goals'] = 1
    df.loc[(df['round'] == 25) & (df['player_name'] == 'Chris Smalling'), 'own_goals'] = 1
    df.loc[(df['round'] == 10) & (df['player_name'] == 'Salvatore Sirigu'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Alex Sandro'), 'own_goals'] = 1
    df.loc[(df['round'] == 36) & (df['player_name'] == 'Simone Romagnoli'), 'own_goals'] = 1
    df.loc[(df['round'] == 35) & (df['player_name'] == 'Ivan Provedel'), 'own_goals'] = 1
    df.loc[(df['round'] == 32) & (df['player_name'] == 'Patric'), 'own_goals'] = 1
    df.loc[(df['round'] == 18) & (df['player_name'] == 'Dimitris Nikolaou'), 'own_goals'] = 1
    df.loc[(df['round'] == 18) & (df['player_name'] == 'Riccardo Marchizza'), 'own_goals'] = 1
    df.loc[(df['round'] == 33) & (df['player_name'] == 'Teun Koopmeiners'), 'own_goals'] = 1
    df.loc[(df['round'] == 14) & (df['player_name'] == 'Simon Kjær'), 'own_goals'] = 1
    df.loc[(df['round'] == 19) & (df['player_name'] == 'Juan Jesus'), 'own_goals'] = 1
    df.loc[(df['round'] == 4) & (df['player_name'] == 'Ivan Ilić'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Zlatan Ibrahimović'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Emmanuel Gyasi'), 'own_goals'] = 1
    df.loc[(df['round'] == 8) & (df['player_name'] == 'Koray Günter'), 'own_goals'] = 1
    df.loc[(df['round'] == 20) & (df['player_name'] == 'Remo Freuler'), 'own_goals'] = 1
    df.loc[(df['round'] == 12) & (df['player_name'] == 'Davide Frattesi'), 'own_goals'] = 1
    df.loc[(df['round'] == 18) & (df['player_name'] == 'Bryan Cristante'), 'own_goals'] = 1
    df.loc[(df['round'] == 21) & (df['player_name'] == 'Berat Djimsiti'), 'own_goals'] = 1
    df.loc[(df['round'] == 13) & (df['player_name'] == 'Francesco Di Tacchio'), 'own_goals'] = 1
    df.loc[(df['round'] == 16) & (df['player_name'] == 'Andrea Carboni'), 'own_goals'] = 1
    df.loc[(df['round'] == 6) & (df['player_name'] == 'Kevin Bonifazi'), 'own_goals'] = 1
    df.loc[(df['round'] == 24) & (df['player_name'] == 'Cristiano Biraghi'), 'own_goals'] = 1
    df.loc[(df['round'] == 10) & (df['player_name'] == 'Kristoffer Askildsen'), 'own_goals'] = 1
    df.loc[(df['round'] == 33) & (df['player_name'] == 'Ardian Ismajli'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Ardian Ismajli'), 'own_goals'] = 1
    df.loc[(df['round'] == 9) & (df['player_name'] == 'Thomas Henry'), 'own_goals'] = 1
    df.loc[(df['round'] == 16) & (df['player_name'] == 'Thomas Henry'), 'own_goals'] = 1

    df['own_goals'] = df['own_goals'].astype(int)

    return df

def own_goals_serie_a_2022(df):
    df['own_goals'] = 0
    df.loc[(df['round'] == 10) & (df['player_name'] == 'Miguel Veloso'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Milan Škriniar'), 'own_goals'] = 1
    df.loc[(df['round'] == 5) & (df['player_name'] == 'Jerdy Schouten'), 'own_goals'] = 1
    df.loc[(df['round'] == 35) & (df['player_name'] == 'Ruan'), 'own_goals'] = 1
    df.loc[(df['round'] == 22) & (df['player_name'] == 'Nehuén Pérez'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'André Onana'), 'own_goals'] = 1
    df.loc[(df['round'] == 24) & (df['player_name'] == 'Juan Musso'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Jeison Murillo'), 'own_goals'] = 1
    df.loc[(df['round'] == 15) & (df['player_name'] == 'Nikola Milenković'), 'own_goals'] = 1
    df.loc[(df['round'] == 5) & (df['player_name'] == 'Marlon'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Giangiacomo Magnani'), 'own_goals'] = 1
    df.loc[(df['round'] == 15) & (df['player_name'] == 'José Luis Palomino'), 'own_goals'] = 1
    df.loc[(df['round'] == 33) & (df['player_name'] == 'Jhon Lucumí'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Manuel Lazzari'), 'own_goals'] = 1
    df.loc[(df['round'] == 24) & (df['player_name'] == 'Ardian Ismajli'), 'own_goals'] = 1
    df.loc[(df['round'] == 22) & (df['player_name'] == 'Roger Ibañez'), 'own_goals'] = 1
    df.loc[(df['round'] == 18) & (df['player_name'] == 'Theo Hernández'), 'own_goals'] = 1
    df.loc[(df['round'] == 7) & (df['player_name'] == 'Joan González'), 'own_goals'] = 1
    df.loc[(df['round'] == 33) & (df['player_name'] == 'Adolfo Gaich'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Martin Erlić'), 'own_goals'] = 1
    df.loc[(df['round'] == 17) & (df['player_name'] == 'Denzel Dumfries'), 'own_goals'] = 1
    df.loc[(df['round'] == 11) & (df['player_name'] == 'Lorenzo De Silvestri'), 'own_goals'] = 1
    df.loc[(df['round'] == 19) & (df['player_name'] == 'Vlad Chiricheş'), 'own_goals'] = 1
    df.loc[(df['round'] == 28) & (df['player_name'] == 'Mattia Caldara'), 'own_goals'] = 1
    df.loc[(df['round'] == 31) & (df['player_name'] == 'Cristiano Biraghi'), 'own_goals'] = 1
    df.loc[(df['round'] == 20) & (df['player_name'] == 'Rodrigo Becão'), 'own_goals'] = 1
    df.loc[(df['round'] == 5) & (df['player_name'] == 'Emil Audero'), 'own_goals'] = 1
    df.loc[(df['round'] == 29) & (df['player_name'] == 'Przemysław Wiśniewski'), 'own_goals'] = 1
    df.loc[(df['round'] == 37) & (df['player_name'] == 'Przemysław Wiśniewski'), 'own_goals'] = 1
    df.loc[(df['round'] == 27) & (df['player_name'] == 'Antonino Gallo'), 'own_goals'] = 1
    df.loc[(df['round'] == 29) & (df['player_name'] == 'Antonino Gallo'), 'own_goals'] = 1


    df['own_goals'] = df['own_goals'].astype(int)

    return df

def own_goals_other_league(df):
    df['own_goals'] = 0
    df.loc[(df['round'] == 31) & (df['player_name'] == 'Andrea La Mantia'), 'own_goals'] = 1

    df['own_goals'] = df['own_goals'].astype(int)

    return df

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
    df['substitute'] = df['substitute'].apply(lambda x: 1 if x == 'True' else 0)

    #deleting columns
    columns_to_drop = ['updateAt', 'captain', 'event_timestamp',
                       'firstHalfStart', 'secondHalfStart',
                       'status','statusShort', 'elapsed',
                       'venue', 'referee', 'league_logo', 'league_flag',
                       'homeTeam_logo', 'awayTeam_logo', 'score_halftime',
                       'score_fulltime', 'score_extratime', 'score_penalty']
    df = df.drop(columns=columns_to_drop)


    return df.to_csv(f'league.csv', index=False)



def find_players_other_league_and_add_to_serie_a(year,csv_year_minus_2, csv_year_minus_1, csv_year, csv_other_league):
    df_year_minus_2 = pd.read_csv(csv_year_minus_2)
    league_year_minus_2 = df_year_minus_2.copy()
    if year == 2022:
        league_year_minus_2 = own_goals_serie_a_2020(league_year_minus_2)
    else:
        league_year_minus_2 == 0

    df_year_minus_1 = pd.read_csv(csv_year_minus_1)
    league_year_minus_1 = df_year_minus_1.copy()
    if year == 2022:
        league_year_minus_1 = own_goals_serie_a_2021(league_year_minus_1)
    elif year == 2021:
        league_year_minus_1 = own_goals_serie_a_2020(league_year_minus_1)
    else:
        league_year_minus_1 == 0

    df_year = pd.read_csv(csv_year)
    league_year = df_year.copy()
    if year == 2022:
        league_year = own_goals_serie_a_2022(league_year)
    elif year == 2021:
        league_year = own_goals_serie_a_2021(league_year)
    else:
        league_year == 0

    df_other_league = pd.read_csv(csv_other_league)
    other_league = df_other_league.copy()
    other_league = own_goals_other_league(other_league)

    serie_a_players_22 = csv_year.groupby('player_name')['round'].count().sort_values(ascending=False)
    serie_a_players_22_df = serie_a_players_22.reset_index(name='round_count_22')

    serie_a_players_21 = csv_year_minus_1.groupby('player_name')['round'].count().sort_values(ascending=False)
    serie_a_players_21_df = serie_a_players_21.reset_index(name='round_count_21')

    serie_a_players_20 = csv_year_minus_2.groupby('player_name')['round'].count().sort_values(ascending=False)
    serie_a_players_20_df = serie_a_players_20.reset_index(name='round_count_20')

    merged_df = pd.merge(serie_a_players_22_df, serie_a_players_21_df, on='player_name', how='outer')
    merged_df = pd.merge(merged_df, serie_a_players_20_df, on='player_name', how='outer')

    df_overview = merged_df.dropna(subset=['round_count_22'])

    df_overview.loc[:, '21_nan'] = df_overview['round_count_21'].isna()
    df_overview.loc[:, '20_nan'] = df_overview['round_count_20'].isna()
    df_overview['21_gap'] = df_overview['round_count_22'] - df_overview['round_count_21']
    df_overview['20_gap'] = df_overview['round_count_22'] - df_overview['round_count_20']

    filtered_rows = df_overview[df_overview['21_nan']]

    filtered_players_list = filtered_rows['player_name'].tolist()

    #iterates through the list and gives back the stats for games
    stacked_player_data = []

    for p in filtered_players_list:
        player_rows = other_league[other_league['player_name'] == p]
        if not player_rows.empty:
            stacked_player_data.append(player_rows)
        else:
            print(f"Player '{p}' not found")

    stacked_data = pd.concat(stacked_player_data)

    # Concatenate the existing data and the stacked data
    if year == 2022:
        combined_data = pd.concat([league_year, stacked_data], ignore_index=True)
    elif year == 2021:
        combined_data = pd.concat([league_year_minus_1, stacked_data], ignore_index=True)
    else:
        combined_data = pd.concat([league_year_minus_2, stacked_data], ignore_index=True)



    return combined_data.to_csv('new.csv', index=False)
