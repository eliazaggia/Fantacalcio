import pandas as pd


def calculate_score(row):

    #minutes played
    if row['minutes_played'] >= 77:
        minutes_played_score = 5
    elif row['minutes_played'] >= 10 and row['minutes_played'] < 77:
        minutes_played_score = 2
    else:
        minutes_played_score = 0

    #starting 11
    if row['substitute'] == 1:
        substitute_score = 25
    else:
        substitute_score = 35

    #cards
    yellow_card_score = row['cards_yellow'] * (-3)
    red_card_score = row['cards_red'] * (-6) if row['cards_yellow'] != 2 else 0

    #goal conceded
    if row['position_G'] == 1:
        if row['goals_conceded'] > 3:
            goals_conceded_score = (row['goals_conceded'] * (-3)) - 3
        else:
            goals_conceded_score = row['goals_conceded'] * (-3)
    elif row['position_D'] == 1:
        goals_conceded_score = row['goals_conceded'] * (-4)
    elif row['position_M'] == 1:
        goals_conceded_score = row['goals_conceded'] * (-2)
    else:
        goals_conceded_score = 0

    #clean sheet
    if row['position_G'] == 1:
        if row['goals_conceded'] == 0:
            clean_sheet_score = 20
        else:
            clean_sheet_score = 0
    elif row['position_D'] == 1:
        if row['goals_conceded'] == 0:
            clean_sheet_score = 10
        else:
            clean_sheet_score = 0
    else:
        clean_sheet_score = 0

    #penalties
    penalty_missed_score = row['penalty_missed'] * (-5)
    penalty_won_score = row['penalty_won'] * 2
    penalty_commited_score = row['penalty_commited'] * (-3)
    penalty_success_score = row['penalty_success'] * 5
    penalty_saved_score =  row['penalty_saved'] * 10

    #goals not as penalty
    if row['penalty_success'] > 0:
        goals_total_score = (row['goals_total'] * 10) - (row['penalty_success']*10)
    else:
        goals_total_score = row['goals_total'] * 10

    #fouls
    ## player was fouled
    if row['position_M'] == 1:
        fouls_drawn_score = row['fouls_drawn'] * 1
    elif row['position_F'] == 1:
        fouls_drawn_score = row['fouls_drawn'] * 1
    else:
        fouls_drawn_score = 0

    ##players' foul
    if row['position_G'] == 1:
        fouls_committed_score = row['fouls_committed'] * (-1)
    elif row['position_D'] == 1:
        fouls_committed_score = row['fouls_committed'] * (-2)
    elif row['position_M'] == 1:
        fouls_committed_score = row['fouls_committed'] * (-1)
    elif row['position_F'] == 1:
        fouls_committed_score = row['fouls_committed'] * (-0.5)
    else:
        fouls_committed_score = 0

    #shots
    shots_total_score = row['shots_total'] * 1.5
    shots_on_score = row['shots_on'] * 3

    #assist
    goals_assists_score = row['goals_assists'] * 3

    #passes
    passes_key_score = row['passes_key'] * 2

    if row['passes_total'] != 0:
        if row['passes_accuracy'] / row['passes_total'] >= 0.8:
            passes_accuracy_score = 3
        elif row['passes_accuracy'] / row['passes_total'] < 0.8 and row['passes_accuracy'] / row['passes_total'] >= 0.5:
            passes_accuracy_score = 1
        elif row['passes_accuracy'] / row['passes_total'] < 0.5 and row['passes_accuracy'] / row['passes_total'] >= 0.3:
            passes_accuracy_score = -1
        else:
            passes_accuracy_score = -3
    else:
       passes_accuracy_score = 0

    #goals saved
    goals_saves_score = row['goals_saves'] * 2

    #offside
    offsides_score = row['offsides'] * (-0.3)

    #tackles
    tackles_blocks_score = row['tackles_blocks'] * 3
    tackles_interceptions_score = row['tackles_interceptions'] * 3

    #duels
    ## duels lost
    if row['position_D'] == 1:
        duels_lost_score = (row['duels_total'] - row['duels_won']) * (-2)
    elif row['position_M'] == 1:
        duels_lost_score = (row['duels_total'] - row['duels_won']) * (-0.8)
    elif row['position_F'] == 1:
        duels_lost_score = (row['duels_total'] - row['duels_won']) * (-1)
    else:
        duels_lost_score = 0

    ##duels won
    if row['position_D'] == 1:
        duels_won_score = row['duels_won'] * 1.5
    elif row['position_M'] == 1:
        duels_won_score = row['duels_won'] * 0.8
    elif row['position_F'] == 1:
        duels_won_score = row['duels_won'] * 1
    else:
        duels_won_score = 0

    #dribbles
    ## player got dribbled
    if row['position_D'] == 1:
        dribbles_past_score = row['dribbles_past'] * (-2)
    elif row['position_M'] == 1:
        dribbles_past_score = row['dribbles_past'] * (-0.8)
    elif row['position_F'] == 1:
        dribbles_past_score = row['dribbles_past'] * (-1)
    else:
        dribbles_past_score = 0

    ##dribbles won
    if row['position_D'] == 1:
        dribbles_success_score = row['dribbles_success'] * 1.5
    elif row['position_M'] == 1:
        dribbles_success_score = row['dribbles_success'] * 0.8
    elif row['position_F'] == 1:
        dribbles_success_score = row['dribbles_success'] * 1
    else:
        dribbles_success_score = 0

    #own goals
    if row['position_G'] == 1:
        own_goals_score = row['own_goals'] * (-5)
    else:
        own_goals_score = row['own_goals'] * (-10)

    #rating
    rating_score = row['rating']


    total_score =  yellow_card_score + red_card_score + goals_conceded_score + penalty_won_score + penalty_success_score + penalty_commited_score + penalty_missed_score + penalty_saved_score + goals_total_score + fouls_drawn_score + fouls_committed_score + fouls_drawn_score + shots_on_score + shots_total_score + goals_assists_score + goals_saves_score + offsides_score + passes_accuracy_score + passes_key_score + tackles_blocks_score + tackles_interceptions_score + duels_won_score + duels_lost_score + dribbles_past_score + dribbles_success_score + minutes_played_score + substitute_score + rating_score + clean_sheet_score + own_goals_score

    return total_score

def apply_score(df):
    # Apply the function to calculate the total score for each player
    df['total_score'] = df.apply(calculate_score, axis=1)
