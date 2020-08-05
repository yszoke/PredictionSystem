import sqlite3
import pandas as pd
import numpy as np
import featuretools as ft
import statistics as stats
import math
import datetime
from datetime import timedelta

def main():
    ### INIT TABLES(SQL)
    conn=createConnection("database.sqlite");
    tables=getAllTables(conn);
    countries=getAllCountries(conn);
    leagues=getAllLeagues(conn);
    teams=getAllTeams(conn);
    teamsAtt=getTeamsAtt(conn);
    matches=getAllMatches(conn);
    players=getAllPlayers(conn);
    player_attributes = getPlayerattributes(conn);

    print( "step 1")
    ### ADD TEAM'S DIFFRENCE DAYS BEFORE LAST GAME
    for index, row in matches.iterrows():
        homeLastGame=getLastGame(matches,row["home_team_api_id"],row["date"])
        awayLastGame=getLastGame(matches,row["away_team_api_id"],row["date"])
        diffDays=homeLastGame-awayLastGame
        matches.loc[index, 'diffrenceDaysBeforeLastGame'] = diffDays

    ### ADD TEAM'S DIFFERENCE NUMBER OF WINS
    for index, row in matches.iterrows():
        homeWins=get_wins(matches,row["home_team_api_id"],row["date"])
        awayWins=get_wins(matches,row["away_team_api_id"],row["date"])
        diffrence=homeWins-awayWins
        matches.loc[index, 'diffrenceWins'] = diffrence

    print( "step 2")

    ### ADD TEAM'S DIFFERENCE GOALS
    for index, row in matches.iterrows():
        homegoals = get_goals(matches, row["home_team_api_id"],row["date"])
        homegoalsCon = get_goals_conceided(matches, row["home_team_api_id"],row["date"])
        awaygoals = get_goals(matches, row["away_team_api_id"],row["date"])
        awaygoalsCon = get_goals_conceided(matches, row["away_team_api_id"],row["date"])
        diffrencegoals = (homegoals - homegoalsCon)-(awaygoals-awaygoalsCon)
        matches.loc[index, 'diffrenceGoals'] = diffrencegoals
    print( "step 3")

    ### ADD TEAM'S SPEED AND PASS
    teamsAtt_kept_columns = ["team_api_id", "date", "buildUpPlaySpeed", "buildUpPlayPassing","chanceCreationShooting","defencePressure","defenceAggression"]
    teamsAtt = teamsAtt[teamsAtt_kept_columns]
    for index, row in matches.iterrows():
        matches = addSpeedToHomeTeam(matches, row.home_team_api_id, index, row.date, teamsAtt)
        matches = addSpeedToAwayTeam(matches, row.away_team_api_id, index, row.date, teamsAtt)
        matches = addPassToHomeTeam(matches, row.home_team_api_id, index, row.date, teamsAtt)
        matches = addPassToAwayTeam(matches, row.away_team_api_id, index, row.date, teamsAtt)
        matches = addChanceToHomeTeam(matches, row.home_team_api_id, index, row.date, teamsAtt)
        matches = addChanceToAwayTeam(matches, row.away_team_api_id, index, row.date, teamsAtt)
        matches = addDefPressToHomeTeam(matches, row.home_team_api_id, index, row.date, teamsAtt)
        matches = addDefPressToAwayTeam(matches, row.away_team_api_id, index, row.date, teamsAtt)
        matches = addDefAggToHomeTeam(matches, row.home_team_api_id, index, row.date, teamsAtt)
        matches = addDefAggToAwayTeam(matches, row.away_team_api_id, index, row.date, teamsAtt)
    matches['speed_difference'] = matches['homeTeamSpeed'] - matches['awayTeamSpeed']
    matches['pass_difference'] = matches['homeTeamPass'] - matches['awayTeamPass']
    matches['chance_creations_difference'] = matches['homeTeamChance'] - matches['awayTeamChance']
    matches['defence_pressure_difference'] = matches['homeTeamDefPress'] - matches['awayTeamDefPress']
    matches['defence_aggression_difference'] = matches['homeTeamDefAgg'] - matches['awayTeamDefAgg']

    print( "step 4")

    ### ADD PLAYERS ATTRIBUTE: overall_rating AND potential
    matches=addPlayersAttribute(matches,player_attributes)

    print( "step 5")

    ### SET GOAL-DIFFRENCE
    matches['goal_difference'] = matches['home_team_goal'] - matches['away_team_goal']
    matches['goal_difference'] = matches['home_team_goal'] - matches['away_team_goal']
    matches['home_status'] = 'D'
    matches['home_status'] = np.where(matches['goal_difference'] > 0, 'W', matches['home_status'])
    matches['home_status'] = np.where(matches['goal_difference'] < 0, 'L', matches['home_status'])

    print( "step 6")

    ### SET COLLUMN AND DROP ALL THE REST
    matches_kept_columns = ["id", "season","speed_difference","pass_difference","diffrenceWins","diffrenceGoals", "overall_rating_difference",
                            "potential_difference","chance_creations_difference","defence_pressure_difference","defence_aggression_difference",
                            "goal_difference","diffrenceDaysBeforeLastGame","home_status"]
    matches=matches[matches_kept_columns]

    ### FILL MISSING VALUES
    matches['speed_difference'].fillna(matches['speed_difference'].median(), inplace=True)
    matches['pass_difference'].fillna(matches['pass_difference'].median(), inplace=True)
    matches['chance_creations_difference'].fillna(matches['chance_creations_difference'].median(), inplace=True)
    matches['defence_pressure_difference'].fillna(matches['defence_pressure_difference'].median(), inplace=True)
    matches['defence_aggression_difference'].fillna(matches['defence_aggression_difference'].median(), inplace=True)

    ### SPLIT DATA TO TRAIN AND TEST
    train = pd.DataFrame(columns=["id", "season","speed_difference","pass_difference","diffrenceWins","diffrenceGoals","overall_rating_difference","potential_difference",
                                  "chance_creations_difference","defence_pressure_difference","defence_aggression_difference","goal_difference",
                                  "diffrenceDaysBeforeLastGame","home_status"])
    test = pd.DataFrame(columns=["id", "season","speed_difference","pass_difference","diffrenceWins","diffrenceGoals","overall_rating_difference","potential_difference",
                                 "chance_creations_difference","defence_pressure_difference","defence_aggression_difference","goal_difference",
                                 "diffrenceDaysBeforeLastGame","home_status"])
    train,test=splitData(matches,train,test,"2015/2016")
    print( "step 7")

    ### WRITE DATAFRAMES TO FILE
    train.to_csv("train.csv", index=False)
    test.to_csv("test.csv", index=False)


### FUNCTIONS


### CREATE CONNECTION
def createConnection(path):
    conn = sqlite3.connect(path);
    return conn

### GET ALL THE TABLES FROM THE SQL FILE
def getAllTables(conn):
    tables = pd.read_sql("""SELECT *
                        FROM sqlite_master
                        WHERE type='table';""", conn)
    return tables

### GET ALL COUNTRIES
def getAllCountries(conn):
    countries = pd.read_sql("""SELECT *
                        FROM Country;""", conn)
    return countries

### GET ALL LEAGUES
def getAllLeagues(conn):
    leagues = pd.read_sql("""SELECT *
                        FROM League
                        JOIN Country ON Country.id = League.country_id;""", conn)
    return leagues

### GET ALL TEAMS
def getAllTeams(conn):
    teams = pd.read_sql("""SELECT *
                        FROM Team
                        ORDER BY team_long_name
                        """, conn)
    return teams

### GET ALL TEAM'S ATTRIBUTE
def getTeamsAtt(conn):
    teamsAtt = pd.read_sql("""SELECT *
                        FROM Team_Attributes
                        """, conn)
    return teamsAtt

### GET ALL MATCHES
def getAllMatches(conn):
    detailed_matches = pd.read_sql("""SELECT * from MATCH""", conn)
    return detailed_matches

### GET ALL PLAYERS
def getAllPlayers(conn):
    detailed_matches = pd.read_sql("""SELECT * from PLAYER""", conn)
    return detailed_matches

### GET ALL PLAYER_ATTRIBUTE
def getPlayerattributes(conn):
    detailed_matches = pd.read_sql("""SELECT * from PLAYER_ATTRIBUTES""", conn)
    return detailed_matches

### ADD TEAM'S SPEED TO HOME TEAM
def addSpeedToHomeTeam(matches,home_team_api_id,matches_index,date,teamsAtt):
    speed = teamsAtt.loc[(teamsAtt['team_api_id'] == home_team_api_id)]
    if speed.buildUpPlaySpeed.array:
        matches.loc[matches_index, 'homeTeamSpeed'] = speed.buildUpPlaySpeed.array[-1]
    return matches

### ADD TEAM'S SPEED TO AWAY TEAM
def addSpeedToAwayTeam(matches,away_team_api_id,matches_index,date,teamsAtt):
    speed = teamsAtt.loc[(teamsAtt['team_api_id'] == away_team_api_id)]
    if speed.buildUpPlaySpeed.array:
        matches.loc[matches_index, 'awayTeamSpeed'] = speed.buildUpPlaySpeed.array[-1]
    return matches

### ADD TEAM'S PASS TO HOME TEAM
def addPassToHomeTeam(matches,home_team_api_id,matches_index,date,teamsAtt):
    pass_ = teamsAtt.loc[(teamsAtt['team_api_id'] == home_team_api_id)]
    if pass_.buildUpPlayPassing.array:
        matches.loc[matches_index, 'homeTeamPass'] = pass_.buildUpPlayPassing.array[-1]
    return matches

### ADD TEAM'S PASS TO AWAY TEAM
def addPassToAwayTeam(matches,away_team_api_id,matches_index,date,teamsAtt):
    pass_ = teamsAtt.loc[(teamsAtt['team_api_id'] == away_team_api_id)]
    if pass_.buildUpPlayPassing.array:
        matches.loc[matches_index, 'awayTeamPass'] = pass_.buildUpPlayPassing.array[-1]
    return matches

### ADD TEAM'S CHANCE CREATION SHOOTING TO HOME TEAM
def addChanceToHomeTeam(matches,home_team_api_id,matches_index,date,teamsAtt):
    chance = teamsAtt.loc[(teamsAtt['team_api_id'] == home_team_api_id)]
    if chance.chanceCreationShooting.array:
        matches.loc[matches_index, 'homeTeamChance'] = chance.chanceCreationShooting.array[-1]
    return matches

### ADD TEAM'S CHANCE CREATION SHOOTING TO AWAY TEAM
def addChanceToAwayTeam(matches,away_team_api_id,matches_index,date,teamsAtt):
    chance = teamsAtt.loc[(teamsAtt['team_api_id'] == away_team_api_id)]
    if chance.chanceCreationShooting.array:
        matches.loc[matches_index, 'awayTeamChance'] = chance.chanceCreationShooting.array[-1]
    return matches

### ADD TEAM'S DEFENCE PRESSURE TO HOME TEAM
def addDefPressToHomeTeam(matches,home_team_api_id,matches_index,date,teamsAtt):
    pressure = teamsAtt.loc[(teamsAtt['team_api_id'] == home_team_api_id)]
    if pressure.defencePressure.array:
        matches.loc[matches_index, 'homeTeamDefPress'] = pressure.defencePressure.array[-1]
    return matches

### ADD TEAM'S DEFENCE PRESSURE TO AWAY TEAM
def addDefPressToAwayTeam(matches,away_team_api_id,matches_index,date,teamsAtt):
    pressure = teamsAtt.loc[(teamsAtt['team_api_id'] == away_team_api_id)]
    if pressure.defencePressure.array:
        matches.loc[matches_index, 'awayTeamDefPress'] = pressure.defencePressure.array[-1]
    return matches

### ADD TEAM'S DEFENCE AGGREGATION TO HOME TEAM
def addDefAggToHomeTeam(matches,home_team_api_id,matches_index,date,teamsAtt):
    aggregation = teamsAtt.loc[(teamsAtt['team_api_id'] == home_team_api_id)]
    if aggregation.defenceAggression.array:
        matches.loc[matches_index, 'homeTeamDefAgg'] = aggregation.defenceAggression.array[-1]
    return matches

### ADD TEAM'S DEFENCE AGGREGATION TO AWAY TEAM
def addDefAggToAwayTeam(matches,away_team_api_id,matches_index,date,teamsAtt):
    aggregation = teamsAtt.loc[(teamsAtt['team_api_id'] == away_team_api_id)]
    if aggregation.defenceAggression.array:
        matches.loc[matches_index, 'awayTeamDefAgg'] = aggregation.defenceAggression.array[-1]
    return matches

### ADD HOME TEAM PLAYERS
def addPlayersHomeTeam(matches, home_team, index, player_attributes):

    players=player_attributes.loc[(player_attributes['player_api_id'] == matches['home_player_X1'])]

### SPLIT DATA FRAME TO TRAIN AND TEST BY YEAR
def splitData(matches,train,test,year):
    trainTest = []
    testTest = []
    for index, row in matches.iterrows():
        if row.season == year:
            testTest.append(row)
        else:
            trainTest.append(row)
    train = pd.DataFrame(trainTest)
    test = pd.DataFrame(testTest)
    return train,test

### Get the number of wins of a specfic team from a set of matches.
def get_wins(matches, team, date):
    # Find home and away wins
    home_wins = int(matches.home_team_goal[
                        (matches.home_team_api_id == team) & (matches.home_team_goal > matches.away_team_goal) & (matches.date<date)].count())
    away_wins = int(matches.away_team_goal[
                        (matches.away_team_api_id == team) & (matches.away_team_goal > matches.home_team_goal) & (matches.date<date)].count())

    total_wins = home_wins + away_wins

    return total_wins

### Get the goals of a specfic team from a set of matches.
def get_goals(matches, team, date):

    # Find home and away goals
    home_goals = int(matches.home_team_goal[(matches.home_team_api_id == team) & (matches.date<date)].sum())
    away_goals = int(matches.away_team_goal[(matches.away_team_api_id == team) & (matches.date<date)].sum())

    total_goals = home_goals + away_goals

    # Return total goals
    return total_goals

 ### Get the goals conceided of a specfic team from a set of matches.
def get_goals_conceided(matches, team, date):

    # Find home and away goals
    home_goals = int(matches.home_team_goal[(matches.away_team_api_id == team) & (matches.date<date)].sum())
    away_goals = int(matches.away_team_goal[(matches.home_team_api_id == team) & (matches.date<date)].sum())

    total_goals = home_goals + away_goals

    # Return total goals
    return total_goals

### ADD PLAYERS ATTRIBUTE: overall_rating AND potential
def addPlayersAttribute(matches,player_attributes):
    home_players = ["home_player_" + str(x) for x in range(1, 12)]
    away_players = ["away_player_" + str(x) for x in range(1, 12)]

    for player in home_players:
        matches = pd.merge(matches, player_attributes[["id", "overall_rating", "potential"]], left_on=[player],
                           right_on=["id"], how='left',
                           suffixes=["", "_" + player])
    for player in away_players:
        matches = pd.merge(matches, player_attributes[["id", "overall_rating", "potential"]], left_on=[player],
                           right_on=["id"], how='left',
                           suffixes=["", "_" + player])

    matches = matches.rename(columns={"overall_rating": "overall_rating_home_player_1"})

    matches['overall_rating_home'] = matches[['overall_rating_' + p for p in home_players]].sum(axis=1)
    matches['overall_rating_away'] = matches[['overall_rating_' + p for p in away_players]].sum(axis=1)
    matches['overall_rating_difference'] = matches['overall_rating_home'] - matches['overall_rating_away']

    matches = matches.rename(columns={"potential": "potential_home_player_1"})

    matches['potential_home'] = matches[['potential_' + p for p in home_players]].sum(axis=1)
    matches['potential_away'] = matches[['potential_' + p for p in away_players]].sum(axis=1)
    matches['potential_difference'] = matches['potential_home'] - matches['potential_away']
    return matches

### GET THE NUMBER OF DAYS SINCE THE LAST GAME OF TEAM
def getLastGame(matches,team,date):
    days = (matches.date[
        ((matches.home_team_api_id == team) | (matches.away_team_api_id == team)) & (
                                    matches.date < date)])
    if days.values.size>0:
        closestDate = max(days)
        date1=pd.to_datetime(date, format='%Y-%m-%d 00:00:00')
        date2=pd.to_datetime(closestDate, format='%Y-%m-%d 00:00:00')
        diff=(date1-date2).days
    else:
        diff=0
    return diff


### START BUILD THE DATA
main();