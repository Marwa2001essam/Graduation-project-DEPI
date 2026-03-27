import pandas as pd
import os

BASE = r'E:\رواد\power pi\fifa project\File 1\WorldCups'
OUT = r'E:\رواد\power pi\fifa project\File 1\data'
os.makedirs(OUT, exist_ok=True)

# ── Load raw files ──────────────────────────────────────────────────
matches  = pd.read_csv(f'{BASE}/matches.csv')
goals    = pd.read_csv(f'{BASE}/goals.csv')
stadiums = pd.read_csv(f'{BASE}/stadiums.csv')
team_app = pd.read_csv(f'{BASE}/team_appearances.csv')
host     = pd.read_csv(f'{BASE}/host_countries.csv')
bookings = pd.read_csv(f'{BASE}/bookings.csv')
players  = pd.read_csv(f'{BASE}/players.csv')

# ── dim_tournaments ─────────────────────────────────────────────────
tournaments = matches[['tournament_id','tournament_name']].drop_duplicates()
tournaments['year'] = tournaments['tournament_name'].str.extract(r'(\d{4})').astype(int)
tournaments['era'] = pd.cut(tournaments['year'],
    bins=[1929,1954,1974,1994,2014,2030],
    labels=['1930–1954','1954–1974','1974–1994','1994–2014','2014+'])
tournaments = tournaments.rename(columns={
    'tournament_id':'TournamentID','tournament_name':'TournamentName'})
tournaments.to_csv(f'{OUT}/dim_tournaments.csv', index=False)

# ── dim_teams ────────────────────────────────────────────────────────
teams = team_app[['team_id','team_name','team_code']].drop_duplicates().rename(
    columns={'team_id':'TeamID','team_name':'TeamName','team_code':'TeamCode'})

continent_map = {
    'Brazil':'South America','Argentina':'South America','Uruguay':'South America',
    'Colombia':'South America','Chile':'South America','Peru':'South America',
    'Germany':'Europe','Italy':'Europe','France':'Europe','Spain':'Europe',
    'England':'Europe','Netherlands':'Europe','Portugal':'Europe','Belgium':'Europe',
    'Sweden':'Europe','Croatia':'Europe','Hungary':'Europe','Poland':'Europe',
    'West Germany':'Europe','Romania':'Europe','Austria':'Europe','Switzerland':'Europe',
    'Denmark':'Europe','Serbia':'Europe','Soviet Union':'Europe','Russia':'Europe',
    'Mexico':'North America','United States':'North America','Costa Rica':'North America',
    'South Korea':'Asia','Japan':'Asia','Saudi Arabia':'Asia','Iran':'Asia',
    'Australia':'Oceania','New Zealand':'Oceania',
    'Senegal':'Africa','Nigeria':'Africa','Cameroon':'Africa','Ghana':'Africa',
    'Morocco':'Africa','South Africa':'Africa','Algeria':'Africa','Tunisia':'Africa',
}
teams['Continent'] = teams['TeamName'].map(continent_map).fillna('Other')
teams.to_csv(f'{OUT}/dim_teams.csv', index=False)

# ── dim_stadiums ─────────────────────────────────────────────────────
stadiums[['stadium_id','stadium_name','city_name','country_name','stadium_capacity']]\
    .rename(columns={
        'stadium_id':'StadiumID','stadium_name':'StadiumName',
        'city_name':'City','country_name':'Country',
        'stadium_capacity':'Capacity'})\
    .to_csv(f'{OUT}/dim_stadiums.csv', index=False)

# ── dim_players ──────────────────────────────────────────────────────
dim_players = players[['player_id','family_name','given_name','birth_date',
                        'goal_keeper','defender','midfielder','forward',
                        'count_tournaments']].copy()
dim_players['FullName'] = (
    dim_players['given_name'].str.replace('not applicable','').str.strip()
    + ' ' + dim_players['family_name'])
dim_players['Position'] = dim_players.apply(lambda r:
    'Goalkeeper' if r['goal_keeper'] else
    'Defender'   if r['defender']    else
    'Midfielder' if r['midfielder']  else
    'Forward'    if r['forward']     else 'Unknown', axis=1)
dim_players.rename(columns={
    'player_id':'PlayerID','family_name':'FamilyName',
    'given_name':'GivenName','birth_date':'BirthDate',
    'count_tournaments':'TournamentCount'})\
    [['PlayerID','FullName','FamilyName','GivenName',
      'BirthDate','Position','TournamentCount']]\
    .to_csv(f'{OUT}/dim_players.csv', index=False)

# ── dim_host_countries ───────────────────────────────────────────────
dim_hosts = host[['tournament_id','team_id','team_name','performance']].rename(
    columns={'tournament_id':'TournamentID','team_id':'TeamID',
             'team_name':'HostCountry','performance':'Performance'})
dim_hosts['IsChampion'] = dim_hosts['Performance'].str.lower()\
    .str.contains('champion').astype(int)
dim_hosts.to_csv(f'{OUT}/dim_host_countries.csv', index=False)

# ── fact_matches ─────────────────────────────────────────────────────
fact_matches = matches[[
    'match_id','tournament_id','stadium_id','match_date','match_time',
    'home_team_id','home_team_name','away_team_id','away_team_name',
    'stage_name','group_name','group_stage','knockout_stage',
    'home_team_score','away_team_score','result',
    'home_team_win','away_team_win','draw',
    'extra_time','penalty_shootout',
    'home_team_score_penalties','away_team_score_penalties'
]].rename(columns={
    'match_id':'MatchID','tournament_id':'TournamentID','stadium_id':'StadiumID',
    'match_date':'MatchDate','match_time':'MatchTime',
    'home_team_id':'HomeTeamID','home_team_name':'HomeTeamName',
    'away_team_id':'AwayTeamID','away_team_name':'AwayTeamName',
    'stage_name':'Stage','group_name':'GroupName',
    'group_stage':'IsGroupStage','knockout_stage':'IsKnockout',
    'home_team_score':'HomeScore','away_team_score':'AwayScore',
    'result':'Result','home_team_win':'HomeWin','away_team_win':'AwayWin',
    'draw':'Draw','extra_time':'ExtraTime','penalty_shootout':'PenaltyShootout',
    'home_team_score_penalties':'HomePenalties',
    'away_team_score_penalties':'AwayPenalties'
}).copy()
fact_matches['TotalGoals'] = fact_matches['HomeScore'] + fact_matches['AwayScore']
fact_matches['MatchDate'] = pd.to_datetime(fact_matches['MatchDate'])
fact_matches.to_csv(f'{OUT}/fact_matches.csv', index=False)

# ── fact_goals ───────────────────────────────────────────────────────
fact_goals = goals[[
    'goal_id','match_id','tournament_id','player_id','team_id',
    'stage_name','group_name','match_date',
    'minute_regulation','minute_stoppage','match_period',
    'own_goal','penalty'
]].rename(columns={
    'goal_id':'GoalID','match_id':'MatchID','tournament_id':'TournamentID',
    'player_id':'PlayerID','team_id':'TeamID',
    'stage_name':'Stage','group_name':'GroupName','match_date':'MatchDate',
    'minute_regulation':'Minute','minute_stoppage':'StoppageMinute',
    'match_period':'Period','own_goal':'OwnGoal','penalty':'IsPenalty'
}).copy()
fact_goals['MatchDate'] = pd.to_datetime(fact_goals['MatchDate'])
fact_goals['GoalType'] = fact_goals.apply(lambda r:
    'Own Goal'  if r['OwnGoal']    else
    'Penalty'   if r['IsPenalty']  else 'Open Play', axis=1)
fact_goals.to_csv(f'{OUT}/fact_goals.csv', index=False)

# ── fact_team_appearances ────────────────────────────────────────────
fact_team_app = team_app[[
    'match_id','tournament_id','team_id','stage_name','group_name','match_date',
    'goals_for','goals_against','goal_differential','result',
    'win','lose','draw','extra_time','penalty_shootout',
    'penalties_for','penalties_against','home_team','away_team'
]].rename(columns={
    'match_id':'MatchID','tournament_id':'TournamentID','team_id':'TeamID',
    'stage_name':'Stage','group_name':'GroupName','match_date':'MatchDate',
    'goals_for':'GoalsFor','goals_against':'GoalsAgainst',
    'goal_differential':'GoalDiff','result':'Result',
    'win':'Win','lose':'Loss','draw':'Draw',
    'extra_time':'ExtraTime','penalty_shootout':'PenaltyShootout',
    'penalties_for':'PenaltiesFor','penalties_against':'PenaltiesAgainst',
    'home_team':'IsHome','away_team':'IsAway'
}).copy()
fact_team_app['MatchDate'] = pd.to_datetime(fact_team_app['MatchDate'])
fact_team_app.to_csv(f'{OUT}/fact_team_appearances.csv', index=False)

# ── fact_bookings ────────────────────────────────────────────────────
fact_bookings = bookings[[
    'booking_id','match_id','tournament_id','player_id','team_id',
    'stage_name','match_date','minute_regulation','match_period',
    'yellow_card','red_card','second_yellow_card','sending_off'
]].rename(columns={
    'booking_id':'BookingID','match_id':'MatchID','tournament_id':'TournamentID',
    'player_id':'PlayerID','team_id':'TeamID',
    'stage_name':'Stage','match_date':'MatchDate',
    'minute_regulation':'Minute','match_period':'Period',
    'yellow_card':'YellowCard','red_card':'RedCard',
    'second_yellow_card':'SecondYellow','sending_off':'SendingOff'
}).copy()
fact_bookings['MatchDate'] = pd.to_datetime(fact_bookings['MatchDate'])
fact_bookings['CardType'] = fact_bookings.apply(lambda r:
    'Red Card'      if r['RedCard']      else
    'Second Yellow' if r['SecondYellow'] else 'Yellow Card', axis=1)
fact_bookings.to_csv(f'{OUT}/fact_bookings.csv', index=False)

print("✅ Done — 9 CSVs written to", OUT)