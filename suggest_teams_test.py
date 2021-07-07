# -*- coding: utf-8 -*-
"""Testing the suggestion of teams based on TrueSkill."""

from itertools import combinations
from random import sample

import yaml
import sqlite3
from trueskill import Rating, quality

config = yaml.safe_load(open("config.yml"))
DATABASE = config['database']

conn = sqlite3.connect(DATABASE)

cur = conn.cursor()
sql = ''' SELECT DISTINCT discord_id FROM trueskills '''
cur.execute(sql)
data = cur.fetchall()

discord_ids = [i[0] for i in data]
player_ids = sample(discord_ids, 10)

player_ratings = {}
for player_id in player_ids:
    sql = ''' SELECT mu, sigma FROM trueskills WHERE discord_id = ? AND game_id IN ( SELECT MAX(game_id) FROM trueskills
              WHERE discord_id = ? ) '''
    values = (player_id, player_id)
    cur.execute(sql, values)
    data = cur.fetchone()
    if data:
        player_ratings[player_id] = Rating(data[0], data[1])
    else:
        player_ratings[player_id] = Rating()

best_team1_ids = []
best_team2_ids = []
best_chance_to_draw = 0

for c in combinations(player_ids, 5):
    team1_ids = list(c)
    team2_ids = [x for x in player_ids if x not in team1_ids]
    team1_rating = [player_ratings[i] for i in team1_ids]
    team2_rating = [player_ratings[i] for i in team2_ids]
    chance_to_draw = quality([team1_rating, team2_rating])
    if chance_to_draw > best_chance_to_draw:
        best_team1_ids = team1_ids
        best_team2_ids = team2_ids
        best_chance_to_draw = chance_to_draw

team1_str = '<@!' + '>, <@!'.join([str(i) for i in best_team1_ids]) + '>'
team2_str = '<@!' + '>, <@!'.join([str(i) for i in best_team2_ids]) + '>'

print(f'Suggested teams: {team1_str} versus {team2_str} ({best_chance_to_draw:.1%} chance to draw).')
