# -*- coding: utf-8 -*-
"""Calculate TrueSkill of players."""


from enum import IntEnum, auto
import yaml
import sqlite3
from trueskill import Rating, rate, quality, backends

FIRST_GAME_ID = 495


class GameStatus(IntEnum):
    PICKING = auto()
    CANCELLED = auto()
    INPROGRESS = auto()
    TEAM1 = auto()
    TEAM2 = auto()
    TIED = auto()


config = yaml.safe_load(open("../config.yml"))
DATABASE = config['database']

conn = sqlite3.connect(DATABASE)

cur = conn.cursor()
sql = ''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='trueskills' '''
cur.execute(sql)
if cur.fetchone()[0] == 1:
    print('TueSkills table found. Continuing will erase the table.')
    input("Press Enter to continue...")
    conn.execute(''' DROP TABLE trueskills ''')
conn.execute("""
    CREATE TABLE IF NOT EXISTS trueskills (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        discord_id INTEGER NOT NULL,
        game_id INT NOT NULL,
        mu FLOAT NOT NULL,
        sigma FLOAT NOT NULL,
        trueskill FLOAT NOT NULL
    );
""")

player_ratings = {}
number_of_games = {}
backends.choose_backend('scipy')
values = (FIRST_GAME_ID, GameStatus.TEAM1, GameStatus.TEAM2, GameStatus.TIED)
sql = ''' SELECT id, team1, team2, status 
          FROM games 
          WHERE id > ? 
          AND (queue = 'NA' OR queue = 'EU' OR queue = 'AU') 
          AND (status = ? OR status = ? or status = ?) '''
cur = conn.cursor()
cur.execute(sql, values)
games = cur.fetchall()
for game in games:
    game_id: int = game[0]
    team1_str: str = game[1]
    team2_str: str = game[2]
    status: int = game[3]
    if len(team1_str.split()) == 5 or len(team2_str.split()) == 5:
        team1_skills = []
        for player in team1_str.split():
            if player not in player_ratings:
                player_ratings[player] = Rating()
            team1_skills.append(player_ratings[player])
        team2_skills = []
        for player in team2_str.split():
            if player not in player_ratings:
                player_ratings[player] = Rating()
            team2_skills.append(player_ratings[player])
        ranks = [0, 0]
        if status == GameStatus.TEAM1:
            ranks = [0, 1]
        elif status == GameStatus.TEAM2:
            ranks = [1, 0]
        draw_chance = quality([team1_skills, team2_skills])
        print(f'id: {game_id}, chance to draw: {draw_chance:.2f}, result: {GameStatus(status).name}.')
        (new_team1_skills, new_team2_skills) = rate([team1_skills, team2_skills], ranks)
        for idx, player in enumerate(team1_str.split()):
            rating = new_team1_skills[idx]
            player_ratings[player] = rating
            trueskill_update = (player, game_id, rating.mu, rating.sigma, rating.exposure)
            sql = ''' INSERT INTO trueskills(discord_id, game_id, mu, sigma, trueskill)
                      VALUES(?, ?, ?, ?, ?) '''
            cur = conn.cursor()
            cur.execute(sql, trueskill_update)
        for idx, player in enumerate(team2_str.split()):
            rating = new_team2_skills[idx]
            player_ratings[player] = rating
            trueskill_update = (player, game_id, rating.mu, rating.sigma, rating.exposure)
            sql = ''' INSERT INTO trueskills(discord_id, game_id, mu, sigma, trueskill)
                      VALUES(?, ?, ?, ?, ?) '''
            cur = conn.cursor()
            cur.execute(sql, trueskill_update)
sql = ''' SELECT nick FROM users WHERE discord_id = ? '''
for player in player_ratings:
    player_nick = player
    cur = conn.cursor()
    cur.execute(sql, (player,))
    if user := cur.fetchone():
        player_nick = user[0]
    rating = player_ratings[player]
    print(f'player: {player_nick}, mu: {rating.mu:.2f}, sigma: {rating.sigma:.2f}, '
          f'trueskill: {rating.exposure:.2f}.')
print(f'number of games analyzed: {len(games)}')
conn.commit()
conn.close()
