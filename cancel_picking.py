# -*- coding: utf-8 -*-
"""A discord bot to bet on PUGs."""

from enum import IntEnum
import yaml
import sqlite3


config = yaml.safe_load(open("config.yml"))
DATABASE = config['database']
GAME_STATUS = IntEnum('Game_Status', 'Picking Cancelled InProgress Team1 Team2 Tied')
WAGER_RESULT = IntEnum('Wager_Result', 'InProgress Won Lost Canceled')

# Main
if __name__ == '__main__':
    conn = sqlite3.connect(DATABASE)
    sql = ''' UPDATE games SET status = ? WHERE status = ? '''
    values = (GAME_STATUS.Cancelled, GAME_STATUS.Picking,)
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    conn.close()
