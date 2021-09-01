# -*- coding: utf-8 -*-
"""Fix non-integer balances"""
from math import ceil

import yaml
import sqlite3

config = yaml.safe_load(open("config.yml"))
DATABASE: str = config['database']
DISCORD_ID: int = config['discord_id']


def change_balance(conn, user_id, balance_change) -> None:
    """Change the balance of a user

    :param sqlite3.Connection conn: The connection to the database
    :param int user_id: The id of the user whose balance needs updating
    :param int balance_change: The amount the balance needs to change
    """
    values = (balance_change, user_id)
    sql = ''' UPDATE users SET balance = balance + ? WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def create_transfer(conn, transfer) -> int:
    """Create a new transfer into the transfers table and update the balances

    :param sqlite3.Connection conn:Connection to the database
    :param tuple(int,int,int) transfer: Tuple of the user_id of the sender, user_id of the receiver and the amount
        to be transferred
    :return: The id of the transfer or 0 if an error occurred
    """
    sql = ''' INSERT INTO transfers(sender, receiver, amount, transfer_time)
              VALUES(?, ?, ?, strftime('%s','now')) '''
    cur = conn.cursor()
    cur.execute(sql, transfer)
    conn.commit()
    if (change_balance(conn, transfer[0], -transfer[2]) == 0 or
            change_balance(conn, transfer[1], transfer[2]) == 0):
        return 0
    else:
        return cur.lastrowid


# Main
if __name__ == '__main__':
    # Connect to database and initialise
    db_conn = sqlite3.connect(DATABASE)
    bot_user_id = 1
    curr = db_conn.cursor()
    curr.execute("SELECT id, nick, balance FROM users")
    data = curr.fetchall()
    if data:
        for player in data:
            player_id: int = player[0]
            nick: str = player[1]
            balance = player[2]
            if balance - round(balance) != 0:
                correction = ceil(balance) - balance
                create_transfer(db_conn, (bot_user_id, player_id, correction))
                print(f'Transferred {correction} to {nick}.')
    # Close database
    db_conn.close()
