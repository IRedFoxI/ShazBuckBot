# -*- coding: utf-8 -*-
"""database handling for shazbuckbot"""

import sqlite3
from typing import Tuple, List
from helper_classes import GameStatus, WagerResult, TimeDuration

DATABASE_VERSION = 1


class DataBase:

    def __init__(self, db_file, bot_discord_id, default_bet_window) -> None:
        """Initialize the database

        :param str db_file: Path and filename of the database
        :param int bot_discord_id: Discord id of the bot
        :param TimeDuration default_bet_window: The default bet window used if none specified
        """
        self.conn = sqlite3.connect(db_file)
        self.bot_discord_id = bot_discord_id

        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        if not cur.fetchall():
            self.new_database()

        cur = self.conn.cursor()
        cur.execute("PRAGMA user_version")
        data = cur.fetchone()
        db_version = 0
        if data:
            db_version = data[0]
        if db_version < DATABASE_VERSION:
            self.update_database(db_version, default_bet_window)

        cur = self.conn.cursor()
        cur.execute(''' SELECT id FROM users WHERE discord_id = ? ''', (bot_discord_id,))
        self.bot_user_id = cur.fetchone()[0]

    def close(self) -> None:
        self.conn.close()

    def new_database(self) -> None:
        """Initialize a new database"""

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                discord_id INTEGER NOT NULL,
                create_time INT NOT NULL,
                nick TEXT NOT NULL,
                mute_dm INTEGER NOT NULL,
                balance INTEGER NOT NULL
            );
        """)
        self.create_user((self.bot_discord_id, 'ShazBuckBot', 1, 0))
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                transfer_time INT NOT NULL,
                sender INTEGER NOT NULL,
                receiver INTEGER NOT NULL,
                amount INTEGER NOT NULL
            );
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                start_time INT NOT NULL,
                pick_time INT,
                team1 TEXT NOT NULL,
                team2 TEXT NOT NULL,
                status INTEGER NOT NULL,
                bet_window INTEGER NOT NULL
            );
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wagers (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                wager_time INTEGER NOT NULL,
                game_id INTEGER NOT NULL,
                prediction INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                result INTEGER NOT NULL
            );
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS motds (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                discord_id INT NOT NULL,
                channel_id INT NOT NULL,
                start_time INT NOT NULL,
                end_time INT,
                message TEXT NOT NULL
            );
        """)

    def update_database(self, db_version, default_bet_window) -> None:
        """Update the database to current version

        :param int db_version: Version of the database
        :param TimeDuration default_bet_window: The default bet window to use if none is specified
        :return:
        """
        if db_version < 1:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) AS CNTREC FROM pragma_table_info('games') WHERE name='bet_window'")
            data = cur.fetchone()
            if data[0] == 0:
                self.conn.execute(f"""
                    ALTER TABLE games ADD COLUMN bet_window INTEGER NOT NULL DEFAULT {default_bet_window.to_seconds}
                """)
            else:
                self.conn.execute("UPDATE games SET bet_window = bet_window * 60")
            self.conn.execute("PRAGMA user_version = 1")
            self.conn.commit()

    def create_user(self, user) -> int:
        """Create a new user into the users table
    
        :param tuple[int,str,int,int] user: The discord_id, nick, mute_dm and balance
        :return: The id of the user created
        """
        sql = ''' INSERT INTO users(discord_id,nick,mute_dm,balance,create_time)
                  VALUES(?,?,?,?,strftime('%s','now')) '''
        cur = self.conn.cursor()
        cur.execute(sql, user)
        self.conn.commit()
        return cur.lastrowid

    def get_user_data(self, user_id, fields) -> tuple:
        """Get user data from database
    
        :param int user_id: The id of the user
        :param Tuple[str] fields: tuple of field names
        :return: A tuple containing the requested data
        """
        fields = ', '.join(fields)
        cur = self.conn.cursor()
        cur.execute(f''' SELECT {fields} FROM users WHERE id = ? ''', (user_id,))
        return cur.fetchone()

    def get_user_data_by_discord_id(self, discord_id, fields) -> tuple:
        """Get user data from database

        :param int discord_id: The discord id of the user
        :param Tuple[str] fields: tuple of field names
        :return: A tuple containing the requested data
        """
        fields = ', '.join(fields)
        cur = self.conn.cursor()
        cur.execute(f''' SELECT {fields} FROM users WHERE discord_id = ? ''', (discord_id,))
        return cur.fetchone()

    def set_user_data(self, user_id, fields, values) -> None:
        """Set values of a user
    
        :param int user_id: The id of the user to change
        :param tuple[str] fields: Tuple of fields to be changed
        :param tuple values: Values of the fields to be changed
        """
        if len(fields) != len(values):
            raise ValueError('Number of values not equal to number of fields to be updated')
        fields_str = ' = ?, '.join(fields) + ' = ?'
        values += (user_id,)
        sql = f''' UPDATE users SET {fields_str} WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def change_balance(self, user_id, balance_change) -> None:
        """Change the balance of a user
    
        :param int user_id: The id of the user whose balance needs updating
        :param int balance_change: The amount the balance needs to change
        """
        values = (balance_change, user_id)
        sql = ''' UPDATE users SET balance = balance + ? WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def create_transfer(self, transfer) -> int:
        """Create a new transfer into the transfers table and update the balances
    
        :param tuple(int,int,int) transfer: Tuple of the user_id of the sender, user_id of the receiver and the amount
            to be transferred
        :return: The id of the transfer or 0 if an error occurred
        """
        sql = ''' INSERT INTO transfers(sender, receiver, amount, transfer_time)
                  VALUES(?, ?, ?, strftime('%s','now')) '''
        cur = self.conn.cursor()
        cur.execute(sql, transfer)
        self.conn.commit()
        if (self.change_balance(transfer[0], -transfer[2]) == 0 or
                self.change_balance(transfer[1], transfer[2]) == 0):
            return 0
        else:
            return cur.lastrowid

    def create_game(self, game) -> int:
        """Create a new game into the games table
    
        :param tuple[str,str,str,int] game: Tuple with the details of the game
        :return: The id of the created game
        """
        game += (GameStatus.PICKING,)
        sql = ''' INSERT INTO games(queue, start_time, team1, team2, bet_window, status)
                  VALUES(?, strftime('%s','now'), ?, ?, ?, ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, game)
        self.conn.commit()
        return cur.lastrowid

    def cancel_game(self, game_id) -> None:
        """Update a game in the games table to Cancelled status
    
        :param int game_id: The id of the game to update to InProgress status
        """
        values = (GameStatus.CANCELLED, game_id)
        sql = ''' UPDATE games SET status = ? WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def update_teams(self, game_id, teams) -> None:
        """Update a game in the games table to InProgress status
    
        :param int game_id: The id of the game to update to InProgress status
        :param tuple[str,str] teams: The picked teams of the game
        """
        values = teams + (game_id,)
        sql = ''' UPDATE games
                  SET team1 = ?, team2 = ?
                  WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def pick_game(self, game_id, teams) -> None:
        """Update a game in the games table to InProgress status
    
        :param int game_id: The id of the game to update to InProgress status
        :param tuple[str,str] teams: The picked teams of the game
        """
        values = teams + (GameStatus.INPROGRESS, game_id)
        sql = ''' UPDATE games
                  SET pick_time = strftime('%s','now'), team1 = ?, team2 = ?, 
                  status = ? 
                  WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def finish_game(self, game_id, result) -> None:
        """Update a game into the games table with result
    
        :param int game_id: The id of the game to be finished
        :param int result: The result of the game in GAME_STATUS format
        """
        if result not in set(r.value for r in GameStatus):
            raise ValueError()
        values = (result, game_id)
        sql = ''' UPDATE games SET status = ? WHERE id = ?'''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def games_in_progress(self, game_id) -> List[Tuple[int, str, str, str, int, int]]:
        """Provide data on games that are in progress

        :param int game_id: id of the game or zero to return the data on all in progress games
        :return: List of Tuples containing the game_id, team1, team2, queue, time since pick and bet window for each
        game
        """
        if game_id == 0:
            sql = ''' SELECT id, team1, team2, queue,
                      CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                      bet_window
                      FROM games WHERE status = ? '''
            game_data = (GameStatus.INPROGRESS,)
        else:
            sql = ''' SELECT id, team1, team2, queue,
                      CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                      bet_window
                      FROM games WHERE id = ? AND status = ? '''
            game_data = (game_id, GameStatus.INPROGRESS)
        cursor = self.conn.cursor()
        cursor.execute(sql, game_data)
        data = cursor.fetchall()
        games = []
        for game in data:
            game_id: int = game[0]
            teams: Tuple[str, str] = game[1:3]
            queue: str = game[3]
            time_since_pick: int = game[4]
            bet_window: int = game[5]
            games.append((game_id,) + teams + (queue, time_since_pick, bet_window))
        return games

    def create_wager(self, wager) -> int:
        """Create a new wager into the wagers table
    
        :param tuple[int,int,int,int] wager: Tuple with the details of the wager
        :return: The id of the created wager or 0 if an error occurred
        """
        wager += (WagerResult.INPROGRESS,)
        sql = ''' INSERT INTO wagers(user_id, wager_time, game_id, prediction, 
                  amount, result)
                  VALUES(?, strftime('%s','now'), ?, ?, ?, ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, wager)
        self.conn.commit()
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM users WHERE discord_id = ?", (self.bot_discord_id,))
        bot_user_id: int = cur.fetchone()[0]
        transfer = (wager[0], bot_user_id, wager[3])
        if self.create_transfer(transfer) == 0:
            return 0
        else:
            return cur.lastrowid

    def change_wager(self, wager_id, amount_change) -> None:
        """Change the wager amount
    
        :param int wager_id: The id of the user whose balance needs updating
        :param int amount_change: The amount the balance needs to change
        """
        values = (amount_change, wager_id)
        sql = ''' UPDATE wagers SET amount = amount + ? WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()
        cur = self.conn.cursor()
        cur.execute("SELECT user_id FROM wagers WHERE id = ?", (wager_id,))
        user_id: int = cur.fetchone()[0]
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM users WHERE discord_id = ?", (self.bot_discord_id,))
        bot_user_id: int = cur.fetchone()[0]
        transfer = (user_id, bot_user_id, amount_change)
        self.create_transfer(transfer)

    def wager_result(self, wager_id, result) -> None:
        """Update the result of a wager
    
        :param int wager_id: The id of the wager to be updated
        :param int result: Result of the wager in the format of WAGER_RESULT
        """
        if result not in set(r.value for r in WagerResult):
            raise ValueError()
        values = (result, wager_id)
        sql = ''' UPDATE wagers SET result = ? WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, values)
        self.conn.commit()

    def wagers_from_game_id(self, game_id, wager_result) -> List[Tuple[int, int, int, str, str, str]]:
        """Return all the data of the wagers placed on a certain game
        
        :param int game_id: Game id of the game
        :param WagerResult wager_result: Only return wagers with this status
        :return: List of wager data (id, user_id, amount, nick, team1, team2)
        """
        sql = ''' SELECT wagers.id, user_id, amount, nick, team1, team2 FROM wagers, users, games 
                  WHERE game_id = ? AND users.id = user_id AND games.id = game_id AND result = ?'''
        cursor = self.conn.cursor()
        cursor.execute(sql, (game_id, wager_result))
        data = cursor.fetchall()
        wagers = []
        for wager in data:
            wager_id: int = wager[0]
            user_id: int = wager[1]
            amount: int = wager[2]
            nick: str = wager[3]
            teams: Tuple[str, str] = wager[4:6]
            wagers.append((wager_id, user_id, amount, nick) + teams)
        return wagers

    def create_motd(self, motd) -> int:
        """Create a new motd into the motds table
    
        :param tuple[int,int,str,int] motd: Tuple with the details of the wager
        :return: The id of the created motd or 0 if an error occurred
        """
        if len(motd) != 4:
            raise ValueError
        sql = ''' INSERT INTO motds(discord_id, channel_id, start_time, message, end_time)
                  VALUES(?, ?, strftime('%s','now'), ?, strftime('%s','now') + ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, motd)
        self.conn.commit()
        return cur.lastrowid

    def end_motd(self, motd_id) -> None:
        """End a motd
    
        :param int motd_id: The id of the motd to be ended
        """
        sql = ''' UPDATE motds SET end_time = strftime('%s','now') WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, (motd_id,))
        self.conn.commit()
