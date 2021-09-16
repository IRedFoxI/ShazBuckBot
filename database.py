# -*- coding: utf-8 -*-
"""database handling for shazbuckbot"""

import sqlite3

from trueskill import Rating, expose

from helper_classes import GameStatus, WagerResult, TimeDuration, TransferReason

DATABASE_VERSION = 2


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
            db_version = self.update_database(db_version, default_bet_window)
            if db_version != DATABASE_VERSION:
                raise Exception('Database upgrade failed')

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

    def update_database(self, db_version, default_bet_window) -> int:
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
            db_version = 1
        if db_version < 2:
            self.conn.execute(""" ALTER TABLE transfers ADD COLUMN reason INTEGER """)
            self.conn.execute(""" ALTER TABLE transfers ADD COLUMN reason_id INTEGER """)
            self.conn.execute("PRAGMA user_version = 2")
            self.conn.commit()
            db_version = 2
        return db_version

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
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

    def get_user_data_by_discord_id(self, discord_id, fields) -> tuple:
        """Get user data from database

        :param int discord_id: The discord id of the user
        :param Tuple[str] fields: tuple of field names
        :return: A tuple containing the requested data
        """
        fields = ', '.join(fields)
        cur = self.conn.cursor()
        cur.execute(f''' SELECT {fields} FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

    def get_top5(self) -> list[tuple[str, int, int]]:
        """Returns the top 5

        :return: List of Tuples with the data of the top 5 (nick, discord_id and balance)
        """
        sql = ''' SELECT nick, discord_id, balance FROM users ORDER BY balance DESC LIMIT 5 '''
        cur = self.conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        top5 = []
        for user in data:
            nick: str = user[0]
            discord_id: int = user[1]
            balance: int = user[2]
            top5.append((nick, discord_id, balance))
        return top5

    def get_beggars(self) -> list[tuple[str, int, int]]:
        """Returns the beggars

        :return: List of Tuples with the data of the beggars (nick, discord_id and balance)
        """
        sql = ''' SELECT nick, discord_id, SUM(CASE WHEN users.id = receiver THEN amount ELSE -amount END) 
                  AS total_sender_amount FROM users, transfers 
                  WHERE (users.id = receiver or users.id = sender) AND sender <> 1 AND receiver <> 1 
                  AND sender <> receiver GROUP BY nick ORDER BY total_sender_amount DESC LIMIT 5 '''
        cur = self.conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        beggars = []
        for user in data:
            nick: str = user[0]
            discord_id: int = user[1]
            amount: int = user[2]
            beggars.append((nick, discord_id, amount))
        return beggars

    def get_philanthropists(self) -> list[tuple[str, int, int]]:
        """Returns the philanthropists

        :return: List of Tuples with the data of the philanthropists (nick, discord_id and balance)
        """
        sql = ''' SELECT nick, discord_id, SUM(CASE WHEN users.id = sender THEN amount ELSE -amount END) 
                  AS total_sender_amount FROM users, transfers
                  WHERE (users.id = receiver or users.id = sender) AND sender <> 1 AND receiver <> 1 
                  AND sender <> receiver GROUP BY nick ORDER BY total_sender_amount DESC LIMIT 5 '''
        cur = self.conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        beggars = []
        for user in data:
            nick: str = user[0]
            discord_id: int = user[1]
            amount: int = user[2]
            beggars.append((nick, discord_id, amount))
        return beggars

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
    
        :param tuple[int, int, int, TransferReason, int] transfer: Tuple of the user_id of the sender, user_id of the
            receiver and the amount to be transferred
        :return: The id of the transfer or 0 if an error occurred
        """
        sql = ''' INSERT INTO transfers(sender, receiver, amount, transfer_time, reason, reason_id)
                  VALUES(?, ?, ?, strftime('%s','now'), ?, ?) '''
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
        game += (GameStatus.PICKING.value,)
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

    def get_games_by_status(self, status) -> list[tuple[int, str, str, str, GameStatus, int, int, int]]:
        """Provide data on the currently running games

        :param GameStatus status: The status of the games to search for
        :return: List of Tuples containing the game_id, team1, team2, queue, status, time since start, time since pick
        and bet window for each game
        """
        sql = ''' SELECT id, team1, team2, queue, status, 
                  CAST (((julianday('now') - julianday(start_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                  CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                  bet_window FROM games WHERE status = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, (status, ))
        data = cur.fetchall()
        games = []
        for game in data:
            game_id: int = game[0]
            teams: tuple[str, str] = game[1:3]
            queue: str = game[3]
            status = GameStatus(game[4])
            time_since_start: int = game[5]
            time_since_pick: int = game[6]
            bet_window: int = game[7]
            games.append((game_id,) + teams + (queue, status, time_since_start, time_since_pick, bet_window))
        return games

    def get_game_by_id(self, game_id) -> tuple[int, str, str, str, GameStatus, int, int, int]:
        """Provide data on a game

        :param int game_id: The id of the game
        :return: List of Tuples containing the game_id, team1, team2, queue, status, time since start, time since pick
        and bet window
        """
        sql = ''' SELECT id, team1, team2, queue, status,
                  CAST (((julianday('now') - julianday(start_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                  CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60 * 60) AS INTEGER),
                  bet_window FROM games WHERE id = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, (game_id,))
        data = cur.fetchone()
        if data:
            game_id: int = data[0]
            teams: tuple[str, str] = data[1:3]
            queue: str = data[3]
            status = GameStatus(data[4])
            time_since_start: int = data[5]
            time_since_pick: int = data[6]
            bet_window: int = data[7]
            return tuple((game_id,) + teams + (queue, status, time_since_start, time_since_pick, bet_window))
        else:
            return tuple()

    def get_game_data(self, game_id, fields) -> tuple:
        """Get user data from database

        :param int game_id: The id of the user
        :param Tuple[str] fields: tuple of field names
        :return: A tuple containing the requested data
        """
        fields = ', '.join(fields)
        cur = self.conn.cursor()
        cur.execute(f''' SELECT {fields} FROM games WHERE id = ? ''', (game_id,))
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

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

    def get_wagers_from_game_id(self, game_id, wager_result) -> list[tuple[int, int, GameStatus, int, str, int, str,
                                                                           str]]:
        """Return all the data of the wagers placed on a certain game
        
        :param int game_id: Game id of the game
        :param WagerResult wager_result: Only return wagers with this status
        :return: List of wager data (id, user_id, prediction, amount, nick, discord_id, team1, team2)
        """
        sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id, team1, team2 
                  FROM wagers, users, games 
                  WHERE game_id = ? AND users.id = user_id AND games.id = game_id AND result = ? '''
        cur = self.conn.cursor()
        cur.execute(sql, (game_id, wager_result))
        data = cur.fetchall()
        wagers = []
        for wager in data:
            wager_id: int = wager[0]
            user_id: int = wager[1]
            prediction = GameStatus(wager[2])
            amount: int = wager[3]
            nick: str = wager[4]
            discord_id: int = wager[5]
            teams: tuple[str, str] = wager[6:8]
            wagers.append((wager_id, user_id, prediction, amount, nick, discord_id) + teams)
        return wagers

    def get_current_wager(self, user_id, game_id) -> tuple[int, GameStatus]:
        """Return all the data of the wagers placed on a certain game

        :param int user_id: User id of the person placing the bet
        :param int game_id: Game id of the game
        :return: Tuple of the wager data (id, prediction)
        """
        sql = ''' SELECT id, prediction FROM wagers WHERE user_id = ? AND game_id = ? AND result = ? '''
        values = (user_id, game_id, WagerResult.INPROGRESS)
        cur = self.conn.cursor()
        cur.execute(sql, values)
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

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

    def get_motd(self, channel_id, motd_id, *, general=False) -> tuple[int, int, int, int, str]:
        """Get the currently active MOTDs

        :param motd_id: The id of the MOTD
        :param general: If True searches in the general MOTDs as well as the channel specific ones
        :param channel_id: The id of the channel
        :return: List of Tuples of the motd data (id, discord_id, channel_id, start_time, end_time, message)
        """
        if general:
            sql = ''' SELECT discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE id = ? AND (channel_id = 0 OR channel_id = ?) AND end_time > strftime('%s','now') '''
        else:
            sql = ''' SELECT discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE id = ? AND channel_id = ? AND end_time > strftime('%s','now') '''
        cur = self.conn.cursor()
        cur.execute(sql, (motd_id, channel_id))
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

    def get_motds(self, channel_id, *, general=False) -> list[tuple[int, int, int, int, int, str]]:
        """Get the currently active MOTDs

        :param general: If True return the general MOTDs as well as the channel specific ones
        :param channel_id: The id of the channel
        :return: List of Tuples of the motd data (id, discord_id, channel_id, start_time, end_time, message)
        """
        if general:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE (channel_id = 0 OR channel_id = ?) AND end_time > strftime('%s','now') '''
        else:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE channel_id = ? AND end_time > strftime('%s','now') '''
        cur = self.conn.cursor()
        cur.execute(sql, (channel_id,))
        data = cur.fetchall()
        motds = []
        for motd in data:
            motd_id: int = motd[0]
            author_id: int = motd[1]
            channel_id: int = motd[2]
            start_time: int = motd[3]
            end_time: int = motd[4]
            message: str = motd[5]
            motds.append((motd_id, author_id, channel_id, start_time, end_time, message))
        return motds

    def get_trueskill_rating(self, player_id) -> tuple[int, int, int]:
        """Return the trueskill rating of a player

        :param int player_id: Discord id of the player
        :return: Tuple of the mean and standard deviation of the trueskill rating and the number of recorded matches
        """
        sql = ''' SELECT mu, sigma, ROW_NUMBER() OVER(ORDER BY game_id ASC) AS game_nr FROM trueskills 
                  WHERE discord_id = ? ORDER BY game_id DESC LIMIT 1 '''
        cur = self.conn.cursor()
        cur.execute(sql, (player_id,))
        data = cur.fetchone()
        if data:
            return tuple(data)
        else:
            return tuple()

    def new_trueskill_rating(self, player_id, game_id, rating) -> None:
        """

        :param player_id: The id of the player to update
        :param game_id: The id of the game that cause the update
        :param Rating rating: The new player rating
        :return:
        """
        trueskill_update = (player_id, game_id, rating.mu, rating.sigma, expose(rating))
        sql = ''' INSERT INTO trueskills(discord_id, game_id, mu, sigma, trueskill) VALUES(?, ?, ?, ?, ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, trueskill_update)
        self.conn.commit()
