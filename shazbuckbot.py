# -*- coding: utf-8 -*-
"""A discord bot to bet on PUGs."""

import asyncio
import atexit
import re
import unicodedata
from enum import IntEnum
import yaml
import sqlite3

import discord
from discord.ext import commands
from typing import List, Tuple

config = yaml.safe_load(open("config.yml"))
TOKEN = config['token']
DATABASE = config['database']
DISCORD_ID = config['discord_id']
INIT_BAL = config['init_bal']
BET_WINDOW = config['bet_window']
BULLYBOT_DISCORD_ID = config['bullybot_discord_id']
PUG_CHANNEL_ID = config['pug_channel_id']
BOT_CHANNEL_ID = config['bot_channel_id']
GAME_STATUS = IntEnum('Game_Status', 'Picking Cancelled InProgress Team1 Team2 Tied')
WAGER_RESULT = IntEnum('Wager_Result', 'InProgress Won Lost Canceled')
DM_TIME_TO_WAIT = 0.21  # Seconds
DURATION_TOLERANCE = 60  # Minutes
REACTIONS = ["👎", "👍"]


def normalize_caseless(text):
    return unicodedata.normalize("NFKD", text.casefold())


def caseless_equal(left, right):
    return normalize_caseless(left) == normalize_caseless(right)


def create_user(conn, user) -> int:
    """Create a new user into the users table

    :param sqlite3.Connection conn: The database connection to be used
    :param tuple[int,str,int,int] user: The discord_id, nick, mute_dm and balance
    :return: The id of the user created
    """
    sql = ''' INSERT INTO users(discord_id,nick,mute_dm,balance,create_time)
              VALUES(?,?,?,?,strftime('%s','now')) '''
    cur = conn.cursor()
    cur.execute(sql, user)
    conn.commit()
    return cur.lastrowid


def get_user_data(conn, user_id, fields) -> tuple:
    """Get user data from database

    :param sqlite3.Connection conn: The database connection to be used
    :param int user_id: The id of the user
    :param str fields: String of field names separated by a comma
    :return: A tuple containing the requested data
    """
    cur = conn.cursor()
    cur.execute(f''' SELECT {fields} FROM users WHERE id = ? ''', (user_id,))
    return cur.fetchone()


def set_user_data(conn, user_id, fields, values) -> None:
    """Set values of a user

    :param sqlite3.Connection conn: Connection to the database
    :param int user_id: The id of the user to change
    :param tuple[str] fields: Tuple of fields to be changed
    :param tuple values: Values of the fields to be changed
    """
    fields_str = ' = ?, '.join(fields) + ' = ?'
    values += (user_id,)
    sql = f''' UPDATE users SET {fields_str} WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


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


def create_game(conn, game) -> int:
    """Create a new game into the games table

    :param sqlite3.Connection conn:
    :param tuple[str,str,str] game: Tuple with the details of the game
    :return: The id of the created game
    """
    game += (GAME_STATUS.Picking,)
    sql = ''' INSERT INTO games(queue, start_time, team1, team2, status)
              VALUES(?, strftime('%s','now'), ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, game)
    conn.commit()
    return cur.lastrowid


def cancel_game(conn, game_id) -> None:
    """Update a game in the games table to Cancelled status

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to update to InProgress status
    """
    values = (GAME_STATUS.Cancelled, game_id)
    sql = ''' UPDATE games SET status = ? WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def update_teams(conn, game_id, teams) -> None:
    """Update a game in the games table to InProgress status

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to update to InProgress status
    :param tuple[str,str] teams: The picked teams of the game
    """
    values = teams + (game_id,)
    sql = ''' UPDATE games
              SET team1 = ?, team2 = ?
              WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def pick_game(conn, game_id, teams) -> None:
    """Update a game in the games table to InProgress status

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to update to InProgress status
    :param tuple[str,str] teams: The picked teams of the game
    """
    values = teams + (GAME_STATUS.InProgress, game_id)
    sql = ''' UPDATE games
              SET pick_time = strftime('%s','now'), team1 = ?, team2 = ?, 
              status = ? 
              WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def finish_game(conn, game_id, result) -> None:
    """Update a game into the games table with result

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to be finished
    :param int result: The result of the game in GAME_STATUS format
    """
    if result not in set(r.value for r in GAME_STATUS):
        raise ValueError()
    values = (result, game_id)
    sql = ''' UPDATE games SET status = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def create_wager(conn, wager) -> int:
    """Create a new wager into the wagers table

    :param sqlite3.Connection conn: Connection to the database
    :param tuple[int,int,int,int] wager: Tuple with the details of the wager
    :return: The id of the created wager or 0 if an error occurred
    """
    wager += (WAGER_RESULT.InProgress,)
    sql = ''' INSERT INTO wagers(user_id, wager_time, game_id, prediction, 
              amount, result)
              VALUES(?, strftime('%s','now'), ?, ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, wager)
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE discord_id = ?", (DISCORD_ID,))
    bot_user_id: int = cur.fetchone()[0]
    transfer = (wager[0], bot_user_id, wager[3])
    if create_transfer(conn, transfer) == 0:
        return 0
    else:
        return cur.lastrowid


def change_wager(conn, wager_id, amount_change) -> None:
    """Change the wager amount

    :param sqlite3.Connection conn: The connection to the database
    :param int wager_id: The id of the user whose balance needs updating
    :param int amount_change: The amount the balance needs to change
    """
    values = (amount_change, wager_id)
    sql = ''' UPDATE wagers SET amount = amount + ? WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM wagers WHERE id = ?", (wager_id,))
    user_id: int = cur.fetchone()[0]
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE discord_id = ?", (DISCORD_ID,))
    bot_user_id: int = cur.fetchone()[0]
    transfer = (user_id, bot_user_id, amount_change)
    create_transfer(conn, transfer)


def wager_result(conn, wager_id, result) -> None:
    """Update the result of a wager

    :param sqlite3.Connection conn: Connection to the database
    :param int wager_id: The id of the wager to be updated
    :param int result: Result of the wager in the format of WAGER_RESULT
    """
    if result not in set(r.value for r in WAGER_RESULT):
        raise ValueError()
    values = (result, wager_id)
    sql = ''' UPDATE wagers SET result = ? WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def init_db(conn) -> None:
    """Initialize a new database

    :param sqlite3.Connection conn: Connection to the database
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            discord_id INTEGER NOT NULL,
            create_time INT NOT NULL,
            nick TEXT NOT NULL,
            mute_dm INTEGER NOT NULL,
            balance INTEGER NOT NULL
        );
    """)
    cur = conn.cursor()
    cur.execute("SELECT rowid FROM users WHERE discord_id = ?", (DISCORD_ID,))
    data = cur.fetchone()
    if data is None:
        create_user(conn, (DISCORD_ID, 'ShazBuckBot', 1, 0))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            transfer_time INT NOT NULL,
            sender INTEGER NOT NULL,
            receiver INTEGER NOT NULL,
            amount INTEGER NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            start_time INT NOT NULL,
            pick_time INT,
            team1 TEXT NOT NULL,
            team2 TEXT NOT NULL,
            status INTEGER NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wagers (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            wager_time INT NOT NULL,
            game_id INTEGER NOT NULL,
            prediction INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            result INTEGER NOT NULL
        );
    """)


def close_db(conn) -> None:
    """Close the database connection

    :param sqlite3.Connection conn: Connection to the database
    """
    conn.close()
    print('Database closed.')


def start_bot():
    bot = commands.Bot(command_prefix='!')
    conn = sqlite3.connect(DATABASE)
    init_db(conn)
    cur = conn.cursor()
    cur.execute(''' SELECT id FROM users WHERE discord_id = ? ''', (DISCORD_ID,))
    bot_user_id = cur.fetchone()[0]
    atexit.register(close_db, conn)

    async def get_member(discord_id) -> discord.Member:
        """Find the discord member based on their discord id

        :param int discord_id: The discord id of the user
        """
        member = None
        for guild in bot.guilds:
            if guild.get_channel(BOT_CHANNEL_ID):
                try:
                    member = await guild.fetch_member(discord_id)
                except discord.NotFound:
                    pass
        return member

    async def send_dm(user_id, message) -> None:
        """Send a discord DM to the user

        :param int user_id: User id in database
        :param str message: The message to be send to the user
        """
        (discord_id, mute_dm) = get_user_data(conn, user_id, 'discord_id, mute_dm')
        if not mute_dm:
            user = await get_member(discord_id)
            if user:
                await asyncio.sleep(DM_TIME_TO_WAIT)
                await user.create_dm()
                await user.dm_channel.send(message)

    async def cancel_wagers(game_id, reason) -> None:
        """Cancel wagers and return the bet to the users

        :param int game_id: The id of game which bets should be canceled
        :param str reason: The reason of the cancellation to send to the users in a DM
        """
        sql = ''' SELECT wagers.id, user_id, amount, nick, team1, team2 FROM wagers, users, games 
                  WHERE game_id = ? AND users.id = user_id AND games.id = ? AND result = ?'''
        cursor = conn.cursor()
        cursor.execute(sql, (game_id, game_id, WAGER_RESULT.InProgress))
        wagers = cursor.fetchall()
        for wager in wagers:
            wager_id: int = wager[0]
            user_id: int = wager[1]
            amount: int = wager[2]
            nick: str = wager[3]
            teams: Tuple[str, str] = wager[4:6]
            captains = [team.split(':')[0] for team in teams]
            transfer = (bot_user_id, user_id, amount)
            create_transfer(conn, transfer)
            wager_result(conn, wager_id, WAGER_RESULT.Canceled)
            msg = (f'Hi {nick}. Your bet on the game captained by {" and ".join(captains)} was canceled '
                   f'due to {reason}. Your bet of {amount} shazbucks has been returned to you.')
            await send_dm(user_id, msg)

    @bot.event
    async def on_ready():
        print(f'{bot.user} is connected to the following guild(s):')
        for guild in bot.guilds:
            print(f'{guild.name}(id: {guild.id})')

    def in_channel(channel_id):
        def predicate(ctx):
            return ctx.message.channel.id == channel_id
        return commands.check(predicate)

    @bot.command(name='shazbucks', help='Create an account and get free shazbucks')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_shazbucks(ctx):
        success = False
        discord_id = ctx.author.id
        nick = ctx.author.name
        cursor = conn.cursor()
        cursor.execute(''' SELECT id,nick FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            user_id = create_user(conn, (discord_id, nick, 0, 0))
            if user_id == 0 or create_transfer(conn, (bot_user_id, user_id, INIT_BAL)) == 0:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, something went wrong creating your account. Please try again later or '
                    f'contact an admin.'
                )
                print(
                    f'Something went wrong creating an account for {ctx.author.name}. User id {user_id}.'
                )
            else:
                msg = (
                    f'Hi {ctx.author.name}, welcome! You have received an initial balance of {INIT_BAL} '
                    f'shazbucks, bet wisely! These are the basic commands:\n'
                    f'- !balance - to check your balance\n'
                    f'- !show - to show games that are currently open for betting\n'
                    f'- !mute - to mute the bot\'s DMs\n'
                    f'- !bet <captain> <amount> - to bet <amount> on the team captained by <captain>\n'
                    f'Instead of <captain> you can also use 1,2, Red or Blue to select a team '
                    f'from the last picked game'
                )
                await send_dm(user_id, msg)
                success = True
        else:
            msg = f'Hi {data[1]}, you already have an account!'
            await send_dm(data[0], msg)
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='balance', help='Check balance')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_balance(ctx):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT nick, balance FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {data[0]}, your balance is {data[1]} shazbucks!')
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='gift', help='Gift shazbucks to a discord user')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_gift(ctx, receiver: discord.Member, amount: int):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, nick, balance FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            sender_id: int = data[0]
            nick: str = data[1]
            balance: int = data[2]
            if balance < amount:
                msg = (f'Hi {nick}, you do not have enough balance to transfer {amount} shazbucks to '
                       f'{receiver.name}! Your current balance is {balance} shazbucks.')
                await send_dm(sender_id, msg)
            elif amount < 0:
                msg = f'Hi {nick}, you cannot gift a negative amount.'
                await send_dm(sender_id, msg)
            else:
                discord_id = receiver.id
                cursor = conn.cursor()
                cursor.execute(''' SELECT id, nick, balance FROM users WHERE discord_id = ? ''', (discord_id,))
                data = cursor.fetchone()
                if data is None:
                    msg = f'Hi {nick}, {receiver} does not have an account yet!'
                    await send_dm(sender_id, msg)
                else:
                    receiver_id: int = data[0]
                    receiver_nick: str = data[1]
                    receiver_bal: int = data[2]
                    transfer = (sender_id, receiver_id, amount)
                    if create_transfer(conn, transfer) == 0:
                        msg = (f'Hi {nick}, your gift of {amount} shazbucks to {receiver} was somehow '
                               f'unsuccessful. Please try again later.')
                        await send_dm(sender_id, msg)
                        print(f'{ctx.author.name} tried to gift {amount} shazbucks to {receiver_nick} '
                              f'but something went wrong.')
                    else:
                        balance -= amount
                        receiver_bal += amount
                        msg = (f'Hi {nick}, your gift of {amount} shazbucks to {receiver.name} was successful. '
                               f'Your new balance is {balance} shazbucks.')
                        await send_dm(sender_id, msg)
                        msg = (f'Hi {receiver_nick}, you have received a gift of {amount} shazbucks from {nick}. '
                               f'Your new balance is {receiver_bal} shazbucks.')
                        await send_dm(receiver_id, msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='bet', help='Bet shazbucks on a game. Winner should be either the name of the captain '
                                  'or 1, 2, Red or Blue.')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_bet(ctx, winner: str, amount: int):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, nick, balance FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            user_id: int = data[0]
            nick: str = data[1]
            balance: int = data[2]
            if balance < amount:
                msg = (f'Hi {nick}, you do not have enough balance to bet {amount} shazbucks! Your current '
                       f'balance is {balance} shazbucks.')
                await send_dm(user_id, msg)
            elif amount <= 0:
                msg = f'Hi {nick}, you cannot bet a negative or zero amount.'
                await send_dm(user_id, msg)
            else:
                sql = ''' SELECT id, team1, team2, 
                          CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER) 
                          FROM games WHERE status = ? '''
                cursor = conn.cursor()
                cursor.execute(sql, (GAME_STATUS.InProgress,))
                games = cursor.fetchall()
                if not games:
                    msg = f'Hi {nick}. No games are running. Please wait until teams are picked.'
                    await send_dm(user_id, msg)
                else:
                    game_id: int = games[-1][0]
                    prediction = 0
                    time_since_pick = 0
                    if winner == "1" or caseless_equal(winner, "Red"):
                        prediction += GAME_STATUS.Team1
                        winner = games[-1][1].split(':')[0]
                        time_since_pick = games[-1][3]
                    elif winner == "2" or caseless_equal(winner, "Blue"):
                        prediction += GAME_STATUS.Team2
                        winner = games[-1][2].split(':')[0]
                        time_since_pick = games[-1][3]
                    else:
                        for game in games:
                            teams: Tuple[str, str] = game[1:3]
                            if caseless_equal(winner, teams[0].split(':')[0]):
                                game_id: int = game[0]
                                prediction += GAME_STATUS.Team1
                                time_since_pick = game[3]
                            elif caseless_equal(winner, teams[1].split(':')[0]):
                                game_id: int = game[0]
                                prediction += GAME_STATUS.Team2
                                time_since_pick = game[3]
                    if prediction == 0:
                        msg = (f'Hi {nick}, could not find a game captained by {winner}. Please check the spelling, '
                               f'use 1, 2, Red or Blue, or wait until the teams have been picked.')
                        await send_dm(user_id, msg)
                    elif time_since_pick > BET_WINDOW:
                        msg = (f'Hi {nick}, too late! The game has started {time_since_pick} minutes ago. '
                               f'Bets have to be made within {BET_WINDOW} minutes after picking is complete')
                        await send_dm(user_id, msg)
                    else:
                        sql = ''' SELECT id, prediction FROM wagers 
                                  WHERE user_id = ? AND game_id = ? AND result = ? '''
                        values = (user_id, game_id, WAGER_RESULT.InProgress)
                        cursor = conn.cursor()
                        cursor.execute(sql, values)
                        prev_wager: Tuple[int] = cursor.fetchone()
                        if prev_wager and prediction != prev_wager[1]:
                            msg = f'Hi {nick}, you cannot bet against yourself!'
                            await send_dm(user_id, msg)
                        elif prev_wager and prediction == prev_wager[1]:
                            change_wager(conn, prev_wager[0], amount)
                            balance -= amount
                            msg = (f'Hi {ctx.author.name}, your additional bet of {amount} shazbucks on {winner} was '
                                   f'successful. Your new balance is {balance} shazbucks.')
                            await send_dm(user_id, msg)
                            success = True
                        else:
                            wager = (user_id, game_id, prediction, amount)
                            if create_wager(conn, wager) == 0:
                                msg = (f'Hi {nick}, your bet of {amount} shazbucks on {winner} was somehow '
                                       f'unsuccessful. Please try again later.')
                                await send_dm(user_id, msg)
                                print(f'{nick} tried to bet {amount} shazbucks on {winner} but something '
                                      f'went wrong. User id {user_id}, game id {game_id}, prediction {prediction}.')
                            else:
                                balance -= amount
                                msg = (f'Hi {ctx.author.name}, your bet of {amount} shazbucks on {winner} was '
                                       f'successful. Your new balance is {balance} shazbucks.')
                                await send_dm(user_id, msg)
                                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='mute', help='Mute or unmute the bot\'s direct messages')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_mute(ctx):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, mute_dm FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            user_id: int = data[0]
            mute_dm: int = (get_user_data(conn, user_id, 'mute_dm')[0] + 1) % 2
            set_user_data(conn, user_id, ('mute_dm',), (mute_dm,))
            msg = f'Hi {ctx.author.name}, direct messages have been unmuted!'
            await send_dm(user_id, msg)
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='show', help='Show current open bets')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_show(ctx):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, nick FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            user_id: int = data[0]
            nick: str = data[1]
            sql = ''' SELECT id, team1, team2, 
                      CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER) 
                      FROM games WHERE status = ? '''
            cursor = conn.cursor()
            cursor.execute(sql, (GAME_STATUS.InProgress,))
            games = cursor.fetchall()
            if not games:
                msg = f'Hi {nick}. No games are running. Please wait until teams are picked.'
                await send_dm(user_id, msg)
            else:
                show_str = ''
                for game in games:
                    game_id = game[0]
                    teams = game[1:3]
                    captains = [team.split(':')[0] for team in teams]
                    run_time = game[3]
                    cursor = conn.cursor()
                    sql = ''' SELECT prediction, amount FROM wagers WHERE game_id = ? AND result = ? '''
                    cursor.execute(sql, (game_id, WAGER_RESULT.InProgress))
                    wagers = cursor.fetchall()
                    total_amounts = {GAME_STATUS.Team1: 0, GAME_STATUS.Team2: 0}
                    for wager in wagers:
                        prediction = GAME_STATUS(wager[0])
                        amount: int = wager[1]
                        total_amounts[prediction] += amount
                    if run_time <= BET_WINDOW:
                        show_str += (f'Game {game_id} ({BET_WINDOW - run_time} minutes left to bet): '
                                     f'{captains[0]}({total_amounts[GAME_STATUS.Team1]}) vs '
                                     f'{captains[1]}({total_amounts[GAME_STATUS.Team2]})\n')
                    else:
                        show_str += (f'Game {game_id} (Betting closed): '
                                     f'{captains[0]}({total_amounts[GAME_STATUS.Team1]}) vs '
                                     f'{captains[1]}({total_amounts[GAME_STATUS.Team2]})\n')
                if show_str:
                    await ctx.send(show_str)
                    success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='top5', help='Show the top 5 players')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_top5(ctx):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, nick FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            user_id: int = data[0]
            nick: str = data[1]
            sql = ''' SELECT nick, discord_id, balance FROM users ORDER BY balance DESC LIMIT 5 '''
            cursor = conn.cursor()
            cursor.execute(sql)
            users = cursor.fetchall()
            if not users:
                msg = f'Hi {nick}. Something went wrong, no top 5.'
                await send_dm(user_id, msg)
            else:
                top5_str = 'The top 5 players with the most shazbucks are: '
                for i, user in enumerate(users):
                    nick: str = user[0]
                    discord_id: int = user[1]
                    balance: int = user[2]
                    member = await get_member(discord_id)
                    username = member.mention if member else nick
                    top5_str += f'{username} ({balance})'
                    if i < len(users) - 2:
                        top5_str += ', '
                    elif i == len(users) - 2:
                        top5_str += ' and '
                    else:
                        top5_str += '.'
                await ctx.send(top5_str)
                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='quit', help='Shutdown bot')
    @in_channel(BOT_CHANNEL_ID)
    @commands.has_role('Developer')
    async def cmd_quit(ctx):
        success = True
        await ctx.message.add_reaction(REACTIONS[success])
        await ctx.bot.logout()
        try:
            quit()
        except SystemExit:
            pass

    @bot.command(name='change_game', help='Change the outcome of a game')
    @commands.has_role('Developer')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_change_game(ctx, game_id: int, result: str):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(''' SELECT id, nick FROM users WHERE discord_id = ? ''', (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, you do not have an account yet!')
        else:
            user_id: int = data[0]
            nick: str = data[1]
            sql = ''' SELECT team1, team2, status FROM games 
                      WHERE id = ? AND (status = ? OR status = ? OR status = ? OR status = ? OR status = ?) '''
            values = (game_id, GAME_STATUS.InProgress, GAME_STATUS.Cancelled, GAME_STATUS.Team1,
                      GAME_STATUS.Team2, GAME_STATUS.Tied)
            cursor = conn.cursor()
            cursor.execute(sql, values)
            games = cursor.fetchone()
            if not games:
                msg = (f'Hi {nick}. The game with id {game_id} does not exist or it\'s status is not '
                       f'InProgress, Team1, Team2, Tied or Cancelled.')
                await send_dm(user_id, msg)
            else:
                teams: Tuple[str, str] = games[0:2]
                captains = [team.split(':')[0] for team in teams]
                old_status = games[2]
                new_status = None
                if result in ['1', 'Red', 'red', 'Team1', 'team1', captains[0]]:
                    new_status = GAME_STATUS.Team1
                elif result in ['2', 'Blue', 'blue', 'Team2', 'team2', captains[1]]:
                    new_status = GAME_STATUS.Team2
                elif result in ['3', 'Tie', 'tie', 'Tied', 'tied']:
                    new_status = GAME_STATUS.Tied
                elif result in ['4', 'Cancel', 'cancel', 'Canceled', 'canceled', 'Cancelled', 'cancelled']:
                    new_status = GAME_STATUS.Cancelled
                else:
                    msg = (f'Hi {nick}. Result not understood. You can use 1, 2, Red or Blue or the captain\'s name '
                           f'to select the winning team. Or use 3/Tie/Tied to tie or '
                           f'4/Cancel/Canceled/Cancelled to cancel the game.')
                    await send_dm(user_id, msg)
                if new_status:
                    if new_status == old_status:
                        msg = (
                            f'Hi {nick}. The game with id {game_id} was already set to {new_status.name}.')
                        await send_dm(user_id, msg)
                    else:
                        result_msg = ''
                        total_amounts = {}
                        no_winners = 0
                        payout = 0
                        winners_msg = f''
                        sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id 
                                  FROM users, wagers 
                                  WHERE game_id = ? AND users.id = wagers.user_id AND result <> ?'''
                        cursor = conn.cursor()
                        cursor.execute(sql, (game_id, WAGER_RESULT.Canceled))
                        wagers = cursor.fetchall()
                        for wager in wagers:
                            prediction = GAME_STATUS(wager[2]).name
                            amount: int = wager[3]
                            if prediction in total_amounts:
                                total_amounts[prediction] += amount
                            else:
                                total_amounts[prediction] = amount
                        if old_status != GAME_STATUS.InProgress:
                            ratio = 0
                            if GAME_STATUS.Team1.name in total_amounts and GAME_STATUS.Team2.name in total_amounts:
                                ta_t1 = total_amounts[GAME_STATUS.Team1.name]
                                ta_t2 = total_amounts[GAME_STATUS.Team2.name]
                                if old_status == GAME_STATUS.Team1:
                                    ratio = (ta_t1 + ta_t2) / ta_t1
                                if old_status == GAME_STATUS.Team2:
                                    ratio = (ta_t1 + ta_t2) / ta_t2
                            # Claw back previous payout
                            finish_game(conn, game_id, GAME_STATUS.InProgress)
                            for wager in wagers:
                                wager_id: int = wager[0]
                                user_id: int = wager[1]
                                prediction: int = wager[2]
                                amount: int = wager[3]
                                nick: str = wager[4]
                                discord_id: int = wager[5]
                                if old_status == GAME_STATUS.Tied:
                                    transfer = (user_id, bot_user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                                    msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                           f'{" and ".join(captains)}, was changed. Your bet of {amount} shazbucks '
                                           f'has been placed again.')
                                    await send_dm(user_id, msg)
                                elif ratio == 0:
                                    transfer = (user_id, bot_user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                                    msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                           f'{" and ".join(captains)}, was changed. Your bet of {amount} shazbucks '
                                           f'has been placed again.')
                                    await send_dm(user_id, msg)
                                elif prediction == old_status:
                                    win_amount = round(amount * ratio)
                                    payout += win_amount
                                    transfer = (user_id, bot_user_id, win_amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                                    msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                           f'{" and ".join(captains)}, was changed. Your previous payout of '
                                           f'{win_amount} shazbucks has been clawed back.')
                                    await send_dm(user_id, msg)
                                    user = await get_member(discord_id)
                                    username = user.mention if user else nick
                                    winners_msg += f'{username}({win_amount}) '
                                    no_winners += 1
                                else:
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                            if old_status == GAME_STATUS.Tied:
                                if len(total_amounts) > 0:
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(captains)}, was changed. '
                                                  f'All wagers have been placed again.')
                            elif (old_status == GAME_STATUS.Team1 or
                                  old_status == GAME_STATUS.Team2):
                                if len(total_amounts) == 1:
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(captains)}, was changed. '
                                                  f'All wagers have been placed again.')
                                if len(total_amounts) == 2:
                                    verb = "was" if no_winners == 1 else "were"
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(captains)}, was changed. The winnings of '
                                                  f'{winners_msg} for a total of {payout} shazbucks {verb} '
                                                  f'clawed back.')
                            if result_msg:
                                await ctx.send(result_msg)
                        # Payout based on new result
                        result_msg = ''
                        no_winners = 0
                        payout = 0
                        winners_msg = f''
                        ratio = 0
                        if GAME_STATUS.Team1.name in total_amounts and GAME_STATUS.Team2.name in total_amounts:
                            ta_t1 = total_amounts[GAME_STATUS.Team1.name]
                            ta_t2 = total_amounts[GAME_STATUS.Team2.name]
                            if new_status == GAME_STATUS.Team1:
                                ratio = (ta_t1 + ta_t2) / ta_t1
                            if new_status == GAME_STATUS.Team2:
                                ratio = (ta_t1 + ta_t2) / ta_t2
                        finish_game(conn, game_id, new_status)
                        for wager in wagers:
                            wager_id: int = wager[0]
                            user_id: int = wager[1]
                            prediction: int = wager[2]
                            amount: int = wager[3]
                            nick: str = wager[4]
                            discord_id: int = wager[5]
                            if new_status == GAME_STATUS.Tied:
                                transfer = (bot_user_id, user_id, amount)
                                create_transfer(conn, transfer)
                                wager_result(conn, wager_id, WAGER_RESULT.Canceled)
                                msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                       f'{" and ".join(captains)}, was changed to a tie. Your bet of {amount} '
                                       f'shazbucks has been returned to you.')
                                await send_dm(user_id, msg)
                            elif ratio == 0:
                                transfer = (bot_user_id, user_id, amount)
                                create_transfer(conn, transfer)
                                wager_result(conn, wager_id, WAGER_RESULT.Canceled)
                                msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                       f'{" and ".join(captains)}, was changed. Nobody took your bet. '
                                       f'Your bet of {amount} shazbucks has been returned to you.')
                                await send_dm(user_id, msg)
                            elif prediction == new_status:
                                win_amount = round(amount * ratio)
                                payout += win_amount
                                transfer = (bot_user_id, user_id, win_amount)
                                create_transfer(conn, transfer)
                                wager_result(conn, wager_id, WAGER_RESULT.Won)
                                msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                       f'{" and ".join(captains)}, was changed. You correctly predicted the '
                                       f'new result. You have won {win_amount} shazbucks.')
                                await send_dm(user_id, msg)
                                user = await get_member(discord_id)
                                username = user.mention if user else nick
                                winners_msg += f'{username}({win_amount}) '
                                no_winners += 1
                            else:
                                wager_result(conn, wager_id, WAGER_RESULT.Lost)
                                msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                       f'{" and ".join(captains)}, was changed. You did not predicted the '
                                       f'new result and lost your bet of {amount} shazbucks.')
                                await send_dm(user_id, msg)
                        if new_status == GAME_STATUS.Tied:
                            if len(total_amounts) > 0:
                                result_msg = (f'The result of game {game_id}, captained by '
                                              f'{" and ".join(captains)}, was changed to a tie. '
                                              f'All wagers have been returned.')
                        elif (new_status == GAME_STATUS.Team1 or
                              new_status == GAME_STATUS.Team2):
                            if len(total_amounts) == 1:
                                result_msg = (f'The result of game {game_id}, captained by '
                                              f'{" and ".join(captains)}, was changed. '
                                              f'The game only had bets on one team. '
                                              f'All wagers have been returned.')
                            if len(total_amounts) == 2:
                                verb = "was" if no_winners == 1 else "were"
                                result_msg = (f'The result of game {game_id}, captained by '
                                              f'{" and ".join(captains)}, was changed. '
                                              f'{winners_msg}{verb} paid out a total of {payout} shazbucks.')
                        if result_msg:
                            await ctx.send(result_msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    # @bot.command(name='win', help='Simulate win result message')  # TODO: Remove this command
    # @in_channel(BOT_CHANNEL_ID)
    # @commands.has_role('Developer')
    # async def cmd_win(ctx):
    #     title = "Game 'NA' finished"
    #     description = '**Winner:** Team jet.Pixel\n**Duration:** 5 Minutes'
    #     embed_msg = discord.Embed(description=description, color=0x00ff00)
    #     await ctx.send(content='`{}`'.format(title.replace('`', '')), embed=embed_msg)

    # @bot.command(name='tie', help='Simulate tie result message')  # TODO: Remove this command
    # @in_channel(BOT_CHANNEL_ID)
    # @commands.has_role('Developer')
    # async def cmd_tie(ctx):
    #     title = "Game 'NA' finished"
    #     description = '**Tie game**\n**Duration:** 53 Minutes'
    #     embed_msg = discord.Embed(description=description, color=0x00ff00)
    #     await ctx.send(content='`{}`'.format(title.replace('`', '')), embed=embed_msg)

    # @bot.command(name='pick', help='Simulate picked message')  # TODO: Remove this command
    # @commands.has_role('Developer')
    # @in_channel(BOT_CHANNEL_ID)
    # async def cmd_picked(ctx):
    #     title = "Game 'NA' teams picked"
    #     description = ('**Teams**:\n'
    #                    'jet.Pixel: joey, thecaptaintea, yami, r.$.e\n'
    #                    'eligh_: iloveoob, Lögïc, GUNDERSTRUTT, Crysta\n'
    #                    '\n'
    #                    '**Maps**: Elite, Exhumed')
    #     embed_msg = discord.Embed(description=description, color=0x00ff00)
    #     await ctx.send(content='`{}`'.format(title.replace('`', '')), embed=embed_msg)

    # @bot.command(name='begin', help='Simulate begin message')  # TODO: Remove this command
    # @commands.has_role('Developer')
    # @in_channel(BOT_CHANNEL_ID)
    # async def cmd_begin(ctx):
    #     title = "Game 'NA' has begun"
    #     description = ('**Captains: @jet.Pixel & @eligh_**\n'
    #                    'joey, thecaptaintea, yami, r.$.e, iloveoob, Lögïc, '
    #                    'GUNDERSTRUTT, Crysta')
    #     embed_msg = discord.Embed(description=description, color=0x00ff00)
    #     await ctx.send(content='`{}`'.format(title.replace('`', '')), embed=embed_msg)

    @bot.event
    async def on_message(message):
        # Print messages to stdout for debugging purposes
        if (message.author.id == BULLYBOT_DISCORD_ID
                or message.author.id == DISCORD_ID):
            print(f'{message.author} wrote in #{message.channel} on '
                  f'{message.guild}: {message.content}')
            for embed in message.embeds:
                print(f'{embed.description}')
        # Parse BullyBot's messages for game info
        # (and own messages during development)
        if ((message.author.id == BULLYBOT_DISCORD_ID
             or message.author.id == DISCORD_ID)  # TODO: Remove this line
                and message.channel.id == PUG_CHANNEL_ID):
            if 'Game' in message.content:
                description = ''
                if message.embeds:
                    description = message.embeds[0].description
                if 'begun' in message.content:
                    queue = message.content.split("'")[1]
                    capt_str = description.split('\n')[0]
                    capt_str = capt_str.replace('**', '').replace('Captains:', '').replace('&', '')
                    pattern = '[<@!>]'
                    capt_ids = re.sub(pattern, '', capt_str).split()
                    capt_ids = [int(i) for i in capt_ids]
                    teams = ()
                    for capt_id in capt_ids:
                        user = await get_member(capt_id)
                        if user:
                            teams += (user.display_name,)
                        else:
                            teams += (str(capt_id),)
                    game = (queue,) + teams
                    game_id = create_game(conn, game)
                    print(f'Game {game_id} created in the {queue} queue:\n{teams[0]}\nversus\n{teams[1]}')
                    await message.add_reaction(REACTIONS[True])
                elif 'picked' in message.content:
                    queue = message.content.split("'")[1]
                    teams: Tuple[str, ...] = tuple(description.split('\n')[1:3])
                    captains = tuple([team.split(':')[0] for team in teams])
                    # Find the game that was just picked
                    game_values = (queue, GAME_STATUS.Picking) + captains + (captains[1], captains[0])
                    sql = ''' SELECT id FROM games WHERE queue = ? AND status = ? 
                              AND ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?)) '''
                    cursor = conn.cursor()
                    cursor.execute(sql, game_values)
                    games = cursor.fetchall()
                    if not games:
                        print(f'Game picked in {queue} queue, but no game with Picking status and captains '
                              f'{" and ".join(captains)} in that queue!')
                        game = (queue,) + teams
                        game_id = create_game(conn, game)
                        print(f'Game {game_id} created in the {queue} queue:\n{teams[0]}\nversus\n{teams[1]}')
                    else:
                        if len(games) > 1:
                            print(f'Game picked in {queue} queue, but multiple games with Picking status and '
                                  f'captains {" and ".join(captains)} in that queue! Selecting the last one '
                                  f'and hoping for the best.')
                        game_id: int = games[-1][0]
                    pick_game(conn, game_id, teams)
                    print(f'Game {game_id} picked in the {queue} queue:\n{teams[0]}\nversus\n{teams[1]}')
                    await message.add_reaction(REACTIONS[True])
                elif 'cancelled' in message.content:
                    success = False
                    # Find the game that was just cancelled
                    cursor = conn.cursor()
                    cursor.execute(''' SELECT id FROM games WHERE status = ? ''', (GAME_STATUS.InProgress,))
                    games = cursor.fetchall()
                    if not games:
                        print('PANIC: Game cancelled, but no game with Picking status, not sure what game to cancel!')
                    elif len(games) > 1:
                        print('PANIC: Game cancelled, but multiple games with Picking status, not sure what game to '
                              'cancel!')
                    else:
                        game_id: int = games[0][0]
                        cancel_game(conn, game_id)
                        print(f'Game {game_id} cancelled, hopefully it was the right one!')
                        success = True
                    await message.add_reaction(REACTIONS[success])
                elif 'finished' in message.content:
                    queue = message.content.split("'")[1]
                    [result, duration] = description.split('\n')
                    duration = int(duration.split(' ')[1])
                    result_msg = ''
                    game_result = None
                    total_amounts = {}
                    no_winners = 0
                    payout = 0
                    winners_msg = f''
                    # Find the game that just finished
                    game_values = (queue, GAME_STATUS.InProgress, duration, DURATION_TOLERANCE)
                    sql = ''' SELECT id, ABS(CAST (((julianday('now') - julianday(start_time, 'unixepoch')) 
                              * 24 * 60) AS INTEGER)), team1, team2 
                              FROM games 
                              WHERE queue = ? AND status = ? AND ABS(CAST (((julianday('now') 
                              - julianday(start_time, 'unixepoch')) * 24 * 60) AS INTEGER)) - ? <= ? '''
                    cursor = conn.cursor()
                    cursor.execute(sql, game_values)
                    games = cursor.fetchall()
                    game_id: int = games[0][0]
                    if not games:
                        print(f'PANIC: Game finished in {queue} queue, but no game with InProgress status and '
                              f'correct time in that queue.')
                    else:
                        teams: Tuple[str, str] = games[0][2:4]
                        captains = [team.split(":")[0] for team in teams]
                        if len(games) > 1:
                            duration_offsets: List[int] = [game[1] for game in games]
                            _, idx = min((val, idx) for (idx, val) in enumerate(duration_offsets))
                            game_id = games[idx][0]
                            teams = games[idx][2:4]
                            captains = [team.split(":")[0] for team in teams]
                        game_result = 0
                        if 'Tie' in result:
                            game_result += GAME_STATUS.Tied
                        else:
                            winner = " ".join(result.split(' ')[2:])
                            if winner == captains[0]:
                                game_result += GAME_STATUS.Team1
                            elif winner == captains[1]:
                                game_result += GAME_STATUS.Team2
                            else:
                                print(f'Winner {winner} not found in game {game_id}:\n{teams[0]}\nversus\n{teams[1]}')
                        if game_result != 0:
                            finish_game(conn, game_id, game_result)
                            sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id 
                                      FROM users, wagers 
                                      WHERE game_id = ? AND users.id = wagers.user_id AND result = ? '''
                            cursor = conn.cursor()
                            cursor.execute(sql, (game_id, WAGER_RESULT.InProgress))
                            wagers = cursor.fetchall()
                            for wager in wagers:
                                prediction = GAME_STATUS(wager[2]).name
                                amount: int = wager[3]
                                if prediction in total_amounts:
                                    total_amounts[prediction] += amount
                                else:
                                    total_amounts[prediction] = amount
                            ratio = 0
                            if GAME_STATUS.Team1.name in total_amounts and GAME_STATUS.Team2.name in total_amounts:
                                ta_t1 = total_amounts[GAME_STATUS.Team1.name]
                                ta_t2 = total_amounts[GAME_STATUS.Team2.name]
                                if game_result == GAME_STATUS.Team1:
                                    ratio = (ta_t1 + ta_t2) / ta_t1
                                if game_result == GAME_STATUS.Team2:
                                    ratio = (ta_t1 + ta_t2) / ta_t2
                            for wager in wagers:
                                wager_id: int = wager[0]
                                user_id: int = wager[1]
                                prediction: int = wager[2]
                                amount: int = wager[3]
                                nick: str = wager[4]
                                discord_id: int = wager[5]
                                if game_result == GAME_STATUS.Tied:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.Canceled)
                                    msg = (f'Hi {nick}. The game captained by {" and ".join(captains)} resulted '
                                           f'in a tie. Your bet of {amount} shazbucks has been returned to you.')
                                    await send_dm(user_id, msg)
                                elif ratio == 0:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.Canceled)
                                    msg = (f'Hi {nick}. Nobody took your bet on the game captained by '
                                           f'{" and ".join(captains)}. Your bet of {amount} shazbucks has been '
                                           f'returned to you.')
                                    await send_dm(user_id, msg)
                                elif prediction == game_result:
                                    win_amount = round(amount * ratio)
                                    payout += win_amount
                                    transfer = (bot_user_id, user_id, win_amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.Won)
                                    msg = (f'Hi {nick}. You correctly predicted the game captained by '
                                           f'{" and ".join(captains)}. You have won {win_amount} shazbucks.')
                                    await send_dm(user_id, msg)
                                    user = await get_member(discord_id)
                                    username = user.mention if user else nick
                                    winners_msg += f'{username}({win_amount}) '
                                    no_winners += 1
                                else:
                                    wager_result(conn, wager_id, WAGER_RESULT.Lost)
                                    msg = (f'Hi {nick}. You lost your bet on the game captained by '
                                           f'{" and ".join(captains)}. You have lost your {amount} shazbucks.')
                                    await send_dm(user_id, msg)
                    if game_result is None:
                        result_msg = '\'ERROR: Game not found\''
                    elif game_result == 0:
                        result_msg = '\'ERROR: Winner not found\''
                    elif game_result == GAME_STATUS.Tied:
                        if len(total_amounts) > 0:
                            result_msg = 'All wagers have been returned because the game resulted in a tie.'
                    elif (game_result == GAME_STATUS.Team1 or
                          game_result == GAME_STATUS.Team2):
                        if len(total_amounts) == 1:
                            result_msg = 'The game only had bets on one team. All wagers have been returned.'
                        if len(total_amounts) == 2:
                            verb = "was" if no_winners == 1 else "were"
                            result_msg = f'Game {game_id}: {winners_msg}{verb} paid out a total of {payout} shazbucks.'
                    if result_msg:
                        await message.channel.send(result_msg)
            elif 'has replaced' and 'as captain' in message.content:
                success = False
                new_capt, old_capt = message.content.replace('`', '').replace(' as captain', '').split(' has replaced ')
                sql = ''' SELECT id, team1, team2 FROM games 
                          WHERE (status = ? OR status = ?) AND (team1 LIKE ? OR team2 LIKE ?)'''
                cursor = conn.cursor()
                cursor.execute(sql, (GAME_STATUS.Picking, GAME_STATUS.InProgress, old_capt + '%', old_capt + '%'))
                games = cursor.fetchall()
                if not games:
                    print('PANIC: Captain replaced, but no game with that captain and Picking or InProgress '
                          'status, not sure what game to replace a captain!')
                else:
                    if len(games) > 1:
                        print('PANIC: Captain replaced, but multiple games with that captain and Picking or '
                              'InProgress status, not sure what game to replace a captain! Replacing captain in'
                              'the last game and hoping for the best')
                    game_id: int = games[-1][0]
                    team1: str = games[-1][1]
                    team2: str = games[-1][2]
                    if team1.startswith(old_capt):
                        teams = (team1.replace(old_capt, new_capt), team2)
                        update_teams(conn, game_id, teams)
                        success = True
                    elif team2.startswith(old_capt):
                        teams = (team1, team2.replace(old_capt, new_capt))
                        update_teams(conn, game_id, teams)
                        success = True
                await message.add_reaction(REACTIONS[success])
            elif 'has been substituted with' in message.content:
                success = False
                old_player, new_player = message.content.replace('`', '').split(' has been substituted with ')
                search_str = '%' + old_player + '%'
                sql = ''' SELECT id, team1, team2, status FROM games 
                          WHERE (status = ? OR status = ?) AND (team1 LIKE ? OR team2 LIKE ?)'''
                values = (GAME_STATUS.Picking, GAME_STATUS.InProgress, search_str, search_str)
                cursor = conn.cursor()
                cursor.execute(sql, values)
                games = cursor.fetchall()
                if not games:
                    print('PANIC: Player substituted, but no game with that player and InProgress '
                          'status, not sure what game to substitute the player!')
                else:
                    if len(games) > 1:
                        print('PANIC: Player substituted, but multiple games with that player and InProgress '
                              'status, not sure what game to substitute the player! Substituting the player '
                              'in the last game and hoping for the best')
                    game_id: int = games[-1][0]
                    team1: str = games[-1][1]
                    team2: str = games[-1][2]
                    status: int = games[-1][3]
                    if old_player in team1:
                        teams = (team1.replace(old_player, new_player), team2)
                        update_teams(conn, game_id, teams)
                        if status == GAME_STATUS.InProgress:
                            await cancel_wagers(game_id, 'a player substitution')
                        success = True
                    elif old_player in team2:
                        teams = (team1, team2.replace(old_player, new_player))
                        update_teams(conn, game_id, teams)
                        if status == GAME_STATUS.InProgress:
                            await cancel_wagers(game_id, 'a player substitution')
                        success = True
                await message.add_reaction(REACTIONS[success])
            elif 'has been swapped with' in message.content:
                success = False
                player1, player2 = message.content.replace('`', '').split(' has been swapped with ')
                search_str1 = '%' + player1 + '%'
                search_str2 = '%' + player2 + '%'
                values = (GAME_STATUS.InProgress, search_str1, search_str2, search_str2, search_str1)
                sql = ''' SELECT id, team1, team2 FROM games 
                          WHERE status = ? AND 
                          ((team1 LIKE ? AND team2 LIKE ?) OR (team1 LIKE ? AND team2 LIKE ?))'''
                cursor = conn.cursor()
                cursor.execute(sql, values)  # Don't care about picking
                games = cursor.fetchall()
                if not games:
                    print('PANIC: Players swapped, but no game with those players and InProgress '
                          'status, not sure what game to swap the players!')
                else:
                    if len(games) > 1:
                        print('PANIC: Players swapped, but multiple games with those players and InProgress '
                              'status, not sure what game to swap the players! Swapping the players '
                              'in the last game and hoping for the best')
                    game_id: int = games[-1][0]
                    team1: str = games[-1][1]
                    team2: str = games[-1][2]
                    if player1 in team1 and player2 in team2:
                        team1 = team1.replace(player1, player2)
                        team2 = team2.replace(player2, player1)
                    elif player2 in team1 and player1 in team2:
                        team1 = team1.replace(player2, player1)
                        team2 = team2.replace(player1, player2)
                    teams = (team1, team2)
                    update_teams(conn, game_id, teams)
                    await cancel_wagers(game_id, 'a player swap')
                    success = True
                await message.add_reaction(REACTIONS[success])
        await bot.process_commands(message)

    @bot.event
    async def on_command_error(ctx, error):
        if isinstance(error, commands.errors.CommandNotFound):
            print(ctx.author.name)
            print(error)
        elif isinstance(error, commands.errors.CommandInvokeError):
            print(error)

    bot.run(TOKEN)


# Main
if __name__ == '__main__':
    start_bot()
