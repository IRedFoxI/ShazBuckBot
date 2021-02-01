# -*- coding: utf-8 -*-
"""A discord bot to bet on PUGs."""
import argparse
import asyncio
import atexit
import os
import re
import time

import unicodedata
from enum import IntEnum

import git
import yaml
import sqlite3
from typing import List, Tuple
import logging

import discord
from aiohttp import ClientConnectorError
from discord.ext import commands

config = yaml.safe_load(open("config.yml"))
TOKEN = config['token']
DATABASE = config['database']
DISCORD_ID = config['discord_id']
INIT_BAL = config['init_bal']
BUCKS_PER_PUG = config['bucks_per_pug']
BET_WINDOW = config['bet_window']
BULLYBOT_DISCORD_ID = config['bullybot_discord_id']
REDFOX_DISCORD_ID = config['redfox_discord_id']
PUG_CHANNEL_ID = config['pug_channel_id']
BOT_CHANNEL_ID = config['bot_channel_id']
GAME_STATUS = IntEnum('Game_Status', 'Picking Cancelled InProgress Team1 Team2 Tied')
WAGER_RESULT = IntEnum('Wager_Result', 'InProgress Won Lost Canceled')
DM_TIME_TO_WAIT = 0.21  # Seconds
DURATION_TOLERANCE = 60  # Minutes
REACTIONS = ["👎", "👍"]
MAX_RETRY_COUNT = 10
RETRY_WAIT = 10  # Seconds


def caseless_equal(left, right):
    def normalize_caseless(text):
        return unicodedata.normalize("NFKD", text.casefold())
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


def start_bot(conn):
    bot = commands.Bot(command_prefix='!', loop=asyncio.new_event_loop())
    cur = conn.cursor()
    cur.execute(''' SELECT id FROM users WHERE discord_id = ? ''', (DISCORD_ID,))
    bot_user_id = cur.fetchone()[0]

    def is_admin():
        def predicate(ctx):
            role = discord.utils.get(ctx.guild.roles, name="Developer")
            return ctx.message.author.id == REDFOX_DISCORD_ID or role in ctx.author.roles
        return commands.check(predicate)

    async def fetch_member(discord_id) -> discord.Member:
        """Find the discord member based on their discord id

        :param int discord_id: The discord id of the user
        """
        member = None
        for guild in bot.guilds:
            if guild.get_channel(BOT_CHANNEL_ID):
                try:
                    member = await guild.fetch_member(discord_id)
                except discord.NotFound as e:
                    logger.error(f'Unable to fetch discord member by id {discord_id}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
                except asyncio.exceptions.TimeoutError as e:
                    logger.error(f'Unable to fetch discord member by id {discord_id}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
        return member

    async def query_members(nick) -> discord.Member:
        """Find the discord member based on their discord id

        :param str nick: The nick of the user
        """
        member = None
        for guild in bot.guilds:
            if guild.get_channel(BOT_CHANNEL_ID):
                try:
                    members: List[discord.Member] = await guild.query_members(nick)
                    if members:
                        for m in members:
                            if m.display_name == nick:
                                member = m
                    if not member:
                        logger.error(f'Unable to fetch discord member from nickname {nick}: unable to find player.')
                except discord.NotFound as e:
                    logger.error(f'Unable to fetch discord member from nickname {nick}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
                except asyncio.exceptions.TimeoutError as e:
                    logger.error(f'Unable to fetch discord member from nickname {nick}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
        return member

    async def send_dm(user_id, message) -> None:
        """Send a discord DM to the user

        :param int user_id: User id in database
        :param str message: The message to be send to the user
        """
        (discord_id, mute_dm) = get_user_data(conn, user_id, 'discord_id, mute_dm')
        if not mute_dm:
            user = await fetch_member(discord_id)
            if user:
                try:
                    await asyncio.sleep(DM_TIME_TO_WAIT)
                    await user.create_dm()
                    await user.dm_channel.send(message)
                except discord.Forbidden as e:
                    logger.error(f'Unable to direct message discord member {user.display_name}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')

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
        logger.info(f'{bot.user} is connected to the following guild(s):')
        for guild in bot.guilds:
            logger.info(f'\t\t{guild.name}(id: {guild.id})')

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
                logger.error(
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
                        logger.error(f'{ctx.author.name} tried to gift {amount} shazbucks to {receiver_nick} '
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
                        team_id_str: str = games[-1][1]
                        capt_id = int(team_id_str.split()[0])
                        winner = (await fetch_member(capt_id)).display_name
                        time_since_pick = games[-1][3]
                    elif winner == "2" or caseless_equal(winner, "Blue"):
                        prediction += GAME_STATUS.Team2
                        team_id_str: str = games[-1][2]
                        capt_id = int(team_id_str.split()[0])
                        winner = (await fetch_member(capt_id)).display_name
                        time_since_pick = games[-1][3]
                    else:
                        for game in games:
                            team_id_strs: Tuple[str, str] = game[1:3]
                            capt_ids = [int(team_id_str.split()[0]) for team_id_str in team_id_strs]
                            capt_nicks = tuple([(await fetch_member(did)).display_name for did in capt_ids])
                            if caseless_equal(winner, capt_nicks[0]):
                                game_id: int = game[0]
                                prediction += GAME_STATUS.Team1
                                time_since_pick = game[3]
                                winner = capt_nicks[0]
                            elif caseless_equal(winner, capt_nicks[1]):
                                game_id: int = game[0]
                                prediction += GAME_STATUS.Team2
                                time_since_pick = game[3]
                                winner = capt_nicks[1]
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
                                logger.error(f'{nick} tried to bet {amount} shazbucks on {winner} but something '
                                             f'went wrong. User id {user_id}, game id {game_id}, prediction '
                                             f'{prediction}.')
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
            sql = ''' SELECT id, team1, team2, queue, status, 
                      CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER)
                      FROM games WHERE status = ? OR status = ?'''
            cursor = conn.cursor()
            cursor.execute(sql, (GAME_STATUS.Picking, GAME_STATUS.InProgress))
            games = cursor.fetchall()
            if not games:
                msg = f'Hi {nick}. No games are running.'
                await send_dm(user_id, msg)
            else:
                show_str = ''
                for game in games:
                    game_id = game[0]
                    teams = game[1:3]
                    capt_ids = [team.split()[0] for team in teams]
                    capt_nicks = [(await fetch_member(did)).display_name for did in capt_ids]
                    queue = game[3]
                    game_status = game[4]
                    run_time = game[5]
                    cursor = conn.cursor()
                    sql = ''' SELECT prediction, amount FROM wagers WHERE game_id = ? AND result = ? '''
                    cursor.execute(sql, (game_id, WAGER_RESULT.InProgress))
                    wagers = cursor.fetchall()
                    total_amounts = {GAME_STATUS.Team1: 0, GAME_STATUS.Team2: 0}
                    for wager in wagers:
                        prediction = GAME_STATUS(wager[0])
                        amount: int = wager[1]
                        total_amounts[prediction] += amount
                    if game_status == GAME_STATUS.Picking:
                        show_str += (f'{queue}: Game {game_id} (Picking): '
                                     f'{capt_nicks[0]} vs '
                                     f'{capt_nicks[1]}\n')
                    elif game_status == GAME_STATUS.InProgress:
                        if run_time <= BET_WINDOW:
                            show_str += (f'{queue}: Game {game_id} ({BET_WINDOW - run_time} minutes left to bet): '
                                         f'{capt_nicks[0]}({total_amounts[GAME_STATUS.Team1]}) vs '
                                         f'{capt_nicks[1]}({total_amounts[GAME_STATUS.Team2]})\n')
                        else:
                            show_str += (f'{queue}: Game {game_id} (Betting closed): '
                                         f'{capt_nicks[0]}({total_amounts[GAME_STATUS.Team1]}) vs '
                                         f'{capt_nicks[1]}({total_amounts[GAME_STATUS.Team2]})\n')
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
                    member = await fetch_member(discord_id)
                    username = member.display_name if member else nick
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

    @bot.command(name='beggars', help='Show the top 5 begging players')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_beggars(ctx):
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
            sql = ''' SELECT nick, discord_id, SUM(CASE WHEN users.id = receiver THEN amount ELSE -amount END) 
                      AS total_sender_amount FROM users, transfers 
                      WHERE (users.id = receiver or users.id = sender) 
                      AND sender <> 1 AND receiver <> 1 AND sender <> receiver 
                      GROUP BY nick ORDER BY total_sender_amount DESC LIMIT 5; '''
            cursor = conn.cursor()
            cursor.execute(sql)
            users = cursor.fetchall()
            if not users:
                msg = f'Hi {nick}. Something went wrong, no top 5 beggars.'
                await send_dm(user_id, msg)
            else:
                top5_str = 'The top 5 players who received the most shazbucks are: '
                for i, user in enumerate(users):
                    nick: str = user[0]
                    discord_id: int = user[1]
                    amount: int = user[2]
                    member = await fetch_member(discord_id)
                    username = member.display_name if member else nick
                    top5_str += f'{username} ({amount})'
                    if i < len(users) - 2:
                        top5_str += ', '
                    elif i == len(users) - 2:
                        top5_str += ' and '
                    else:
                        top5_str += '.'
                await ctx.send(top5_str)
                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='philanthropists', help='Show the top 5 gifting players')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_philanthropists(ctx):
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
            sql = ''' SELECT nick, discord_id, SUM(CASE WHEN users.id = sender THEN amount ELSE -amount END) 
                      AS total_sender_amount FROM users, transfers 
                      WHERE (users.id = receiver or users.id = sender) 
                      AND sender <> 1 AND receiver <> 1 AND sender <> receiver 
                      GROUP BY nick ORDER BY total_sender_amount DESC LIMIT 5; '''
            cursor = conn.cursor()
            cursor.execute(sql)
            users = cursor.fetchall()
            if not users:
                msg = f'Hi {nick}. Something went wrong, no top 5 gifters.'
                await send_dm(user_id, msg)
            else:
                top5_str = 'The top 5 players who gifted the most shazbucks are: '
                for i, user in enumerate(users):
                    nick: str = user[0]
                    discord_id: int = user[1]
                    amount: int = user[2]
                    member = await fetch_member(discord_id)
                    username = member.display_name if member else nick
                    top5_str += f'{username} ({amount})'
                    if i < len(users) - 2:
                        top5_str += ', '
                    elif i == len(users) - 2:
                        top5_str += ' and '
                    else:
                        top5_str += '.'
                await ctx.send(top5_str)
                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='graph', help='Show a graph of your shazbucks over time')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_graph(ctx, members: commands.Greedy[discord.Member], *, with_gifts=False):
        # success = False
        discord_ids = []
        if len(members) == 0:
            discord_ids.append(str(ctx.author.id))
        else:
            for member in members:
                discord_ids.append(str(member.id))
        ids_str = '&discord_id='.join(discord_ids)
        graph_url = f'https://club77.org/shazbuckbot/usergraph.py?discord_id={ids_str}'
        if with_gifts:
            graph_url += '&gift=true'
        e = discord.Embed(title='')
        e.set_image(url=graph_url)
        await ctx.send(embed=e)
        # await ctx.send(graph_url)
        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='quit', help='Shutdown bot')
    @in_channel(BOT_CHANNEL_ID)
    @is_admin()
    async def cmd_quit(ctx):
        logger.info(f'{ctx.author.display_name} requested bot shutdown.')
        success = True
        await ctx.message.add_reaction(REACTIONS[success])
        await bot.close()

    @bot.command(name='restart', help='Restart bot')
    @in_channel(BOT_CHANNEL_ID)
    @is_admin()
    async def cmd_restart(ctx):
        logger.info(f'{ctx.author.display_name} requested bot restart.')
        success = True
        await ctx.message.add_reaction(REACTIONS[success])
        atexit.register(os.system, f'python3 {__file__}')
        await bot.close()

    @bot.command(name='update', help='Update bot using git')
    @in_channel(BOT_CHANNEL_ID)
    @is_admin()
    async def cmd_update(ctx):
        logger.info(f'{ctx.author.display_name} requested bot update.')
        success = False
        repo = git.Repo('./')
        current_commit = repo.head.commit
        try:
            repo.remotes.origin.pull()
            if current_commit == repo.head.commit:
                logger.info('No change or ahead of repo.')
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, no update available!')

            else:
                logger.info('Updated successfully.')
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, updated successfully!')
            success = True
        except git.GitCommandError as e:
            logger.error('Git command did not complete correctly:')
            for line in str(e).split('\n'):
                logger.error(f'\t{line}')
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, update did not complete successfully!')
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='changelog', help='Show changelog')
    @in_channel(BOT_CHANNEL_ID)
    @is_admin()
    async def cmd_update(ctx):
        logger.info(f'{ctx.author.display_name} requested changelog.')
        success = False
        repo = git.Repo('./')
        try:
            log = repo.heads.master.log()
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, these are the latest commits:')
            for entry in log:
                await ctx.author.dm_channel.send(f'{entry}')
            success = True
        except git.GitCommandError as e:
            logger.error('Git command did not complete correctly:')
            for line in str(e).split('\n'):
                logger.error(f'\t{line}')
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, error showing changelog!')
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='change_game', help='Change the outcome of a game')
    @is_admin()
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
                team_id_strs: Tuple[str, str] = games[0:2]
                capt_ids = [int(team_id_str.split()[0]) for team_id_str in team_id_strs]
                capt_nicks = tuple([(await fetch_member(did)).display_name for did in capt_ids])
                old_status = games[2]
                new_status = None
                if result in ['1', 'Red', 'red', 'Team1', 'team1', capt_nicks[0]]:
                    new_status = GAME_STATUS.Team1
                elif result in ['2', 'Blue', 'blue', 'Team2', 'team2', capt_nicks[1]]:
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
                        total_amounts = {}
                        winners = []
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
                            # Set the status of the game back to InProgress
                            finish_game(conn, game_id, GAME_STATUS.InProgress)
                            # Claw back previous payout
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
                                           f'{" and ".join(capt_nicks)}, was changed. Your bet of {amount} shazbucks '
                                           f'has been placed again.')
                                    await send_dm(user_id, msg)
                                elif ratio == 0:
                                    transfer = (user_id, bot_user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                                    msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                           f'{" and ".join(capt_nicks)}, was changed. Your bet of {amount} shazbucks '
                                           f'has been placed again.')
                                    await send_dm(user_id, msg)
                                elif prediction == old_status:
                                    win_amount = round(amount * ratio)
                                    transfer = (user_id, bot_user_id, win_amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                                    msg = (f'Hi {nick}. The result of game {game_id}, captained by '
                                           f'{" and ".join(capt_nicks)}, was changed. Your previous payout of '
                                           f'{win_amount} shazbucks has been clawed back.')
                                    await send_dm(user_id, msg)
                                    user = await fetch_member(discord_id)
                                    winners.append((user, win_amount))
                                else:
                                    wager_result(conn, wager_id, WAGER_RESULT.InProgress)
                            result_msg = ''
                            if old_status == GAME_STATUS.Tied:
                                if len(total_amounts) > 0:
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(capt_nicks)}, was changed. All wagers have been '
                                                  f'placed again.')
                            elif (old_status == GAME_STATUS.Team1 or
                                  old_status == GAME_STATUS.Team2):
                                if len(total_amounts) == 1:
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(capt_nicks)}, was changed. All wagers have been '
                                                  f'placed again.')
                                if len(total_amounts) == 2:
                                    verb = "was" if len(winners) == 1 else "were"
                                    winners_str = ', '.join([f'{user.display_name}({win_amount})' for
                                                             (user, win_amount) in winners])
                                    payout = sum([win_amount for (user, win_amount) in winners])
                                    result_msg = (f'The result of game {game_id}, captained by '
                                                  f'{" and ".join(capt_nicks)}, was changed. The winnings of '
                                                  f'{winners_str} for a total of {payout} shazbucks {verb} clawed '
                                                  f'back.')
                            if result_msg:
                                await ctx.send(result_msg)
                        # Set the status of the game to the new result
                        finish_game(conn, game_id, new_status)
                        # Payout based on new result
                        total_amounts, winners = await resolve_wagers(game_id, new_status, capt_nicks, True)
                        result_msg = ''
                        if new_status == GAME_STATUS.Tied:
                            if len(total_amounts) > 0:
                                result_msg = (f'The result of game {game_id}, captained by {" and ".join(capt_nicks)}, '
                                              f'was changed to a tie. All wagers have been returned.')
                                logger.info(f'Game {game_id} was changed by {nick} to a tie and all wagers have been '
                                            f'returned.')
                        elif (new_status == GAME_STATUS.Team1 or
                              new_status == GAME_STATUS.Team2):
                            if len(total_amounts) == 1:
                                result_msg = (f'The result of game {game_id}, captained by {" and ".join(capt_nicks)}, '
                                              f'was changed. The game only had bets on one team. All wagers have been '
                                              f'returned.')
                                logger.info(f'Game {game_id} was changed by {nick} to a win for {new_status.name}, '
                                            f'but the game only had bets on one team. All wagers have been returned.')
                            if len(total_amounts) == 2:
                                verb = "was" if len(winners) == 1 else "were"
                                winners_str = ', '.join([f'{user.display_name}({win_amount})' for
                                                         (user, win_amount) in winners])
                                payout = sum([win_amount for (user, win_amount) in winners])
                                result_msg = (f'The result of game {game_id}, captained by {" and ".join(capt_nicks)}, '
                                              f'was changed. {winners_str} {verb} paid out a total of {payout} '
                                              f'shazbucks.')
                                logger.info(f'Game {game_id} was changed by {nick} to a win for {new_status.name}. '
                                            f'{winners_str} {verb} paid out a total of {payout} shazbucks.')
                        if result_msg:
                            await ctx.send(result_msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    async def game_begun(message: discord.Message):
        queue = message.content.split("'")[1]
        description = ''
        if message.embeds:
            description = message.embeds[0].description
        descr_lines = description.split('\n')
        captains_str = descr_lines[0].replace('**', '').replace('Captains:', '').replace('&', '')
        pattern = '[<@!>]'
        player_id_strs = re.sub(pattern, '', captains_str).split()
        player_nicks = []
        for capt_id in player_id_strs:
            member = await fetch_member(int(capt_id))
            player_nicks.append(member.display_name)
        for nick in descr_lines[1].split(', '):
            player_nicks.append(nick)
            player = await query_members(nick)
            if player:
                player_id_strs[0] += f' {player.id}'
            else:
                logger.error(f'Could not find discord id for player {nick}')
        team_id_strs = tuple(player_id_strs)
        game = (queue,) + team_id_strs
        game_id = create_game(conn, game)
        logger.info(f'Game {game_id} created in the {queue} queue: {" ".join(player_nicks)}')
        await message.add_reaction(REACTIONS[True])

    async def game_picked(message: discord.Message):
        queue = message.content.split("'")[1]
        description = ''
        if message.embeds:
            description = message.embeds[0].description
        team_strs = description.split('\n')[1:3]
        capt_nicks = tuple([team_str.split(':')[0] for team_str in team_strs])
        team_id_strs: Tuple[str, ...] = ()
        for team_str in team_strs:
            id_strs = []
            players = team_str.replace(':', ',').split(', ')
            for nick in players:
                member = await query_members(nick)
                if member:
                    id_strs.append(str(member.id))
                else:
                    logger.error(f'Could not find discord id for player {nick}')
            team_id_strs += (" ".join(id_strs),)
        # Find all games that are Picking or InProgress (in case of a repick) sorted latest first
        sql = ''' SELECT id, team1, team2, status FROM games WHERE queue = ? AND (status = ? OR status = ?) 
                  ORDER BY start_date DESC '''
        cursor = conn.cursor()
        cursor.execute(sql, (queue, GAME_STATUS.Picking, GAME_STATUS.InProgress))
        games = cursor.fetchall()
        if not games:
            logger.error(f'Game picked in {queue} queue, but no game with Picking or InProgress status in that queue!')
            game = (queue,) + team_id_strs
            game_id = create_game(conn, game)
            game_status = GAME_STATUS.Picking
            logger.info(f'Game {game_id} created in the {queue} queue: {" versus ".join(team_strs)}')
        else:
            game_id = 0
            game_status = 0
            # For each returned game, find the names of the captains
            for game in games:
                team1_id_str: str = game[1]
                team2_id_str: str = game[2]
                capt1_id = int(team1_id_str.split()[0])
                capt2_id = int(team2_id_str.split()[0])
                try:
                    capt1_nick = (await fetch_member(capt1_id)).display_name
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.error(f'Unable to find discord member with id {capt1_id}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
                    capt1_nick = str(capt1_id)
                try:
                    capt2_nick = (await fetch_member(capt2_id)).display_name
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.error(f'Unable to find discord member with id {capt2_id}:')
                    for line in str(e).split('\n'):
                        logger.error(f'\t{line}')
                    capt2_nick = str(capt2_id)
                if capt_nicks == (capt1_nick, capt2_nick):
                    game_id: int = game[0]
                    game_status: int = game[3]
                    team_id_strs = (str(capt1_id) + " " + " ".join(team_id_strs[0].split()[1:]),
                                    str(capt2_id) + " " + " ".join(team_id_strs[1].split()[1:]))
                    break
                if capt_nicks == (capt2_nick, capt1_nick):
                    game_id: int = game[0]
                    game_status: int = game[3]
                    team_id_strs = (str(capt2_id) + " " + " ".join(team_id_strs[0].split()[1:]),
                                    str(capt1_id) + " " + " ".join(team_id_strs[1].split()[1:]))
                    break
            # Create game if no game found with the correct captains
            if game_id == 0:
                logger.error(f'Game picked in {queue} queue, but no game with Picking or InProgress status and '
                             f'captains {" and ".join(capt_nicks)} in that queue!')
                game = (queue,) + team_id_strs
                game_id = create_game(conn, game)
                game_status = GAME_STATUS.Picking
                logger.info(f'Game {game_id} created in the {queue} queue: {" versus ".join(team_strs)}')
        # Cancel wagers if there is a repick
        if game_status == GAME_STATUS.InProgress:
            await cancel_wagers(game_id, 'a repick')
        # Update database and log
        pick_game(conn, game_id, team_id_strs)
        logger.info(f'Game {game_id} picked in the {queue} queue: {" versus ".join(team_strs)}')
        await message.add_reaction(REACTIONS[True])

    async def game_cancelled(message: discord.Message):
        success = False
        # Find the game that was just cancelled
        cursor = conn.cursor()
        cursor.execute(''' SELECT id FROM games WHERE status = ? ''', (GAME_STATUS.Picking,))
        games = cursor.fetchall()
        if not games:
            logger.error('Game cancelled, but no game with Picking status, not sure what game to cancel!')
        elif len(games) > 1:
            logger.error('Game cancelled, but multiple games with Picking status, not sure what game to cancel!')
        else:
            game_id: int = games[0][0]
            cancel_game(conn, game_id)
            logger.info(f'Game {game_id} cancelled, hopefully it was the right one!')
            success = True
        await message.add_reaction(REACTIONS[success])

    async def game_finished(message: discord.Message):
        queue = message.content.split("'")[1]
        description = ''
        if message.embeds:
            description = message.embeds[0].description
        [result, duration] = description.split('\n')
        duration = int(duration.split(' ')[1])
        game_result = GAME_STATUS.Tied if 'Tie' in result else None
        winner_nick = ''
        winner_id = 0
        total_amounts = {}
        winners = []
        # Find the game that just finished
        game_id = 0
        if game_result == GAME_STATUS.Tied:
            game_values = (queue, GAME_STATUS.InProgress, duration, DURATION_TOLERANCE)
            sql = ''' SELECT id, ABS(CAST (((julianday('now') - julianday(start_time, 'unixepoch')) 
                      * 24 * 60) AS INTEGER)), team1, team2 
                      FROM games 
                      WHERE queue = ? AND status = ? AND ABS(CAST (((julianday('now') 
                      - julianday(start_time, 'unixepoch')) * 24 * 60) AS INTEGER)) - ? <= ? '''
        else:
            winner_nick = " ".join(result.split(' ')[2:])
            winner_id = (await query_members(winner_nick)).id
            winner_id_str = str(winner_id)
            game_values = (queue, GAME_STATUS.InProgress, winner_id_str + '%', winner_id_str + '%')
            sql = ''' SELECT id, ABS(CAST (((julianday('now') - julianday(start_time, 'unixepoch')) 
                      * 24 * 60) AS INTEGER)),team1, team2 FROM games 
                      WHERE queue = ? AND status = ? AND (team1 LIKE ? OR team2 LIKE ?) '''
        cursor = conn.cursor()
        cursor.execute(sql, game_values)
        games = cursor.fetchall()
        if not games:
            if game_result == GAME_STATUS.Tied:
                game_result = None
                logger.error(f'Game finished with a tie in {queue} queue, but no game with InProgress status and '
                             f'correct time in that queue.')
            else:
                logger.error(f'Game finished with a win for {winner_nick} in {queue} queue, but no game with '
                             f'InProgress status and that captain in that queue.')
        else:
            game_id: int = games[0][0]
            team_id_strs: Tuple[str, str] = games[0][2:4]
            # If multiple games running in the same queue match, select the game which duration matches most closely
            if len(games) > 1:
                duration_offsets: List[int] = [game[1] for game in games]
                _, idx = min((val, idx) for (idx, val) in enumerate(duration_offsets))
                game_id: int = games[idx][0]
                team_id_strs: Tuple[str, str] = games[idx][2:4]
            # Create a list of discord members per team
            teams = ()
            for team_str in team_id_strs:
                team = []
                for discord_id_str in team_str.split():
                    if discord_id_str.isdigit():
                        player = await fetch_member(int(discord_id_str))
                        if player:
                            team.append(player)
                teams += (team,)
            # Cache captain info
            capt_ids = (teams[0][0].id, teams[1][0].id)
            capt_nicks = (teams[0][0].display_name, teams[1][0].display_name)
            # Establish result if not tied
            if game_result != GAME_STATUS.Tied:
                if winner_id == capt_ids[0]:
                    game_result = GAME_STATUS.Team1
                elif winner_id == capt_ids[1]:
                    game_result = GAME_STATUS.Team2
                else:
                    game_result = 0
                    logger.error(f'Winner {winner_nick} ({winner_id}) not found in game {game_id}: {capt_nicks[0]} '
                                 f'versus {capt_nicks[1]}')
            # Update the database, resolve wagers and pay the participants
            if game_result:
                finish_game(conn, game_id, game_result)
                total_amounts, winners = await resolve_wagers(game_id, game_result, capt_nicks)
                await pay_players(teams)
        # Send summary message to the channel, unless nobody placed a bet
        result_msg = ''
        if game_result is None:
            result_msg = '\'ERROR: Game not found\''
        elif game_result == 0:
            result_msg = '\'ERROR: Winner not found\''
        elif game_result == GAME_STATUS.Tied:
            if len(total_amounts) > 0:
                result_msg = 'All wagers have been returned because the game resulted in a tie.'
                logger.info(f'Game {game_id} finished with a tie and all wagers have been returned.')
        elif (game_result == GAME_STATUS.Team1 or
              game_result == GAME_STATUS.Team2):
            if len(total_amounts) == 0:
                logger.info(f'Game {game_id} finished with a win for {game_result.name}, but the game had no bets.')
            elif len(total_amounts) == 1:
                result_msg = 'The game only had bets on one team. All wagers have been returned.'
                logger.info(f'Game {game_id} finished with a win for {game_result.name}, but the game only had bets '
                            f'on one team. All wagers have been returned.')
            elif len(total_amounts) == 2:
                verb = "was" if len(winners) == 1 else "were"
                winners_str = ', '.join([f'{user.display_name}({win_amount})' for
                                         (user, win_amount) in winners])
                payout = sum([win_amount for (user, win_amount) in winners])
                result_msg = f'Game {game_id}: {winners_str} {verb} paid out a total of {payout} shazbucks.'
                logger.info(f'Game {game_id} finished with a win for {game_result.name}. {winners_str} {verb} paid '
                            f'out a total of {payout} shazbucks.')
        if result_msg:
            await message.channel.send(result_msg)

    async def resolve_wagers(game_id, game_result, capt_nicks, change=False) -> Tuple[dict,
                                                                                      List[Tuple[discord.Member, int]]]:
        """Resolve wagers placed on a game based on its outcome

        :param int game_id: ID of the game
        :param int game_result: Result of the game
        :param Tuple[str, str] capt_nicks: Tuple of captain nicks
        :param bool change: Boolean indicating whether the result of the game is being changed
        :return: a dictionary with the total amounts bet on each team and a dictionary with the amount won by each
            winner
        """
        # Initialize parameters
        total_amounts = {}
        ratio = 0
        winners = []
        # Find wagers on this game
        sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id FROM users, wagers 
                  WHERE game_id = ? AND users.id = wagers.user_id AND result = ? '''
        cursor = conn.cursor()
        cursor.execute(sql, (game_id, WAGER_RESULT.InProgress))
        wagers = cursor.fetchall()
        # Calculate the total amounts bet on each team
        for wager in wagers:
            prediction = GAME_STATUS(wager[2]).name
            amount: int = wager[3]
            if prediction in total_amounts:
                total_amounts[prediction] += amount
            else:
                total_amounts[prediction] = amount
        # If bets are placed on both sides, calculate the ratio between them
        if GAME_STATUS.Team1.name in total_amounts and GAME_STATUS.Team2.name in total_amounts:
            ta_t1 = total_amounts[GAME_STATUS.Team1.name]
            ta_t2 = total_amounts[GAME_STATUS.Team2.name]
            if game_result == GAME_STATUS.Team1:
                ratio = (ta_t1 + ta_t2) / ta_t1
            if game_result == GAME_STATUS.Team2:
                ratio = (ta_t1 + ta_t2) / ta_t2
        # Resolve each individual bet
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
                if change:
                    msg = (f'Hi {nick}. The game captained by {" and ".join(capt_nicks)} was changed to '
                           f'a tie. Your bet of {amount} shazbucks has been returned to you.')
                else:
                    msg = (f'Hi {nick}. The game captained by {" and ".join(capt_nicks)} resulted '
                           f'in a tie. Your bet of {amount} shazbucks has been returned to you.')
                await send_dm(user_id, msg)
            elif ratio == 0:
                transfer = (bot_user_id, user_id, amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WAGER_RESULT.Canceled)
                if change:
                    msg = (f'Hi {nick}. The game captained by {" and ".join(capt_nicks)} was changed. Nobody took '
                           f'your bet or the game was cancelled. Your bet of {amount} shazbucks has been returned '
                           f'to you.')
                else:
                    msg = (f'Hi {nick}. Nobody took your bet on the game captained by '
                           f'{" and ".join(capt_nicks)}. Your bet of {amount} shazbucks has been '
                           f'returned to you.')
                await send_dm(user_id, msg)
            elif prediction == game_result:
                win_amount = round(amount * ratio)
                transfer = (bot_user_id, user_id, win_amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WAGER_RESULT.Won)
                if change:
                    msg = (f'Hi {nick}. The game captained by {" and ".join(capt_nicks)} was changed. You correctly '
                           f'predicted the new result and have won {win_amount} shazbucks.')
                else:
                    msg = (f'Hi {nick}. You correctly predicted the game captained by '
                           f'{" and ".join(capt_nicks)}. You have won {win_amount} shazbucks.')
                await send_dm(user_id, msg)
                user = await fetch_member(discord_id)
                winners.append((user, win_amount))
            else:
                wager_result(conn, wager_id, WAGER_RESULT.Lost)
                if change:
                    msg = (f'Hi {nick}. The game captained by {" and ".join(capt_nicks)} was changed. You did not '
                           f'predict the new result correctly and have lost your {amount} shazbucks.')
                else:
                    msg = (f'Hi {nick}. You lost your bet on the game captained by '
                           f'{" and ".join(capt_nicks)}. You have lost your {amount} shazbucks.')
                await send_dm(user_id, msg)
        # Return the total amount bet on each team and the winners and how much they won
        return total_amounts, winners

    async def pay_players(teams):
        """Pay the players for participating in a PuG

        :param Tuple[List[discord.Member], List[discord.Member]] teams: Tuple of List per team of discord.Member
        """
        # Cache captain info
        capt_nicks = (teams[0][0].display_name, teams[1][0].display_name)
        for idx, team in enumerate(teams):
            captain = True
            for player in team:
                cursor = conn.cursor()
                cursor.execute(''' SELECT id, nick FROM users WHERE discord_id = ? ''',
                               (player.id,))
                user = cursor.fetchone()
                if user:
                    user_id: int = user[0]
                    nick: str = user[1]
                    if captain:
                        captain = False
                        index = 1 - idx
                        transfer = (bot_user_id, user_id, BUCKS_PER_PUG * 2)
                        create_transfer(conn, transfer)
                        msg = (f'Hi {nick}. You captained a game against {capt_nicks[index]}. For '
                               f'your efforts you have been rewarded {BUCKS_PER_PUG * 2} shazbucks')
                        await send_dm(user_id, msg)
                    else:
                        transfer = (bot_user_id, user_id, BUCKS_PER_PUG)
                        create_transfer(conn, transfer)
                        msg = (f'Hi {nick}. You played a game captained by {" and ".join(capt_nicks)}. '
                               f'For your efforts you have been rewarded {BUCKS_PER_PUG} shazbucks')
                        await send_dm(user_id, msg)

    async def replaced_captain(message):
        success = False
        new_capt, old_capt = message.content.replace('`', '').replace(' as captain', '').split(' has replaced ')
        new_capt_id_str = str((await query_members(new_capt)).id)
        old_capt_id_str = str((await query_members(old_capt)).id)
        search_str = f'{old_capt_id_str}%'
        sql = ''' SELECT id, team1, team2 FROM games 
                  WHERE (status = ? OR status = ?) AND (team1 LIKE ? OR team2 LIKE ?)'''
        cursor = conn.cursor()
        cursor.execute(sql, (GAME_STATUS.Picking, GAME_STATUS.InProgress, search_str, search_str))
        games = cursor.fetchall()
        if not games:
            logger.error(f'Captain replaced, but no game with {old_capt} as captain and Picking or InProgress '
                         f'status, not sure what game to replace a captain!')
        else:
            if len(games) > 1:
                logger.warning(f'Captain replaced, but multiple games with {old_capt} as captain and Picking or '
                               f'InProgress status, not sure what game to replace {old_capt}! Replacing '
                               f'{old_capt} in the last game and hoping for the best!')
            game_id: int = games[-1][0]
            team1: str = games[-1][1]
            team2: str = games[-1][2]
            if (old_capt_id_str in team1 or team2) and (new_capt_id_str in team1 or team2):
                team1 = team1.replace(old_capt_id_str, '#')
                team2 = team2.replace(old_capt_id_str, '#')
                team1 = team1.replace(new_capt_id_str, old_capt_id_str)
                team2 = team2.replace(new_capt_id_str, old_capt_id_str)
                teams = (team1.replace('#', new_capt_id_str), team2.replace('#', new_capt_id_str))
                update_teams(conn, game_id, teams)
                logger.info(f'Captain {old_capt} replaced by {new_capt} in game {game_id}.')
                success = True
            else:
                logger.error(f'Captain replaced, and found game {game_id} with {old_capt} as captain and '
                             f'Picking or InProgress status, but did not find {new_capt} in that game!')
        await message.add_reaction(REACTIONS[success])

    async def sub_player(message):
        success = False
        old_player, new_player = message.content.replace('`', '').split(' has been substituted with ')
        old_player_id_str = str((await query_members(old_player)).id)
        new_player_id_str = str((await query_members(new_player)).id)
        search_str = f'%{old_player_id_str}%'
        sql = ''' SELECT id, team1, team2, status FROM games 
                  WHERE (status = ? OR status = ?) AND (team1 LIKE ? OR team2 LIKE ?)'''
        cursor = conn.cursor()
        cursor.execute(sql, (GAME_STATUS.Picking, GAME_STATUS.InProgress, search_str, search_str))
        games = cursor.fetchall()
        if not games:
            logger.error(f'Player {old_player} substituted with {new_player}, but no game with that player and '
                         f'Picking or InProgress status, not sure what game to substitute the player!')
        else:
            if len(games) > 1:
                logger.warning(f'Player {old_player} substituted with {new_player}, but multiple games with that '
                               f'player and Picking or InProgress status, not sure what game to substitute the player! '
                               f'Substituting the player in the last game and hoping for the best!')
            game_id: int = games[-1][0]
            team1: str = games[-1][1]
            team2: str = games[-1][2]
            status: int = games[-1][3]
            if old_player_id_str in team1:
                teams = (team1.replace(old_player_id_str, new_player_id_str), team2)
                update_teams(conn, game_id, teams)
                if status == GAME_STATUS.InProgress:
                    await cancel_wagers(game_id, 'a player substitution')
                logger.info(f'Player {old_player} replaced by {new_player} in game {game_id}.')
                success = True
            elif old_player_id_str in team2:
                teams = (team1, team2.replace(old_player_id_str, new_player_id_str))
                update_teams(conn, game_id, teams)
                if status == GAME_STATUS.InProgress:
                    await cancel_wagers(game_id, 'a player substitution')
                logger.info(f'Player {old_player} replaced by {new_player} in game {game_id}.')
                success = True
        await message.add_reaction(REACTIONS[success])

    async def swap_player(message):
        success = False
        player1, player2 = message.content.replace('`', '').split(' has been swapped with ')
        player1_id_str = str((await query_members(player1)).id)
        player2_id_str = str((await query_members(player2)).id)
        search_str1 = f'%{player1_id_str}%'
        search_str2 = f'%{player2_id_str}%'
        values = (GAME_STATUS.InProgress, search_str1, search_str2, search_str2, search_str1)
        sql = ''' SELECT id, team1, team2 FROM games 
                  WHERE status = ? AND 
                  ((team1 LIKE ? AND team2 LIKE ?) OR (team1 LIKE ? AND team2 LIKE ?))'''
        cursor = conn.cursor()
        cursor.execute(sql, values)
        games = cursor.fetchall()
        if not games:
            logger.error(f'Players swapped, but no game with {player1} and {player2} and InProgress status, not sure '
                         f'what game to swap the players!')
        else:
            if len(games) > 1:
                logger.warning(f'Players swapped, but multiple games with {player1} and {player2} and InProgress '
                               f'status, not sure what game to swap the players! Swapping the players in the last '
                               f'game and hoping for the best!')
            game_id: int = games[-1][0]
            team1: str = games[-1][1]
            team2: str = games[-1][2]
            if player1_id_str in team1 and player2_id_str in team2:
                teams = (team1.replace(player1_id_str, player2_id_str), team2.replace(player2_id_str, player1_id_str))
                update_teams(conn, game_id, teams)
                await cancel_wagers(game_id, 'a player swap')
                logger.info(f'Player {player1} swapped with {player2} in game {game_id}.')
                success = True
            elif player1_id_str in team2 and player2_id_str in team1:
                teams = (team1.replace(player2_id_str, player1_id_str), team2.replace(player1_id_str, player2_id_str))
                update_teams(conn, game_id, teams)
                await cancel_wagers(game_id, 'a player swap')
                logger.info(f'Player {player1} swapped with {player2} in game {game_id}.')
                success = True
            else:
                logger.error(f'Player {player1} and {player2} swapped, and found game {game_id} with those players '
                             f'and InProgress status, but something went wrong!')
        await message.add_reaction(REACTIONS[success])

    @bot.event
    async def on_message(message):
        # Log messages for debugging purposes
        if (message.author.id == BULLYBOT_DISCORD_ID
                or message.author.id == DISCORD_ID):
            logger.debug(f'{message.author} wrote in #{message.channel} on '
                         f'{message.guild}:')
            for line in message.content.split('\n'):
                logger.debug(f'\t{line}')
            for embed in message.embeds:
                logger.debug(f'\t{repr(embed.title)}')
                if embed.description:
                    for line in embed.description.split('\n'):
                        logger.debug(f'\t\t{line}')
        # Parse BullyBot's messages for game info
        if message.author.id == BULLYBOT_DISCORD_ID and message.channel.id == PUG_CHANNEL_ID:
            if 'Game' in message.content:
                if 'begun' in message.content:
                    await game_begun(message)
                elif 'picked' in message.content:
                    await game_picked(message)
                elif 'cancelled' in message.content:
                    await game_cancelled(message)
                elif 'finished' in message.content:
                    await game_finished(message)
            elif 'has replaced' and 'as captain' in message.content:
                await replaced_captain(message)
            elif 'has been substituted with' in message.content:
                await sub_player(message)
            elif 'has been swapped with' in message.content:
                await swap_player(message)
        await bot.process_commands(message)

    @bot.event
    async def on_command_error(ctx, error):
        if isinstance(error, commands.errors.CommandNotFound):
            logger.debug(f'({ctx.author.display_name}) {ctx.message.content}: {error}')
        elif isinstance(error, commands.errors.CommandInvokeError):
            logger.error(f'({ctx.author.display_name}) {ctx.message.content}: {error}')

    bot.run(TOKEN)


# Main
if __name__ == '__main__':
    # Setup logging
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-log",
        "--log",
        default="info",
        help=(
            "Provide logging level. "
            "Example --log debug', default='warning'"
        ),
    )
    options = parser.parse_args()
    levels = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warn': logging.WARNING,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
    }
    level = levels.get(options.log.lower())
    if level is None:
        raise ValueError(
            f"log level given: {options.log}"
            f" -- must be one of: {' | '.join(levels.keys())}")
    logging.basicConfig(format='%(asctime)s %(levelname)-8s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=level)
    logger = logging.getLogger(__name__)
    # Connect to database and initialise
    db_conn = sqlite3.connect(DATABASE)
    init_db(db_conn)
    # Attempt to connect to Discord server
    retry_count = 0
    while retry_count < MAX_RETRY_COUNT:
        try:
            start_bot(db_conn)
            break
        except ClientConnectorError as ex:
            retry_count += 1
            logger.error(f'Attempt number {retry_count} to connect to the Discord server failed. Waiting to retry.')
            time.sleep(RETRY_WAIT)
    if retry_count == MAX_RETRY_COUNT:
        logger.error('Unable to connect to the Discord server. Aborting.')
    # Close database
    db_conn.close()
    logger.info('Database closed.')
