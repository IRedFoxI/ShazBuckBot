# -*- coding: utf-8 -*-
"""A discord bot to bet on PUGs."""
import argparse
import asyncio
import atexit
import os
import re
import time
import typing
from datetime import datetime
from itertools import combinations, chain
from math import sqrt, floor

import unicodedata
from enum import IntEnum, auto

import git
import requests
import yaml
import sqlite3
from typing import List, Tuple
import logging

import discord
from aiohttp import ClientConnectorError
from discord.ext import commands

from trueskill import Rating, rate, quality, backends, BETA, global_env

backends.choose_backend('scipy')

config = yaml.safe_load(open("config.yml"))
TOKEN: str = config['token']
DATABASE: str = config['database']
DISCORD_ID: int = config['discord_id']
INIT_BAL: int = config['init_bal']
BUCKS_PER_PUG: int = config['bucks_per_pug']
BET_WINDOW: int = config['bet_window']
BULLYBOT_DISCORD_ID: int = config['bullybot_discord_id']
REDFOX_DISCORD_ID: int = config['redfox_discord_id']
PUG_CHANNEL_ID: int = config['pug_channel_id']
BOT_CHANNEL_ID: int = config['bot_channel_id']


class GameStatus(IntEnum):
    PICKING = auto()
    CANCELLED = auto()
    INPROGRESS = auto()
    TEAM1 = auto()
    TEAM2 = auto()
    TIED = auto()


class WagerResult(IntEnum):
    INPROGRESS = auto()
    WON = auto()
    LOST = auto()
    CANCELLED = auto()
    CANCELLEDNOWINNERS = auto()
    CANCELLEDONESIDED = auto()


DM_TIME_TO_WAIT = 0.21  # Seconds
DURATION_TOLERANCE = 60  # Minutes
REACTIONS = ["👎", "👍"]
TIE_PAYOUT_SCALE = 0.5
MAX_RETRY_COUNT = 10
RETRY_WAIT = 10  # Seconds
TWITCH_GAME_ID = "517069"  # midair community edition
TWITCH_CLIENT_ID: str = config['twitch_client_id']
TWITCH_AUTH_ACCESS_TOKEN: str = config['twitch_auth_access_token']


class TimeDuration:
    SECONDS_PER_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    def __init__(self, value: int, unit: str):
        self.value = value
        self.unit = unit

    def __str__(self):
        return f'{str(self.value)}{self.unit}'

    @classmethod
    async def convert(cls, _ctx, argument):
        if type(argument) == str and len(argument) > 1:
            unit = argument[-1]
            if unit in TimeDuration.SECONDS_PER_UNIT and argument[:-1].isdigit():
                value = int(argument[:-1])
                return cls(value, unit)
            else:
                raise commands.BadArgument('Incorrect string format for TimeDuration.')
        else:
            raise commands.BadArgument('No string or too short for TimeDuration.')

    @property
    def to_seconds(self):
        return self.value * TimeDuration.SECONDS_PER_UNIT[self.unit]


DEFAULT_MOTD_TIME = TimeDuration(1, 'd')


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
    :param tuple[str,str,str,int] game: Tuple with the details of the game
    :return: The id of the created game
    """
    game += (GameStatus.PICKING,)
    sql = ''' INSERT INTO games(queue, start_time, team1, team2, bet_window, status)
              VALUES(?, strftime('%s','now'), ?, ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, game)
    conn.commit()
    return cur.lastrowid


def cancel_game(conn, game_id) -> None:
    """Update a game in the games table to Cancelled status

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to update to InProgress status
    """
    values = (GameStatus.CANCELLED, game_id)
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
    values = teams + (GameStatus.INPROGRESS, game_id)
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
    if result not in set(r.value for r in GameStatus):
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
    wager += (WagerResult.INPROGRESS,)
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
    if result not in set(r.value for r in WagerResult):
        raise ValueError()
    values = (result, wager_id)
    sql = ''' UPDATE wagers SET result = ? WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def create_motd(conn, motd) -> int:
    """Create a new motd into the motds table

    :param sqlite3.Connection conn: Connection to the database
    :param tuple[int,int,str,int] motd: Tuple with the details of the wager
    :return: The id of the created motd or 0 if an error occurred
    """
    if len(motd) != 4:
        raise ValueError
    sql = ''' INSERT INTO motds(discord_id, channel_id, start_time, message, end_time)
              VALUES(?, ?, strftime('%s','now'), ?, strftime('%s','now') + ?) '''
    cur = conn.cursor()
    cur.execute(sql, motd)
    conn.commit()
    return cur.lastrowid


def end_motd(conn, motd_id) -> None:
    """End a motd

    :param sqlite3.Connection conn: Connection to the database
    :param int motd_id: The id of the motd to be ended
    """
    sql = ''' UPDATE motds SET end_time = strftime('%s','now') WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, (motd_id,))
    conn.commit()


def suggest_even_teams(conn, player_ids) -> (List[int], List[int], float):
    """Suggest even teams based on TrueSkill ratings

    :param sqlite3.Connection conn: Connection to the database
    :param list[int] player_ids: List of discord ids
    :return: Two lists of discord ids and the chance to draw
    """
    player_ratings = {}
    for player_id in player_ids:
        sql = ''' SELECT mu, sigma FROM trueskills WHERE discord_id = ? AND game_id IN ( SELECT MAX(game_id) 
                  FROM trueskills WHERE discord_id = ? ) '''
        values = (player_id, player_id)
        cursor = conn.cursor()
        cursor.execute(sql, values)
        data = cursor.fetchone()
        if data:
            player_ratings[player_id] = Rating(data[0], data[1])
        else:
            player_ratings[player_id] = Rating()
    best_team1_ids = []
    best_team2_ids = []
    best_chance_to_draw = 0
    for c in combinations(player_ids, floor(len(player_ids) / 2)):
        team1_ids = list(c)
        team2_ids = [x for x in player_ids if x not in team1_ids]
        team1_rating = [player_ratings[i] for i in team1_ids]
        team2_rating = [player_ratings[i] for i in team2_ids]
        chance_to_draw = quality([team1_rating, team2_rating])
        if chance_to_draw > best_chance_to_draw:
            best_team1_ids = team1_ids
            best_team2_ids = team2_ids
            best_chance_to_draw = chance_to_draw
    return best_team1_ids, best_team2_ids, best_chance_to_draw


def calculate_win_chance(conn, teams_ids) -> float:
    """Calculate the chance for the first team to win

    :param sqlite3.Connection conn: Connection to the database
    :param tuple[list[int], list[int]] teams_ids: Tuple of Lists of discord ids of players on each team
    :return: Chance for the first team to win
    """
    team_ratings = []
    for team_ids in teams_ids:
        team_rating = []
        for player_id in team_ids:
            sql = ''' SELECT mu, sigma FROM trueskills WHERE discord_id = ? AND game_id IN ( SELECT MAX(game_id) 
                      FROM trueskills WHERE discord_id = ? ) '''
            values = (player_id, player_id)
            cursor = conn.cursor()
            cursor.execute(sql, values)
            data = cursor.fetchone()
            if data:
                team_rating.append(Rating(data[0], data[1]))
            else:
                team_rating.append(Rating())
        team_ratings.append(team_rating)
    delta_mu = sum(r.mu for r in team_ratings[0]) - sum(r.mu for r in team_ratings[1])
    sum_sigma = sum(r.sigma ** 2 for r in chain(team_ratings[0], team_ratings[1]))
    size = len(team_ratings[0]) + len(team_ratings[1])
    return global_env().cdf(delta_mu / sqrt(size * (BETA * BETA) + sum_sigma))


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
            status INTEGER NOT NULL,
            bet_window INTEGER NOT NULL
        );
    """)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS CNTREC FROM pragma_table_info('games') WHERE name='bet_window'")
    data = cur.fetchone()
    if data[0] == 0:
        conn.execute(f"""
            ALTER TABLE games ADD COLUMN bet_window INTEGER NOT NULL DEFAULT {BET_WINDOW}
        """)
    conn.execute("""
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS motds (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            discord_id INT NOT NULL,
            channel_id INT NOT NULL,
            start_time INT NOT NULL,
            end_time INT,
            message TEXT NOT NULL
        );
    """)


def start_bot(conn):
    bot = commands.Bot(command_prefix='!', loop=asyncio.new_event_loop(),
                       help_command=commands.DefaultHelpCommand(dm_help=True))
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

    async def get_nick_from_discord_id(discord_id: str) -> str:
        """Convert a discord id to a nick using discord or a database lookup

        :param str discord_id: The discord id to be looked up
        :return: The nick associated with the discord id
        """
        capt_nick = 'Unknown'
        if discord_id.isdigit():
            member = await fetch_member(int(discord_id))
            if member:
                capt_nick = member.display_name
            else:
                cursor = conn.cursor()
                cursor.execute(''' SELECT id, nick FROM users WHERE discord_id = ? ''', (discord_id,))
                data = cursor.fetchone()
                if data:
                    capt_nick = data[1]
                else:
                    logger.warning(f'Unable to fetch nick from discord id ({discord_id}): no valid response from '
                                   f'discord and not found in the database.')
        else:
            logger.warning(f'Unable to fetch nick from discord id: {discord_id} is not a number')
            capt_nick = discord_id
        return capt_nick

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

        :param int game_id: The id of game which bets should be cancelled
        :param str reason: The reason of the cancellation to send to the users in a DM
        """
        sql = ''' SELECT wagers.id, user_id, amount, nick, team1, team2 FROM wagers, users, games 
                  WHERE game_id = ? AND users.id = user_id AND games.id = ? AND result = ?'''
        cursor = conn.cursor()
        cursor.execute(sql, (game_id, game_id, WagerResult.INPROGRESS))
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
            wager_result(conn, wager_id, WagerResult.CANCELLED)
            msg = (f'Hi {nick}. Your bet on the game captained by {" and ".join(captains)} was cancelled '
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
                                  'or 1, 2, 3, Red, Blue or Tie. Optionally you can specify the ID of the game.')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_bet(ctx, winner: str, amount: int, *, game_id=0):
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
                if game_id == 0:
                    sql = ''' SELECT id, team1, team2, queue,
                              CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER),
                              bet_window
                              FROM games WHERE status = ? '''
                    game_data = (GameStatus.INPROGRESS,)
                else:
                    sql = ''' SELECT id, team1, team2, queue,
                              CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER),
                              bet_window
                              FROM games WHERE id = ? AND status = ? '''
                    game_data = (game_id, GameStatus.INPROGRESS)
                cursor = conn.cursor()
                cursor.execute(sql, game_data)
                games = cursor.fetchall()
                if not games:
                    if game_id == 0:
                        msg = f'Hi {nick}. No games are running. Please wait until teams are picked.'
                    else:
                        msg = (f'Hi {nick}. There is currently no game with ID {game_id} running. Please double-check '
                               f'the ID or wait until teams are picked.')
                    await send_dm(user_id, msg)
                else:
                    game_id: int = games[-1][0]
                    queue: str = games[-1][3]
                    bet_window: int = games[-1][5]
                    prediction = 0
                    time_since_pick = 0
                    if winner == "1" or caseless_equal(winner, "Red"):
                        prediction += GameStatus.TEAM1
                        team_id_str: str = games[-1][1]
                        if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                            capt_id_str = team_id_str.split()[0]
                            winner = await get_nick_from_discord_id(capt_id_str)
                        else:
                            winner = team_id_str
                        time_since_pick = games[-1][4]
                    elif winner == "2" or caseless_equal(winner, "Blue"):
                        prediction += GameStatus.TEAM2
                        team_id_str: str = games[-1][2]
                        if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                            capt_id_str = team_id_str.split()[0]
                            winner = await get_nick_from_discord_id(capt_id_str)
                        else:
                            winner = team_id_str
                        time_since_pick = games[-1][4]
                    elif winner == "3" or caseless_equal(winner, "Tie"):
                        prediction += GameStatus.TIED
                        winner = 'a tie'
                        time_since_pick = games[-1][4]
                    else:
                        for game in games:
                            team_id_strs: Tuple[str, str] = game[1:3]
                            queue: str = game[3]
                            if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                                capt_ids_strs = [team_id_str.split()[0] for team_id_str in team_id_strs]
                                capt_nicks = [(await get_nick_from_discord_id(did)) for did in capt_ids_strs]
                            else:
                                capt_nicks = team_id_strs
                            if caseless_equal(winner, capt_nicks[0]):
                                game_id: int = game[0]
                                prediction += GameStatus.TEAM1
                                time_since_pick = game[4]
                                winner = capt_nicks[0]
                            elif caseless_equal(winner, capt_nicks[1]):
                                game_id: int = game[0]
                                prediction += GameStatus.TEAM2
                                time_since_pick = game[4]
                                winner = capt_nicks[1]
                    if prediction == 0:
                        if game_id == 0:
                            msg = (f'Hi {nick}, could not find a game to bet on {winner}. Please check the spelling, '
                                   f'use 1, 2, Red or Blue, or wait until the teams have been picked.')
                        else:
                            msg = (f'Hi {nick}, could not find a game to bet on {winner}. Please check the spelling, '
                                   f'use 1, 2, Red or Blue, check the ID or wait until the teams have been picked.')
                        await send_dm(user_id, msg)
                    elif time_since_pick > bet_window:
                        msg = (f'Hi {nick}, too late! The game has started {time_since_pick} minutes ago. '
                               f'Bets have to be made within {bet_window} minutes after picking is complete')
                        await send_dm(user_id, msg)
                    else:
                        sql = ''' SELECT id, prediction FROM wagers 
                                  WHERE user_id = ? AND game_id = ? AND result = ? '''
                        values = (user_id, game_id, WagerResult.INPROGRESS)
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
            show_str = ''
            # Add MOTDs
            sql = ''' SELECT message FROM motds 
                      WHERE (channel_id = 0 OR channel_id = ?) AND end_time > strftime('%s','now') '''
            cursor = conn.cursor()
            cursor.execute(sql, (ctx.channel.id,))
            motds = cursor.fetchall()
            if motds:
                for motd in motds:
                    show_str += f'MOTD: {motd[0]}\n'
            # Find running games
            sql = ''' SELECT id, team1, team2, queue, status, 
                      CAST (((julianday('now') - julianday(pick_time, 'unixepoch')) * 24 * 60) AS INTEGER),
                      bet_window
                      FROM games WHERE status = ? OR status = ?'''
            cursor = conn.cursor()
            cursor.execute(sql, (GameStatus.PICKING, GameStatus.INPROGRESS))
            games = cursor.fetchall()
            if not games:
                show_str += f'No games are running'
            else:
                for game in games:
                    game_id: int = game[0]
                    teams: Tuple[str, str] = game[1:3]
                    queue: str = game[3]
                    game_status: GameStatus = game[4]
                    run_time: int = game[5]
                    bet_window: int = game[6]
                    capt_ids_strs = [team.split()[0] for team in teams]
                    if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                        capt_nicks = [await get_nick_from_discord_id(did) for did in capt_ids_strs]
                    else:
                        capt_nicks = capt_ids_strs
                    cursor = conn.cursor()
                    sql = ''' SELECT prediction, amount FROM wagers WHERE game_id = ? AND result = ? '''
                    cursor.execute(sql, (game_id, WagerResult.INPROGRESS))
                    wagers = cursor.fetchall()
                    total_amounts = {GameStatus.TEAM1: 0, GameStatus.TEAM2: 0, GameStatus.TIED: 0}
                    for wager in wagers:
                        prediction = GameStatus(wager[0])
                        amount: int = wager[1]
                        total_amounts[prediction] += amount
                    if game_status == GameStatus.PICKING:
                        show_str += (f'{queue}: Game {game_id} (Picking): '
                                     f'{capt_nicks[0]} vs '
                                     f'{capt_nicks[1]}\n')
                    elif game_status == GameStatus.INPROGRESS:
                        if run_time <= bet_window:
                            show_str += (f'{queue}: Game {game_id} ({bet_window - run_time} minutes left to bet): '
                                         f'{capt_nicks[0]}({total_amounts[GameStatus.TEAM1]}), '
                                         f'{capt_nicks[1]}({total_amounts[GameStatus.TEAM2]}) or '
                                         f'tied ({total_amounts[GameStatus.TIED]})\n')
                        else:
                            show_str += (f'{queue}: Game {game_id} (Betting closed): '
                                         f'{capt_nicks[0]}({total_amounts[GameStatus.TEAM1]}), '
                                         f'{capt_nicks[1]}({total_amounts[GameStatus.TEAM2]}) or '
                                         f'tied ({total_amounts[GameStatus.TIED]})\n')
            success = True
            await ctx.send(show_str)
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

    @bot.command(name='tsgraph', help='Show a graph of your trueskill over time')
    @in_channel(BOT_CHANNEL_ID)
    @is_admin()
    async def cmd_tsgraph(ctx, members: commands.Greedy[discord.Member]):
        # success = False
        discord_ids = []
        if len(members) == 0:
            discord_ids.append(str(ctx.author.id))
        else:
            for member in members:
                discord_ids.append(str(member.id))
        ids_str = '&discord_id='.join(discord_ids)
        graph_url = f'https://club77.org/shazbuckbot/trueskillgraph.py?discord_id={ids_str}'
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
    async def cmd_changelog(ctx):
        logger.info(f'{ctx.author.display_name} requested changelog.')
        success = False
        repo = git.Repo('./')
        try:
            log = repo.heads.master.log()
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(f'Hi {ctx.author.name}, these are the latest 5 commits:')
            for entry in log[-5:]:
                commit = repo.commit(entry.newhexsha)
                entry_string = (f'{commit.authored_datetime} {entry.newhexsha} {commit.author.name}:\n'
                                f'{commit.message.rstrip()}')
                await ctx.author.dm_channel.send(entry_string)
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
            change_nick: str = data[1]
            sql = ''' SELECT team1, team2, queue, status FROM games 
                      WHERE id = ? AND (status = ? OR status = ? OR status = ? OR status = ? OR status = ?) '''
            values = (game_id, GameStatus.INPROGRESS, GameStatus.CANCELLED, GameStatus.TEAM1,
                      GameStatus.TEAM2, GameStatus.TIED)
            cursor = conn.cursor()
            cursor.execute(sql, values)
            game = cursor.fetchone()
            if not game:
                msg = (f'Hi {change_nick}. The game with id {game_id} does not exist or it\'s status is not '
                       f'InProgress, Team1, Team2, Tied or Cancelled.')
                await send_dm(user_id, msg)
            else:
                team_id_strs: Tuple[str, str] = game[0:2]
                capt_ids_strs = [team_id_str.split()[0] for team_id_str in team_id_strs]
                queue: str = game[2]
                if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                    capt_nicks = [await get_nick_from_discord_id(did) for did in capt_ids_strs]
                else:
                    capt_nicks = capt_ids_strs
                old_status = game[3]
                new_status = None
                if result in ['1', 'Red', 'red', 'Team1', 'team1', capt_nicks[0]]:
                    new_status = GameStatus.TEAM1
                elif result in ['2', 'Blue', 'blue', 'Team2', 'team2', capt_nicks[1]]:
                    new_status = GameStatus.TEAM2
                elif result in ['3', 'Tie', 'tie', 'Tied', 'tied']:
                    new_status = GameStatus.TIED
                elif result in ['4', 'Cancel', 'cancel', 'Canceled', 'canceled', 'Cancelled', 'cancelled']:
                    new_status = GameStatus.CANCELLED
                else:
                    msg = (f'Hi {change_nick}. Result not understood. You can use 1, 2, Red or Blue or the captain\'s '
                           f'name to select the winning team. Or use 3/Tie/Tied to tie or 4/Cancel/Canceled/Cancelled '
                           f'to cancel the game.')
                    await send_dm(user_id, msg)
                if new_status:
                    if new_status == old_status:
                        msg = (
                            f'Hi {change_nick}. The game with id {game_id} was already set to {new_status.name}.')
                        await send_dm(user_id, msg)
                    elif old_status == GameStatus.CANCELLED:
                        msg = (
                            f'Hi {change_nick}. The game with id {game_id} was previously cancelled. Reviving '
                            f'cancelled games has not been implemented.')
                        await send_dm(user_id, msg)
                    else:
                        # Initialize parameters
                        total_amounts = {GameStatus.TEAM1.name: 0, GameStatus.TEAM2.name: 0, GameStatus.TIED.name: 0}
                        winners = []
                        sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id 
                                  FROM users, wagers 
                                  WHERE game_id = ? AND users.id = wagers.user_id AND result <> ? AND result <> ? '''
                        cursor = conn.cursor()
                        cursor.execute(sql, (game_id, WagerResult.CANCELLED, WagerResult.CANCELLEDONESIDED))
                        wagers = cursor.fetchall()
                        # Calculate the total amounts bet on each outcome
                        for wager in wagers:
                            prediction: str = GameStatus(wager[2]).name
                            amount: int = wager[3]
                            total_amounts[prediction] += amount
                        total_amount = sum(total_amounts.values())
                        if old_status != GameStatus.INPROGRESS:
                            # Calculate the payout ratio (0 if no bets on winning outcome)
                            ratio = 0
                            if old_status == GameStatus.TEAM1 and total_amounts[GameStatus.TEAM1.name] > 0:
                                ratio = total_amount / total_amounts[GameStatus.TEAM1.name]
                            elif old_status == GameStatus.TEAM2 and total_amounts[GameStatus.TEAM2.name] > 0:
                                ratio = total_amount / total_amounts[GameStatus.TEAM2.name]
                            elif old_status == GameStatus.TIED and total_amounts[GameStatus.TIED.name] > 0:
                                ratio = total_amount / total_amounts[GameStatus.TIED.name]
                            # Set the status of the game back to INPROGRESS
                            finish_game(conn, game_id, GameStatus.INPROGRESS)
                            # Claw back previous payout
                            for wager in wagers:
                                wager_id: int = wager[0]
                                user_id: int = wager[1]
                                prediction: int = wager[2]
                                amount: int = wager[3]
                                nick: str = wager[4]
                                discord_id: int = wager[5]
                                if ratio == 0:
                                    transfer = (user_id, bot_user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WagerResult.INPROGRESS)
                                    msg = (f'Hi {nick}. The result of game {game_id}, between '
                                           f'{" and ".join(capt_nicks)}, was changed. Your previously returned bet of '
                                           f'{amount} shazbucks has been placed again.')
                                    await send_dm(user_id, msg)
                                elif prediction == old_status:
                                    win_amount = round(amount * ratio)
                                    if prediction == GameStatus.TIED:
                                        win_amount = win_amount * TIE_PAYOUT_SCALE
                                    transfer = (user_id, bot_user_id, win_amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id, WagerResult.INPROGRESS)
                                    msg = (f'Hi {nick}. The result of game {game_id}, between '
                                           f'{" and ".join(capt_nicks)}, was changed. Your previous payout of '
                                           f'{win_amount} shazbucks has been clawed back.')
                                    await send_dm(user_id, msg)
                                    winner = await get_nick_from_discord_id(str(discord_id))
                                    winners.append((winner, win_amount))
                                else:
                                    wager_result(conn, wager_id, WagerResult.INPROGRESS)
                                    msg = (f'Hi {nick}. The result of game {game_id}, between '
                                           f'{" and ".join(capt_nicks)}, was changed. Your previously lost bet of '
                                           f'{amount} shazbucks has been placed again.')
                                    await send_dm(user_id, msg)
                            result_msg = ''
                            if (old_status == GameStatus.TEAM1 or
                                    old_status == GameStatus.TEAM2 or
                                    old_status == GameStatus.TIED):
                                if ratio == 0:
                                    if total_amount > 0:
                                        result_msg = (f'The result of game {game_id}, between '
                                                      f'{" and ".join(capt_nicks)}, was changed. All wagers have been '
                                                      f'placed again.')
                                else:
                                    verb = "was" if len(winners) == 1 else "were"
                                    winners_str = ', '.join([f'{winner}({win_amount})' for
                                                             (winner, win_amount) in winners])
                                    payout = sum([win_amount for (winner, win_amount) in winners])
                                    result_msg = (f'The result of game {game_id}, between '
                                                  f'{" and ".join(capt_nicks)}, was changed. The previous winnings of '
                                                  f'{winners_str} for a total of {payout} shazbucks {verb} clawed '
                                                  f'back.')
                            if result_msg:
                                await ctx.send(result_msg)
                        # Set the status of the game to the new result
                        finish_game(conn, game_id, new_status)
                        # Payout based on new result
                        total_amounts, winners = await resolve_wagers(game_id, new_status, capt_nicks, True)
                        total_amount = sum(total_amounts.values())
                        result_msg = ''
                        if (new_status == GameStatus.TEAM1 or
                                new_status == GameStatus.TEAM2 or
                                new_status == GameStatus.TIED):
                            if total_amount == 0:
                                logger.info(f'Game {game_id} changed by {change_nick} to result: {new_status.name}, '
                                            f'but the game had no bets or all bets were on a single outcome.')
                            elif total_amounts[new_status.name] == 0:
                                result_msg = (f'The result of game {game_id}, between {" and ".join(capt_nicks)}, '
                                              f'was changed. There were no bets on the correct outcome. '
                                              f'All wagers have been returned.')
                                logger.info(f'Game {game_id} was changed by {change_nick} to: {new_status.name}, '
                                            f'but the game had no bets on that outcome. All wagers have been returned.')
                            elif total_amounts[new_status.name] == total_amount:
                                result_msg = (f'The result of game {game_id}, between {" and ".join(capt_nicks)}, '
                                              f'was changed. There were only bets on the correct outcome. '
                                              f'All wagers have been returned.')
                                logger.info(f'Game {game_id} was changed by {change_nick} to: {new_status.name}, but '
                                            f'the game only had bets on that outcome. All wagers have been returned.')
                            else:
                                verb = "was" if len(winners) == 1 else "were"
                                winners_str = ', '.join([f'{winner}({win_amount})' for
                                                         (winner, win_amount) in winners])
                                payout = sum([win_amount for (winner, win_amount) in winners])
                                result_msg = (f'The result of game {game_id}, between {" and ".join(capt_nicks)}, '
                                              f'was changed. {winners_str} {verb} paid out a total of {payout} '
                                              f'shazbucks.')
                                logger.info(f'Game {game_id} was changed by {change_nick} to: {new_status.name}. '
                                            f'{winners_str} {verb} paid out a total of {payout} shazbucks.')
                        elif new_status == GameStatus.CANCELLED:
                            result_msg = (f'Game {game_id}, between {" and ".join(capt_nicks)}, was cancelled. '
                                          f'All wagers have been returned.')
                            logger.info(f'Game {game_id} was changed by {change_nick} to: {new_status.name}, '
                                        f'All wagers have been returned.')
                        if result_msg:
                            await ctx.send(result_msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='start_game', help='Create a new custom game')
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_start_game(ctx, outcome1: str, outcome2: str, bet_window: typing.Optional[int] = BET_WINDOW,
                             description: typing.Optional[str] = ''):
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
            if description == '':
                description = ctx.author.display_name
            if description in ('NA', 'EU', 'AU', 'TestBranch'):
                msg = (f'Hi {nick}. The description of a custom game cannot be set to NA, EU, AU or TestBranch. '
                       f'Please use a different description.')
                await send_dm(user_id, msg)
            else:
                teams = (outcome1, outcome2)
                game = (description,) + teams + (bet_window,)
                game_id = create_game(conn, game)
                pick_game(conn, game_id, teams)
                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='end_game', help='End a custom game')
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_end_game(ctx, game_id: int, result: str):
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
            sql = ''' SELECT queue, team1, team2 FROM games 
                      WHERE id = ? AND status = ? '''
            values = (game_id, GameStatus.INPROGRESS)
            cursor = conn.cursor()
            cursor.execute(sql, values)
            game = cursor.fetchone()
            if not game:
                msg = f'Hi {nick}. The game with id {game_id} does not exist or it\'s status is not InProgress.'
                await send_dm(user_id, msg)
            else:
                queue: str = game[0]
                outcome1: str = game[1]
                outcome2: str = game[2]
                outcomes = [outcome1, outcome2]
                if queue in ('NA', 'EU', 'AU', 'TestBranch'):
                    msg = (f'Hi {nick}. The game with id {game_id} is not a custom bet, you cannot end the bet this '
                           f'way. Please use the !change_bet command.')
                    await send_dm(user_id, msg)
                else:
                    status = None
                    if result in ['1', 'Red', 'red', 'Team1', 'team1', outcome1]:
                        status = GameStatus.TEAM1
                    elif result in ['2', 'Blue', 'blue', 'Team2', 'team2', outcome2]:
                        status = GameStatus.TEAM2
                    elif result in ['3', 'Tie', 'tie', 'Tied', 'tied']:
                        status = GameStatus.TIED
                    elif result in ['4', 'Cancel', 'cancel', 'Canceled', 'canceled', 'Cancelled', 'cancelled']:
                        status = GameStatus.CANCELLED
                    else:
                        msg = (f'Hi {nick}. Result not understood. You can use 1, 2, Red or Blue or the captain\'s name'
                               f' to select the winning outcome. Or use 3/Tie/Tied to tie or '
                               f'4/Cancel/Canceled/Cancelled to cancel the game.')
                        await send_dm(user_id, msg)
                    if status:
                        # Set the status of the game to the new result
                        finish_game(conn, game_id, status)
                        # Payout based on new result
                        total_amounts, winners = await resolve_wagers(game_id, status, outcomes)
                        result_msg = ''
                        if status == GameStatus.TEAM1 or status == GameStatus.TEAM2 or status == GameStatus.TIED:
                            if sum(total_amounts.values()) == 0:
                                logger.info(f'Custom Game {game_id} ended by {nick} with result: {status.name}, '
                                            f'but the game had no bets. All wagers have been returned.')
                            elif total_amounts[status.name] == 0:
                                result_msg = (f'The game {game_id}, with possible outcomes {" and ".join(outcomes)} '
                                              f' or a tie finished. The game had no bets on the winning outcome. All '
                                              f'wagers have been returned.')
                                logger.info(f'Custom Game {game_id} ended by {nick} with result: {status.name}, '
                                            f'but the game had no bets on that outcome. All wagers have been '
                                            f'returned.')
                            elif total_amounts[status.name] == sum(total_amounts.values()):
                                result_msg = (f'The game {game_id}, with possible outcomes {" and ".join(outcomes)} '
                                              f' or a tie finished. The game only had bets on the winning outcome. '
                                              f'All wagers have been returned.')
                                logger.info(f'Custom Game {game_id} ended by {nick} with result: {status.name}, '
                                            f'but the game only had bets on that outcome. All wagers have been '
                                            f'returned.')
                            else:
                                verb = "was" if len(winners) == 1 else "were"
                                winners_str = ', '.join([f'{winner}({win_amount})' for
                                                         (winner, win_amount) in winners])
                                payout = sum([win_amount for (winner, win_amount) in winners])
                                result_msg = (f'The game {game_id}, with possible outcomes {" and ".join(outcomes)}, '
                                              f'finished. {winners_str} {verb} paid out a total of {payout} '
                                              f'shazbucks.')
                                logger.info(f'Custom Game {game_id} was ended by {nick} to a win for {status.name}.'
                                            f' {winners_str} {verb} paid out a total of {payout} shazbucks.')
                        elif status == GameStatus.CANCELLED:
                            result_msg = (f'The game {game_id}, with possible outcomes {" and ".join(outcomes)} '
                                          f' or a tie was cancelled. All wagers have been returned.')
                            logger.info(f'Custom Game {game_id} ended by {nick} with result: {status.name}, '
                                        f'all wagers have been returned.')
                        if result_msg:
                            await ctx.send(result_msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='streams', help='Show midair streams')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_streams(ctx):
        success = False
        headers = {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {TWITCH_AUTH_ACCESS_TOKEN}',
            'Client-Id': f'{TWITCH_CLIENT_ID}',
        }
        response = requests.get('https://api.twitch.tv/helix/streams?first=5&game_id=' + TWITCH_GAME_ID,
                                headers=headers)
        time.sleep(2)
        streams = response.json()
        if streams['data']:
            embed: discord.Embed = discord.Embed(title="", description="", color=discord.Color(8192255))
            user_names = []
            total_viewer_count = 0
            for stream in streams['data']:
                stream_name = (f"{str(stream['viewer_count'])}<:z_1:771957560005099530> {str(stream['user_name'])} - "
                               f"{str(stream['title'])}")
                stream_link = "https://www.twitch.tv/" + str(stream['user_name'])
                stream_url = "[twitch.tv/" + str(stream['user_name']) + "](" + stream_link + ")"
                embed.add_field(name=stream_name, value=stream_url, inline=False)
                total_viewer_count += int(stream['viewer_count'])
                user_names.append(str(stream['user_name']))
            if len(user_names) > 1:
                multi_stream_name = f'{str(total_viewer_count)}<:z_1:771957560005099530> Multi stream'
                multi_stream_link = f'https://multistre.am/{"/".join(user_names)}'
                multi_stream_url = "[multistre.am/" + "/".join(user_names) + "](" + multi_stream_link + ")"
                embed.add_field(name=multi_stream_name, value=multi_stream_url, inline=False)
            await ctx.channel.send(embed=embed)
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.group(name='motd', help='Message of the Day commands', pass_context=True, invoke_without_command=True)
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_motd(ctx):
        if ctx.invoked_subcommand is None:
            await ctx.message.add_reaction(REACTIONS[False])

    @cmd_motd.command(name='create', help='Create a new Message of the Day')
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_motd_create(ctx, all_channels: typing.Optional[bool] = False,
                              duration: typing.Optional[TimeDuration] = DEFAULT_MOTD_TIME, *, motd_message: str):
        success = False
        author_id = ctx.message.author.id
        channel_id = ctx.channel.id
        if all_channels != 0 and author_id == REDFOX_DISCORD_ID:
            channel_id = 0
        motd = (author_id, channel_id, motd_message, duration.to_seconds)
        if create_motd(conn, motd):
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @cmd_motd.command(name='list', help='Show current Messages of the Day')
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_motd_list(ctx):
        success = False
        requestor = ctx.message.author
        if requestor.id == REDFOX_DISCORD_ID:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE (channel_id = ? or channel_id = 0) AND end_time > strftime('%s','now') '''
        else:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE channel_id = ? AND end_time > strftime('%s','now') '''
        cursor = conn.cursor()
        cursor.execute(sql, (ctx.channel.id,))
        motds = cursor.fetchall()
        if motds:
            for motd in motds:
                motd_id: int = motd[0]
                author_id: int = motd[1]
                channel_id: int = motd[2]
                start_time: int = motd[3]
                end_time: int = motd[4]
                motd_message: str = motd[5]
                author_nick = await get_nick_from_discord_id(str(author_id))
                motd_info = (f'MOTD {motd_id} set by {author_nick} {"on all channels " if channel_id == 0 else ""}'
                             f'on {datetime.utcfromtimestamp(start_time)} and '
                             f'to expire on {datetime.utcfromtimestamp(end_time)}:\n{motd_message}')
                await asyncio.sleep(DM_TIME_TO_WAIT)
                await requestor.create_dm()
                await requestor.dm_channel.send(motd_info)
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @cmd_motd.command(name='end', help='Show current Messages of the Day')
    @is_admin()
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_motd_list(ctx, motd_id: int):
        success = False
        requestor = ctx.message.author
        if requestor.id == REDFOX_DISCORD_ID:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE id = ? AND (channel_id = ? or channel_id = 0) AND end_time > strftime('%s','now') '''
        else:
            sql = ''' SELECT id, discord_id, channel_id, start_time, end_time, message FROM motds 
                      WHERE id = ? AND channel_id = ? AND end_time > strftime('%s','now') '''
        cursor = conn.cursor()
        cursor.execute(sql, (motd_id, ctx.channel.id))
        motd = cursor.fetchone()
        if motd:
            end_motd(conn, motd_id)
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
        player_ids = [int(i) for i in player_id_strs]
        player_nicks = []
        for capt_id in player_id_strs:
            member = await fetch_member(int(capt_id))
            player_nicks.append(member.display_name)
        for nick in descr_lines[1].split(', '):
            player_nicks.append(nick)
            player = await query_members(nick)
            if player:
                player_id_strs[0] += f' {player.id}'
                player_ids.append(player.id)
            else:
                logger.error(f'Could not find discord id for player {nick}')
        team_id_strs = tuple(player_id_strs)
        game = (queue,) + team_id_strs + (BET_WINDOW,)
        game_id = create_game(conn, game)
        logger.info(f'Game {game_id} created in the {queue} queue: {" ".join(player_nicks)}')
        best_team1_ids, best_team2_ids, best_chance_to_draw = suggest_even_teams(conn, player_ids)
        team1_str = '<@!' + '>, <@!'.join([str(i) for i in best_team1_ids]) + '>'
        team2_str = '<@!' + '>, <@!'.join([str(i) for i in best_team2_ids]) + '>'
        result_msg = f'Suggested teams: {team1_str} versus {team2_str} ({best_chance_to_draw:.1%} chance to draw).'
        await message.channel.send(result_msg)
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
                  ORDER BY start_time DESC '''
        cursor = conn.cursor()
        cursor.execute(sql, (queue, GameStatus.PICKING, GameStatus.INPROGRESS))
        games = cursor.fetchall()
        if not games:
            logger.error(f'Game picked in {queue} queue, but no game with Picking or InProgress status in that queue!')
            game = (queue,) + team_id_strs + (BET_WINDOW,)
            game_id = create_game(conn, game)
            game_status = GameStatus.PICKING
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
                game = (queue,) + team_id_strs + (BET_WINDOW,)
                game_id = create_game(conn, game)
                game_status = GameStatus.PICKING
                logger.info(f'Game {game_id} created in the {queue} queue: {" versus ".join(team_strs)}')
        # Cancel wagers if there is a repick
        if game_status == GameStatus.INPROGRESS:
            await cancel_wagers(game_id, 'a repick')
        # Update database and log
        pick_game(conn, game_id, team_id_strs)
        logger.info(f'Game {game_id} picked in the {queue} queue: {" versus ".join(team_strs)}')
        # Estimate chances
        team1_ids = [int(i) for i in team_id_strs[0].split()]
        team2_ids = [int(i) for i in team_id_strs[1].split()]
        team1_win_chance = calculate_win_chance(conn, (team1_ids, team2_ids))
        result_msg = (f'Teams picked, predictions: Team 1 ({team1_win_chance:.1%}), Team 2 '
                      f'({(1 - team1_win_chance):.1%}).')
        await message.channel.send(result_msg)
        await message.add_reaction(REACTIONS[True])

    async def game_cancelled(message: discord.Message):
        success = False
        # Find the game that was just cancelled
        cursor = conn.cursor()
        cursor.execute(''' SELECT id FROM games WHERE status = ? ''', (GameStatus.PICKING,))
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
        game_result = GameStatus.TIED if 'Tie' in result else None
        winner_nick = ''
        winner_id = 0
        total_amounts = {}
        winners = []
        # Find the game that just finished
        game_id = 0
        if game_result == GameStatus.TIED:
            game_values = (queue, GameStatus.INPROGRESS, duration, DURATION_TOLERANCE)
            sql = ''' SELECT id, ABS(CAST (((julianday('now') - julianday(start_time, 'unixepoch')) 
                      * 24 * 60) AS INTEGER)), team1, team2 
                      FROM games 
                      WHERE queue = ? AND status = ? AND ABS(CAST (((julianday('now') 
                      - julianday(start_time, 'unixepoch')) * 24 * 60) AS INTEGER)) - ? <= ? '''
        else:
            winner_nick = " ".join(result.split(' ')[2:])
            winner_id = (await query_members(winner_nick)).id
            winner_id_str = str(winner_id)
            game_values = (queue, GameStatus.INPROGRESS, winner_id_str + '%', winner_id_str + '%')
            sql = ''' SELECT id, ABS(CAST (((julianday('now') - julianday(start_time, 'unixepoch')) 
                      * 24 * 60) AS INTEGER)),team1, team2 FROM games 
                      WHERE queue = ? AND status = ? AND (team1 LIKE ? OR team2 LIKE ?) '''
        cursor = conn.cursor()
        cursor.execute(sql, game_values)
        games = cursor.fetchall()
        if not games:
            if game_result == GameStatus.TIED:
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
                # Log info for diagnostics purposes
                # TODO: remove this block
                logger.info('Game finished but multiple games match:')
                for i, game in enumerate(games):
                    capt_id_strs = [game[2].split()[0], game[3].split()[0]]
                    capt_nicks = [(await get_nick_from_discord_id(did)) for did in capt_id_strs]
                    logger.info(f'Game {game[0]}: {" vs ".join(capt_nicks)}, duration offset: {duration_offsets[i]}')
                logger.info(f'Number {idx} selected: Game {games[idx][0]}.')
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
            capt_ids = [teams[0][0].id, teams[1][0].id]
            capt_nicks = [teams[0][0].display_name, teams[1][0].display_name]
            # Establish result if not tied
            if game_result != GameStatus.TIED:
                if winner_id == capt_ids[0]:
                    game_result = GameStatus.TEAM1
                elif winner_id == capt_ids[1]:
                    game_result = GameStatus.TEAM2
                else:
                    game_result = 0
                    logger.error(f'Winner {winner_nick} ({winner_id}) not found in game {game_id}: {capt_nicks[0]} '
                                 f'versus {capt_nicks[1]}')
            # Update the database, resolve wagers, pay the participants and update trueskills
            if game_result:
                finish_game(conn, game_id, game_result)
                total_amounts, winners = await resolve_wagers(game_id, game_result, capt_nicks)
                await pay_players(teams)
                team_ratings = ()
                for team in teams:
                    team_rating = []
                    for player in team:
                        sql = ''' SELECT mu, sigma FROM trueskills 
                                  WHERE discord_id = ? AND game_id IN ( 
                                    SELECT MAX(game_id) FROM trueskills WHERE discord_id = ? 
                                  ) '''
                        values = (player.id, player.id)
                        cur.execute(sql, values)
                        data = cur.fetchone()
                        if data:
                            team_rating.append(Rating(data[0], data[1]))
                        else:
                            team_rating.append(Rating())
                    team_ratings += (team_rating,)
                ranks = [0, 0]
                if game_result == GameStatus.TEAM1:
                    ranks = [0, 1]
                elif game_result == GameStatus.TEAM2:
                    ranks = [1, 0]
                new_team_ratings = rate([team_ratings[0], team_ratings[1]], ranks)
                for team_idx, team in enumerate(teams):
                    for player_idx, player in enumerate(team):
                        rating = new_team_ratings[team_idx][player_idx]
                        trueskill_update = (player.id, game_id, rating.mu, rating.sigma, rating.exposure)
                        sql = ''' INSERT INTO trueskills(discord_id, game_id, mu, sigma, trueskill)
                                          VALUES(?, ?, ?, ?, ?) '''
                        cursor = conn.cursor()
                        cursor.execute(sql, trueskill_update)
                        conn.commit()
        # Send summary message to the channel, unless nobody placed a bet
        result_msg = ''
        if game_result is None:
            result_msg = '\'ERROR: Game not found\''
        elif game_result == 0:
            result_msg = '\'ERROR: Winner not found\''
        elif (game_result == GameStatus.TEAM1 or
              game_result == GameStatus.TEAM2 or
              game_result == GameStatus.TIED):
            if sum(total_amounts.values()) == 0:
                logger.info(f'Game {game_id} finished with result: {game_result.name}, but the game had no bets.')
            elif total_amounts[game_result.name] == 0:
                result_msg = 'The game had no bets on the correct outcome. All wagers have been returned.'
                logger.info(f'Game {game_id} finished with result: {game_result.name}, but the game had no bets '
                            f'on that outcome. All wagers have been returned.')
            elif total_amounts[game_result.name] == sum(total_amounts.values()):
                result_msg = 'The game only had bets on the correct outcome. All wagers have been returned.'
                logger.info(f'Game {game_id} finished with result: {game_result.name}, but the game only had bets '
                            f'on that outcome. All wagers have been returned.')
            else:
                verb = "was" if len(winners) == 1 else "were"
                winners_str = ', '.join([f'{winner}({win_amount})' for
                                         (winner, win_amount) in winners])
                payout = sum([win_amount for (winner, win_amount) in winners])
                result_msg = f'Game {game_id}: {winners_str} {verb} paid out a total of {payout} shazbucks.'
                logger.info(f'Game {game_id} finished with a win for {game_result.name}. {winners_str} {verb} paid '
                            f'out a total of {payout} shazbucks.')
        if result_msg:
            await message.channel.send(result_msg)

    async def resolve_wagers(game_id, game_result, capt_nicks, change=False) -> Tuple[dict,
                                                                                      List[Tuple[str, int]]]:
        """Resolve wagers placed on a game based on its outcome

        :param int game_id: ID of the game
        :param int game_result: Result of the game
        :param List[str] capt_nicks: List of captain nicks
        :param bool change: Boolean indicating whether the result of the game is being changed
        :return: a dictionary with the total amounts bet on each team and a dictionary with the amount won by each
            winner
        """
        # Initialize parameters
        total_amounts = {GameStatus.TEAM1.name: 0, GameStatus.TEAM2.name: 0, GameStatus.TIED.name: 0}
        winners = []
        # Find wagers on this game
        sql = ''' SELECT wagers.id, user_id, prediction, amount, nick, discord_id FROM users, wagers 
                  WHERE game_id = ? AND users.id = wagers.user_id AND result = ? '''
        cursor = conn.cursor()
        cursor.execute(sql, (game_id, WagerResult.INPROGRESS))
        wagers = cursor.fetchall()
        # Calculate the total amounts bet on each outcome
        for wager in wagers:
            prediction: str = GameStatus(wager[2]).name
            amount: int = wager[3]
            total_amounts[prediction] += amount
        # Calculate the payout ratio (0 if no bets on winning outcome, 1.0 if only bets on winning outcome)
        total_amount = sum(total_amounts.values())
        if game_result == GameStatus.CANCELLED:
            ratio = -1
        else:
            ratio = 0
            if game_result == GameStatus.TEAM1 and total_amounts[GameStatus.TEAM1.name] > 0:
                ratio = total_amount / total_amounts[GameStatus.TEAM1.name]
            elif game_result == GameStatus.TEAM2 and total_amounts[GameStatus.TEAM2.name] > 0:
                ratio = total_amount / total_amounts[GameStatus.TEAM2.name]
            elif game_result == GameStatus.TIED and total_amounts[GameStatus.TIED.name] > 0:
                ratio = total_amount / total_amounts[GameStatus.TIED.name]
        # Resolve each individual bet
        for wager in wagers:
            wager_id: int = wager[0]
            user_id: int = wager[1]
            prediction: int = wager[2]
            amount: int = wager[3]
            nick: str = wager[4]
            discord_id: int = wager[5]
            if ratio == -1:
                transfer = (bot_user_id, user_id, amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WagerResult.CANCELLED)
                if change:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was changed: the game was '
                           f'cancelled. Your bet of {amount} shazbucks has been returned to you.')
                else:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was cancelled. Your bet of '
                           f'{amount} shazbucks has been returned to you.')
                await send_dm(user_id, msg)
            elif ratio == 0:
                transfer = (bot_user_id, user_id, amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WagerResult.CANCELLEDNOWINNERS)
                if change:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was changed. Nobody predicted '
                           f'the correct outcome. Your bet of {amount} shazbucks has been returned to you.')
                else:
                    msg = (f'Hi {nick}. Nobody predicted the correct outcome of the game between '
                           f'{" and ".join(capt_nicks)}. Your bet of {amount} shazbucks has been returned to you.')
                await send_dm(user_id, msg)
            elif ratio == 1.0:
                transfer = (bot_user_id, user_id, amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WagerResult.CANCELLEDONESIDED)
                if change:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was changed. Nobody took '
                           f'your bet. Your bet of {amount} shazbucks has been returned to you.')
                else:
                    msg = (f'Hi {nick}. Nobody took your bet on the game between {" and ".join(capt_nicks)}. '
                           f'Your bet of {amount} shazbucks has been returned to you.')
                await send_dm(user_id, msg)
            elif prediction == game_result:
                win_amount = round(amount * ratio)
                if prediction == GameStatus.TIED:
                    win_amount = win_amount * TIE_PAYOUT_SCALE
                transfer = (bot_user_id, user_id, win_amount)
                create_transfer(conn, transfer)
                wager_result(conn, wager_id, WagerResult.WON)
                if change:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was changed. You correctly '
                           f'predicted the new result and have won {win_amount} shazbucks.')
                else:
                    msg = (f'Hi {nick}. You correctly predicted the game between '
                           f'{" and ".join(capt_nicks)}. You have won {win_amount} shazbucks.')
                await send_dm(user_id, msg)
                winner = await get_nick_from_discord_id(str(discord_id))
                winners.append((winner, win_amount))
            else:
                wager_result(conn, wager_id, WagerResult.LOST)
                if change:
                    msg = (f'Hi {nick}. The game between {" and ".join(capt_nicks)} was changed. You did not '
                           f'predict the new result correctly and have lost your bet of {amount} shazbucks.')
                else:
                    msg = (f'Hi {nick}. You lost your bet of {amount} shazbucks on the game between '
                           f'{" and ".join(capt_nicks)}.')
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
        cursor.execute(sql, (GameStatus.PICKING, GameStatus.INPROGRESS, search_str, search_str))
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
        cursor.execute(sql, (GameStatus.PICKING, GameStatus.INPROGRESS, search_str, search_str))
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
            teams = ('', '')
            if old_player_id_str in team1:
                teams = (team1.replace(old_player_id_str, new_player_id_str), team2)
                update_teams(conn, game_id, teams)
                if status == GameStatus.INPROGRESS:
                    await cancel_wagers(game_id, 'a player substitution')
                logger.info(f'Player {old_player} replaced by {new_player} in game {game_id}.')
                success = True
            elif old_player_id_str in team2:
                teams = (team1, team2.replace(old_player_id_str, new_player_id_str))
                update_teams(conn, game_id, teams)
                if status == GameStatus.INPROGRESS:
                    await cancel_wagers(game_id, 'a player substitution')
                logger.info(f'Player {old_player} replaced by {new_player} in game {game_id}.')
                success = True
            else:
                logger.error(f'Player {new_player} replaced {old_player}, and found game {game_id} with those players '
                             f'and PICKING or INPROGRESS status, but something went wrong!')
            team1_ids = [int(i) for i in teams[0].split()]
            team2_ids = [int(i) for i in teams[1].split()]
            if success:
                if status == GameStatus.INPROGRESS:
                    team1_win_chance = calculate_win_chance(conn, (team1_ids, team2_ids))
                    result_msg = (f'Player subbed, new predictions: Team 1 ({team1_win_chance:.1%}), Team 2 '
                                  f'({(1 - team1_win_chance):.1%}).')
                    await message.channel.send(result_msg)
                else:
                    player_ids = [team1_ids[0], team2_ids[0]]
                    player_ids.extend(team1_ids[1:])
                    best_team1_ids, best_team2_ids, best_chance_to_draw = suggest_even_teams(conn, player_ids)
                    team1_str = '<@!' + '>, <@!'.join([str(i) for i in best_team1_ids]) + '>'
                    team2_str = '<@!' + '>, <@!'.join([str(i) for i in best_team2_ids]) + '>'
                    result_msg = (f'Suggested teams: {team1_str} versus {team2_str} ({best_chance_to_draw:.1%} chance '
                                  f'to draw).')
                    await message.channel.send(result_msg)
        await message.add_reaction(REACTIONS[success])

    async def swap_player(message):
        success = False
        player1, player2 = message.content.replace('`', '').split(' has been swapped with ')
        player1_id_str = str((await query_members(player1)).id)
        player2_id_str = str((await query_members(player2)).id)
        search_str1 = f'%{player1_id_str}%'
        search_str2 = f'%{player2_id_str}%'
        values = (GameStatus.INPROGRESS, search_str1, search_str2, search_str2, search_str1)
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
            teams = ('', '')
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
            if success:
                team1_ids = [int(i) for i in teams[0].split()]
                team2_ids = [int(i) for i in teams[1].split()]
                team1_win_chance = calculate_win_chance(conn, (team1_ids, team2_ids))
                result_msg = (f'Player swapped, new predictions: Team 1 ({team1_win_chance:.1%}), Team 2 '
                              f'({(1 - team1_win_chance):.1%}).')
                await message.channel.send(result_msg)
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
