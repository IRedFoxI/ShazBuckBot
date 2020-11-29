# This is a discord bot to bet on PUGs.

import time
from enum import Enum
import yaml
import sqlite3

import discord
from discord.ext import commands

config = yaml.safe_load(open("config.yml"))
TOKEN = config['token']
DATABASE = config['database']
DISCORD_ID = config['discord_id']
INIT_BAL = config['init_bal']
GAME_RESULT = Enum('RESULT', 'InProgress Team1 Team2 Tied')
WAGER_RESULT = Enum('RESULT', 'InProgress Won Lost Canceled')


def create_user(conn, user):
    """
    Create a new user into the users table
    :param conn:
    :param user:
    :return: user id
    """
    sql = ''' INSERT INTO users(discord_id,nick,balance)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, user)
    conn.commit()
    return cur.lastrowid


def get_discord_id(conn, user_id):
    cur = conn.cursor()
    cur.execute("SELECT discord_id FROM users WHERE id = ?", (user_id,))
    return cur.fetchone()[0]


def change_balance(conn, balance_update):
    """
    Change the balance of a user
    :param conn:
    :param balance_update:
    """
    sql = ''' UPDATE users
               SET balance = balance + ?
               WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, balance_update)
    conn.commit()


def create_transfer(conn, transfer):
    """
    Create a new transfer into the transfers table and update the balances
    :param conn:
    :param transfer:
    :return: transfer id
    """
    sql = ''' INSERT INTO transfers(sender,receiver,amount)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, transfer)
    conn.commit()
    if (change_balance(conn, (-transfer[2], transfer[0])) == 0 or
            change_balance(conn, (transfer[2], transfer[1])) == 0):
        return 0
    else:
        return cur.lastrowid


def create_game(conn, game):
    """
    Create a new game into the games table
    :param conn:
    :param game:
    :return: game id
    """
    game += (GAME_RESULT.InProgress.value,)
    sql = ''' INSERT INTO games(queue,start_time,team1,team2,result)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, game)
    conn.commit()
    return cur.lastrowid


def finish_game(conn, game):
    """
    Update a game into the games table with result
    :param conn:
    :param game:
    """
    sql = ''' UPDATE games
               SET result = ?
               WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, game)
    conn.commit()


def create_wager(conn, wager):
    """
    Create a new wager into the wagers table
    :param conn:
    :param wager:
    :return: wager id
    """
    wager += (WAGER_RESULT.InProgress.value,)
    sql = ''' INSERT INTO wagers(user_id,game_id,prediction,amount,result)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, wager)
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE discord_id = ?", (DISCORD_ID,))
    bot_user_id = cur.fetchone()[0]
    transfer = (wager[0], bot_user_id, wager[3])
    if create_transfer(conn, transfer) == 0:
        return 0
    else:
        return cur.lastrowid


def wager_result(conn, wager_update):
    """
    Update the result of a wager
    :param conn:
    :param wager_update:
    """
    sql = ''' UPDATE wagers
               SET result = ?
               WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, wager_update)
    conn.commit()


def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            discord_id INTEGER NOT NULL,
            nick TEXT NOT NULL,
            balance INTEGER NOT NULL
        );
    """)
    cur = conn.cursor()
    cur.execute("SELECT rowid FROM users WHERE discord_id = ?", (DISCORD_ID,))
    data = cur.fetchone()
    if data is None:
        create_user(conn, (DISCORD_ID, 'ShazBuckBot', 0))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            sender INTEGER NOT NULL,
            receiver INTEGER NOT NULL,
            amount INTEGER NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            queue TEXT NOT NULL,
            start_time REAL NOT NULL,
            team1 TEXT NOT NULL,
            team2 TEXT NOT NULL,
            result INTEGER NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wagers (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            game_id INTEGER NOT NULL,
            prediction INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            result INTEGER NOT NULL
        );
    """)


def start_bot():
    bot = commands.Bot(command_prefix='!')
    conn = sqlite3.connect(DATABASE)
    init_db(conn)
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE discord_id = ?", (DISCORD_ID,))
    bot_user_id = cur.fetchone()[0]
    start_times = {}

    @bot.event
    async def on_ready():
        print(f'{bot.user} is connected to the following guild(s):')
        for guild in bot.guilds:
            print(f'{guild.name}(id: {guild.id})')

    @bot.command(name='hello', help='Create account')
    async def cmd_hello(ctx):
        discord_id = ctx.author.id
        nick = ctx.author.name
        cursor = conn.cursor()
        cursor.execute("SELECT rowid FROM users WHERE discord_id = ?", (discord_id,))
        data = cursor.fetchone()
        if data is None:
            user_id = create_user(conn, (discord_id, nick, 0))
            if (user_id == 0 or
                    create_transfer(conn, (bot_user_id, user_id, INIT_BAL)
                                    ) == 0):
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, something went wrong creating your'
                    f' account. Please try again later.'
                )
                print(
                    f'Something went wrong creating an account for '
                    f'{ctx.author.name}. User id {user_id}.'
                )
            else:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, welcome! You have received an initial '
                    f'balance of {INIT_BAL} shazbucks, bet wisely!'
                )
        else:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you already have an account!'
            )

    @bot.command(name='balance', help='Check balance')
    async def cmd_balance(ctx):
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM users WHERE discord_id = ?", (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, your balance is {data[0]} shazbucks!'
            )

    @bot.command(name='gift', help='Gift shazbucks')
    async def cmd_gift(ctx, member: discord.Member, amount: int):
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute("SELECT id, balance FROM users WHERE discord_id = ?",
                       (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            sender_id = data[0]
            balance = data[1]
            if balance < amount:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, you do not have enough balance to'
                    f'transfer {amount} shazbucks to {member.name}!'
                    f'Your current balance is {balance} shazbucks.'
                )
            elif amount < 0:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, you cannot gift a negative '
                    f'amount.'
                )
            else:
                discord_id = member.id
                cursor = conn.cursor()
                cursor.execute("SELECT id, balance FROM users WHERE discord_id = ?",
                               (discord_id,))
                data = cursor.fetchone()
                if data is None:
                    await ctx.author.create_dm()
                    await ctx.author.dm_channel.send(
                        f'Hi {ctx.author.name}, {member.name} does not have an'
                        f' account yet!'
                    )
                else:
                    receiver_id = data[0]
                    transfer = (sender_id, receiver_id, amount)
                    if create_transfer(conn, transfer) == 0:
                        await ctx.author.create_dm()
                        await ctx.author.dm_channel.send(
                            f'Hi {ctx.author.name}, your gift of {amount} '
                            f'shazbucks to {member.name} was somehow '
                            f'unsuccessful. Please try again later.'
                        )
                        print(
                            f'{ctx.author.name} tried to gift {amount} '
                            f'shazbucks to {member.name} but something went '
                            f'wrong.'
                        )
                    else:
                        balance -= amount
                        await ctx.author.create_dm()
                        await ctx.author.dm_channel.send(
                            f'Hi {ctx.author.name}, your gift of {amount} '
                            f'shazbucks to {member.name} was successful. '
                            f'Your new balance is {balance} shazbucks.'
                        )
                        await member.create_dm()
                        await member.dm_channel.send(
                            f'Hi {member.name}, you have received a gift of '
                            f'{amount} shazbucks from {ctx.author.name}. '
                            f'Your new balance is {data[1]} shazbucks.'
                        )

    @bot.command(name='bet', help='Bet shazbucks on game')
    async def cmd_bet(ctx, winner: str, amount: int):
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute("SELECT id, balance FROM users WHERE discord_id = ?",
                       (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            user_id = data[0]
            balance = data[1]
            if balance < amount:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, you do not have enough balance to'
                    f'bet {amount} shazbucks! Your current balance is '
                    f'{balance} shazbucks.'
                )
            elif amount < 0:
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, you cannot bet a negative amount.'
                )
            else:
                cursor = conn.cursor()
                cursor.execute("SELECT id,start_time,team1,team2 FROM "
                               "games WHERE result = ?",
                               (GAME_RESULT.InProgress.value,))
                games = cursor.fetchall()
                if not games:
                    await ctx.author.create_dm()
                    await ctx.author.dm_channel.send(
                        f'Hi {ctx.author.name}, no games running. Please wait '
                        f'until teams are picked.'
                    )
                else:
                    game_id = games[0][0]
                    prediction = 0
                    for game in games:
                        teams = game[2:4]
                        if winner == teams[0].split(':')[0]:
                            game_id = game[0]
                            prediction = GAME_RESULT.Team1.value
                        elif winner == teams[1].split(':')[0]:
                            game_id = game[0]
                            prediction = GAME_RESULT.Team2.value
                    if prediction == 0:
                        await ctx.author.create_dm()
                        await ctx.author.dm_channel.send(
                            f'Hi {ctx.author.name}, could not find a game '
                            f'captained by {winner}. Please check the spelling'
                            f' or wait until the teams have been picked. '
                        )
                    else:
                        wager = (user_id, game_id, prediction, amount)
                        if create_wager(conn, wager) == 0:
                            await ctx.author.create_dm()
                            await ctx.author.dm_channel.send(
                                f'Hi {ctx.author.name}, your bet of {amount} '
                                f'shazbucks on {winner} was somehow '
                                f'unsuccessful. Please try again later.'
                            )
                            print(
                                f'{ctx.author.name} tried to bet {amount} '
                                f'shazbucks on {winner} but something went '
                                f'wrong. User id {game_id}, game id {game_id},'
                                f' , prediction {prediction}.'
                            )
                        else:
                            balance -= amount
                            await ctx.author.create_dm()
                            await ctx.author.dm_channel.send(
                                f'Hi {ctx.author.name}, your bet of {amount} '
                                f'shazbucks on {winner} was successful. Your '
                                f'new balance is {balance} shazbucks.'
                            )

    @bot.command(name='win', help='Simulate win result message')
    async def cmd_win(ctx):
        title = "Game 'NA' finished"
        description = '**Winner:** Team jet.Pixel\n**Duration:** 5 Minutes'
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.command(name='tie', help='Simulate tie result message')
    async def cmd_tie(ctx):
        title = "Game 'NA' finished"
        description = '**Tie game**\n**Duration:** 53 Minutes'
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.command(name='pick', help='Simulate picked message')
    async def cmd_picked(ctx):
        title = "Game 'NA' teams picked"
        description = ('**Teams**:\n'
                       'jet.Pixel: joey, thecaptaintea, yami, r.$.e\n'
                       'eligh_: iloveoob, Lögïc, GUNDERSTRUTT, Crysta\n'
                       '\n'
                       '**Maps**: Elite, Exhumed')
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.command(name='begin', help='Simulate begin message')
    async def cmd_begin(ctx):
        title = "Game 'NA' has begun"
        description = ('**Captains: @jet.Pixel & @eligh_**\n'
                       'joey, thecaptaintea, yami, r.$.e, iloveoob, Lögïc, '
                       'GUNDERSTRUTT, Crysta')
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.event
    async def on_message(message):
        if (message.author.id == 292031989773500416  # RedFox
                or message.author.id == 359925573134319628  # BullyBot
                or message.author.id == 776567538867503134):  # ShazBuckBot
            print(
                f'{message.author} wrote in #{message.channel} on {message.guild}: {message.content}'
            )
            for embed in message.embeds:
                print(f'{embed.description}')
        if (message.author.id == 359925573134319628  # BullyBot
                or message.author.id == 776567538867503134):  # ShazBuckBot
            if 'Game' in message.content:
                queue = message.content.split("'")[1]
                description = message.embeds[0].description
                if 'picked' in message.content:
                    teams = tuple(description.split('\n')[1:3])
                    start_time = start_times[queue] if queue in start_times \
                        else time.time()
                    game = (queue,) + (start_time,) + teams
                    game_id = create_game(conn, game)
                    print(
                        f'Game {game_id} created in the {queue} queue:\n'
                        f'{teams[0]}\nversus\n{teams[1]}'
                    )
                elif 'finished' in message.content:
                    [result, duration] = description.split('\n')
                    duration = int(duration.split(' ')[1])
                    # Find the game that just finished
                    cursor = conn.cursor()
                    cursor.execute("SELECT id,start_time,team1,team2 FROM "
                                   "games WHERE queue = ? AND result = ?",
                                   (queue, GAME_RESULT.InProgress.value))
                    games = cursor.fetchall()
                    if not games:
                        print(
                            f'Game finished in {queue} queue, but no game '
                            f'In Progress in that queue.'
                        )
                    else:
                        game_id = games[0][0]
                        teams = games[0][2:4]
                        if len(games) > 1:
                            durations = [abs((time.time() - game[1]) // 60
                                             - duration) for game in games]
                            _, idx = min((val, idx) for (idx, val)
                                         in enumerate(durations))
                            game_id = games[idx][0]
                            teams = games[idx][2:4]
                        game_result = 0
                        if 'Tie' in result:
                            game_result = GAME_RESULT.Tied.value
                        else:
                            winner = " ".join(result.split(' ')[2:])
                            if winner == teams[0].split(':')[0]:
                                game_result = GAME_RESULT.Team1.value
                            elif winner == teams[1].split(':')[0]:
                                game_result = GAME_RESULT.Team2.value
                            else:
                                print(
                                    f'Winner {winner} not found in game '
                                    f'{game_id}:\n{teams[0]}\nversus\n'
                                    f'{teams[1]}'
                                )
                        if game_result != 0:
                            game = (game_result, game_id)
                            finish_game(conn, game)
                            cursor = conn.cursor()
                            cursor.execute("SELECT id,user_id,prediction,"
                                           "amount FROM wagers WHERE "
                                           "game_id = ?", (game_id,))
                            wagers = cursor.fetchall()
                            total_amount = {}
                            for wager in wagers:
                                prediction = GAME_RESULT(wager[2]).name
                                amount = wager[3]
                                if prediction in total_amount:
                                    total_amount[prediction] += amount
                                else:
                                    total_amount[prediction] = amount
                            ratio = 0
                            if (GAME_RESULT.Team1.name in total_amount
                                    and GAME_RESULT.Team2.name in
                                    total_amount):
                                if game_result == GAME_RESULT.Team1.value:
                                    ratio = (total_amount[GAME_RESULT.Team1.name]
                                             / total_amount[GAME_RESULT.Team2.name])
                                if game_result == GAME_RESULT.Team2.value:
                                    ratio = (total_amount[GAME_RESULT.Team2.name]
                                             / total_amount[GAME_RESULT.Team1.name])
                            for wager in wagers:
                                wager_id = wager[0]
                                user_id = wager[1]
                                prediction = wager[2]
                                amount = wager[3]
                                if game_result == GAME_RESULT.Tied.value:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_update = (
                                        WAGER_RESULT.Canceled.value,
                                        wager_id
                                    )
                                    wager_result(conn, wager_update)
                                    discord_id = get_discord_id(conn, user_id)
                                    user = bot.get_user(discord_id)
                                    await user.create_dm()
                                    await user.dm_channel.send(
                                        f'Hi {user.name}. The game {teams[0]} '
                                        f'versus {teams[1]} resulted in a tie.'
                                        f' Your bet of {amount} shazbucks has '
                                        f'been returned to you.'
                                    )
                                elif ratio == 0:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_update = (
                                        WAGER_RESULT.Canceled.value,
                                        wager_id
                                    )
                                    wager_result(conn, wager_update)
                                    discord_id = get_discord_id(conn, user_id)
                                    user = bot.get_user(discord_id)
                                    await user.create_dm()
                                    await user.dm_channel.send(
                                        f'Hi {user.name}. Nobody took your '
                                        f'bet on the game {teams[0]} versus '
                                        f'{teams[1]}. Your bet of {amount} '
                                        f'shazbucks has been returned to you.'
                                    )
                                elif prediction == game_result:
                                    win_amount = amount * ratio
                                    transfer = (bot_user_id, user_id,
                                                win_amount)
                                    create_transfer(conn, transfer)
                                    wager_update = (
                                        WAGER_RESULT.Won.value,
                                        wager_id
                                    )
                                    wager_result(conn, wager_update)
                                    discord_id = get_discord_id(conn, user_id)
                                    user = bot.get_user(discord_id)
                                    await user.create_dm()
                                    await user.dm_channel.send(
                                        f'Hi {user.name}. You correctly '
                                        f'predicted the game {teams[0]} '
                                        f'versus {teams[1]}. You have won '
                                        f'{win_amount} shazbucks.'
                                    )
                                else:
                                    wager_update = (
                                        WAGER_RESULT.Lost.value,
                                        wager_id
                                    )
                                    wager_result(conn, wager_update)
                                    discord_id = get_discord_id(conn, user_id)
                                    user = bot.get_user(discord_id)
                                    await user.create_dm()
                                    await user.dm_channel.send(
                                        f'Hi {user.name}. You lost your bet on'
                                        f' the game {teams[0]} versus '
                                        f'{teams[1]}. You have lost {amount} '
                                        f'shazbucks.'
                                    )
                elif 'begun' in message.content:
                    if queue in start_times:
                        print(
                            f'Queue {queue} already has a game picking. Cannot'
                            f' handle two at the same time.'
                        )
                    else:
                        start_times[queue] = time.time()
        await bot.process_commands(message)

    @bot.event
    async def on_command_error(ctx, error):
        if isinstance(error, commands.errors.CommandNotFound):
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}. {error}!'
            )

    bot.run(TOKEN)


# Main
if __name__ == '__main__':
    start_bot()
