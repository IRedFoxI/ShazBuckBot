"""A discord bot to bet on PUGs."""

import asyncio
import atexit
import time
import unicodedata
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
BULLYBOT_DISCORD_ID = config['bullybot_discord_id']
PUG_CHANNEL_ID = config['pug_channel_id']
BOT_CHANNEL_ID = config['bot_channel_id']
GAME_RESULT = Enum('GAME_RESULT', 'InProgress Team1 Team2 Tied')
WAGER_RESULT = Enum('WAGER_RESULT', 'InProgress Won Lost Canceled')
TIME_TO_WAIT = 0.21
REACTIONS = ["👎", "👍"]


def normalize_caseless(text):
    return unicodedata.normalize("NFKD", text.casefold())


def caseless_equal(left, right):
    return normalize_caseless(left) == normalize_caseless(right)


def create_user(conn, user) -> int:
    """Create a new user into the users table

    :param sqlite3.Connection conn: The database connection to be used
    :param tuple[int,str,int,int] user: The discord_id, nick, mute_dm and
        balance
    :return: The id of the user created
    """
    user += (time.time(),)
    sql = ''' INSERT INTO users(discord_id,nick,mute_dm,balance,create_time)
              VALUES(?,?,?,?,?) '''
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
    cur.execute(f"SELECT {fields} FROM users WHERE id = ?", (user_id,))
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
    sql = f''' UPDATE users
               SET {fields_str}
               WHERE id = ?'''
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
    sql = ''' UPDATE users
               SET balance = balance + ?
               WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def create_transfer(conn, transfer) -> int:
    """Create a new transfer into the transfers table and update the balances

    :param sqlite3.Connection conn:Connection to the database
    :param tuple(int,int,int) transfer: Tuple of the user_id of the sender,
    user_id of the receiver and the amount to be transferred
    :return: The id of the transfer or 0 if an error occured
    """
    transfer += (time.time(),)
    sql = ''' INSERT INTO transfers(sender,receiver,amount,transfer_time)
              VALUES(?,?,?,?) '''
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
    :param tuple[str,float,str,str,int] game: Tuple with the details of the
        game
    :return: The id of the created game
    """
    game += (GAME_RESULT.InProgress.value,)
    sql = ''' INSERT INTO games(queue,start_time,team1,team2,result)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, game)
    conn.commit()
    return cur.lastrowid


def finish_game(conn, game_id, result) -> None:
    """Update a game into the games table with result

    :param sqlite3.Connection conn: The connection to the database
    :param int game_id: The id of the game to be finished
    :param int result: The result of the game in GAME_RESULT format
    """
    if result not in set(r.value for r in GAME_RESULT):
        raise ValueError()
    values = (result, game_id)
    sql = ''' UPDATE games
               SET result = ?
               WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()


def create_wager(conn, wager) -> int:
    """Create a new wager into the wagers table

    :param sqlite3.Connection conn: Connection to the database
    :param tuple[int,int,int,int] wager: Tuple with the details of the wager
    :return: The id of the created wager or 0 if an error occurred
    """
    wager += (WAGER_RESULT.InProgress.value, time.time())
    sql = ''' INSERT INTO wagers(user_id,game_id,prediction,amount,result,
                                 wager_time)
              VALUES(?,?,?,?,?,?) '''
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


def wager_result(conn, wager_id, result) -> None:
    """Update the result of a wager

    :param sqlite3.Connection conn: Connection to the database
    :param int wager_id: The id of the wager to be updated
    :param int result: Result of the wager in the format of WAGER_RESULT
    """
    if result not in set(r.value for r in WAGER_RESULT):
        raise ValueError()
    values = (result, wager_id)
    sql = ''' UPDATE wagers
               SET result = ?
               WHERE id = ?'''
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
            create_time REAL NOT NULL,
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
            transfer_time REAL NOT NULL,
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
            wager_time REAL NOT NULL,
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
    cur.execute("SELECT id FROM users WHERE discord_id = ?", (DISCORD_ID,))
    bot_user_id = cur.fetchone()[0]
    start_times = {}
    atexit.register(close_db, conn)

    async def send_dm(user_id, message) -> None:
        """Send a discord DM to the user

        :param int user_id: User iin our database
        :param str message: The message to be send to the user
        """
        (discord_id, mute_dm) = get_user_data(
            conn, user_id, 'discord_id,mute_dm')
        if not mute_dm:
            user = bot.get_user(discord_id)
            await asyncio.sleep(TIME_TO_WAIT)
            await user.create_dm()
            await user.dm_channel.send(message)

    @bot.event
    async def on_ready():
        print(f'{bot.user} is connected to the following guild(s):')
        for guild in bot.guilds:
            print(f'{guild.name}(id: {guild.id})')

    def in_channel(channel_id):
        def predicate(ctx):
            return ctx.message.channel.id == channel_id
        return commands.check(predicate)

    @bot.command(name='hello', help='Create account')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_hello(ctx):
        success = False
        discord_id = ctx.author.id
        nick = ctx.author.name
        cursor = conn.cursor()
        cursor.execute("SELECT id,nick FROM users WHERE discord_id = ?",
                       (discord_id,))
        data = cursor.fetchone()
        if data is None:
            user_id = create_user(conn, (discord_id, nick, 0, 0))
            if (user_id == 0 or
                    create_transfer(conn, (bot_user_id, user_id, INIT_BAL)
                                    ) == 0):
                await ctx.author.create_dm()
                await ctx.author.dm_channel.send(
                    f'Hi {ctx.author.name}, something went wrong creating your'
                    f' account. Please try again later or contact an admin.'
                )
                print(
                    f'Something went wrong creating an account for '
                    f'{ctx.author.name}. User id {user_id}.'
                )
            else:
                msg = (
                    f'Hi {ctx.author.name}, welcome! You have received an '
                    f'initial balance of {INIT_BAL} shazbucks, bet wisely!'
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
        cursor.execute("SELECT nick, balance FROM users WHERE discord_id = ?",
                       (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {data[0]}, your balance is {data[1]} shazbucks!'
            )
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='gift', help='Gift shazbucks to a discord user')
    async def cmd_gift(ctx, receiver: discord.Member, amount: int):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, nick, balance FROM users WHERE discord_id = ?",
            (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            sender_id = data[0]
            nick = data[1]
            balance = data[2]
            if balance < amount:
                msg = (
                    f'Hi {nick}, you do not have enough balance to transfer '
                    f'{amount} shazbucks to {receiver.name}! Your current '
                    f'balance is {balance} shazbucks.'
                )
                await send_dm(sender_id, msg)
            elif amount < 0:
                msg = (
                    f'Hi {nick}, you cannot gift a negative amount.'
                )
                await send_dm(sender_id, msg)
            else:
                discord_id = receiver.id
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, nick, balance FROM users WHERE discord_id = ?",
                    (discord_id,)
                )
                data = cursor.fetchone()
                if data is None:
                    msg = (
                        f'Hi {nick}, {receiver} does not have an'
                        f' account yet!'
                    )
                    await send_dm(sender_id, msg)
                else:
                    receiver_id = data[0]
                    receiver_nick = data[1]
                    receiver_bal = data[2]
                    transfer = (sender_id, receiver_id, amount)
                    if create_transfer(conn, transfer) == 0:
                        msg = (
                            f'Hi {nick}, your gift of {amount} shazbucks to '
                            f'{receiver} was somehow unsuccessful. Please '
                            f'try again later.'
                        )
                        await send_dm(sender_id, msg)
                        print(
                            f'{ctx.author.name} tried to gift {amount} '
                            f'shazbucks to {receiver_nick} but something went '
                            f'wrong.'
                        )
                    else:
                        balance -= amount
                        receiver_bal += amount
                        msg = (
                            f'Hi {nick}, your gift of {amount} shazbucks to '
                            f'{receiver.name} was successful. Your new balance'
                            f' is {balance} shazbucks.'
                        )
                        await send_dm(sender_id, msg)
                        msg = (
                            f'Hi {receiver_nick}, you have received a gift of '
                            f'{amount} shazbucks from {nick}. Your new '
                            f'balance is {receiver_bal} shazbucks.'
                        )
                        await send_dm(receiver_id, msg)
                        success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='bet', help='Bet shazbucks on a game. Winner should be '
                                  'either the name of the captain or 1, 2, '
                                  'Red or Blue.')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_bet(ctx, winner: str, amount: int):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute("SELECT id, nick, balance FROM users "
                       "WHERE discord_id = ?", (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            user_id = data[0]
            nick = data[1]
            balance = data[2]
            if balance < amount:
                msg = (
                    f'Hi {nick}, you do not have enough balance to '
                    f'bet {amount} shazbucks! Your current balance is '
                    f'{balance} shazbucks.'
                )
                await send_dm(user_id, msg)
            elif amount < 0:
                msg = (
                    f'Hi {nick}, you cannot bet a negative amount.'
                )
                await send_dm(user_id, msg)
            else:
                cursor = conn.cursor()
                cursor.execute("SELECT id,start_time,team1,team2 FROM "
                               "games WHERE result = ?",
                               (GAME_RESULT.InProgress.value,))
                games = cursor.fetchall()
                if not games:
                    msg = (
                        f'Hi {nick}. No games are running. Please wait until '
                        f'teams are picked.'
                    )
                    await send_dm(user_id, msg)
                else:
                    game_id = games[0][0]
                    prediction = 0
                    for game in games:
                        teams = game[2:4]
                        if (caseless_equal(winner, teams[0].split(':')[0]) or
                                winner == "1" or
                                caseless_equal(winner, "Red")):
                            game_id = game[0]
                            prediction = GAME_RESULT.Team1.value
                        elif (caseless_equal(winner, teams[1].split(':')[0]) or
                                winner == "2" or
                                caseless_equal(winner, "Blue")):
                            game_id = game[0]
                            prediction = GAME_RESULT.Team2.value
                    if prediction == 0:
                        msg = (
                            f'Hi {nick}, could not find a game '
                            f'captained by {winner}. Please check the spelling'
                            f' or wait until the teams have been picked.'
                        )
                        await send_dm(user_id, msg)
                    else:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT prediction "
                            "FROM wagers WHERE user_id = ? AND game_id = ?",
                            (user_id, game_id,)
                        )
                        prev_wager = cursor.fetchone()
                        if prev_wager and prediction != prev_wager[0]:
                            msg = (
                                f'Hi {nick}, you cannot bet against yourself!'
                            )
                            await send_dm(user_id, msg)
                        else:
                            wager = (user_id, game_id, prediction, amount)
                            if create_wager(conn, wager) == 0:
                                msg = (
                                    f'Hi {nick}, your bet of {amount} '
                                    f'shazbucks on {winner} was somehow '
                                    f'unsuccessful. Please try again later.'
                                )
                                await send_dm(user_id, msg)
                                print(
                                    f'{nick} tried to bet {amount} shazbucks '
                                    f'on {winner} but something went wrong. '
                                    f'User id {user_id}, game id {game_id}, '
                                    f'prediction {prediction}.'
                                )
                            else:
                                balance -= amount
                                msg = (
                                    f'Hi {ctx.author.name}, your bet of '
                                    f'{amount} shazbucks on {winner} was '
                                    f'successful. Your new balance is '
                                    f'{balance} shazbucks.'
                                )
                                await send_dm(user_id, msg)
                                success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='mute', help='Mute or unmute the bot\'s direct messages')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_mute(ctx):
        success = False
        discord_id = ctx.author.id
        cursor = conn.cursor()
        cursor.execute("SELECT id, mute_dm FROM users WHERE discord_id = ?",
                       (discord_id,))
        data = cursor.fetchone()
        if data is None:
            await ctx.author.create_dm()
            await ctx.author.dm_channel.send(
                f'Hi {ctx.author.name}, you do not have an account yet!'
            )
        else:
            user_id = data[0]
            mute_dm = (get_user_data(conn, user_id, 'mute_dm')[0]+1) % 2
            set_user_data(conn, user_id, ('mute_dm',), (mute_dm,))
            msg = f'Hi {ctx.author.name}, direct messages have been unmuted!'
            await send_dm(user_id, msg)
            success = True
        await ctx.message.add_reaction(REACTIONS[success])

    @bot.command(name='win', help='Simulate win result message')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_win(ctx):
        title = "Game 'NA' finished"
        description = '**Winner:** Team jet.Pixel\n**Duration:** 5 Minutes'
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.command(name='tie', help='Simulate tie result message')
    @in_channel(BOT_CHANNEL_ID)
    async def cmd_tie(ctx):
        title = "Game 'NA' finished"
        description = '**Tie game**\n**Duration:** 53 Minutes'
        embed_msg = discord.Embed(description=description,
                                  color=0x00ff00)
        await ctx.send(content='`{}`'.format(title.replace('`', '')),
                       embed=embed_msg)

    @bot.command(name='pick', help='Simulate picked message')
    @in_channel(BOT_CHANNEL_ID)
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
    @in_channel(BOT_CHANNEL_ID)
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
        # Print messages to stdout for debugging purposes
        if (message.author.id == BULLYBOT_DISCORD_ID
                or message.author.id == DISCORD_ID):
            print(
                f'{message.author} wrote in #{message.channel} on '
                f'{message.guild}: {message.content}'
            )
            for embed in message.embeds:
                print(f'{embed.description}')
        # Parse BullyBot's messages for game info
        # (and own messages during development)
        if (
                (message.author.id == BULLYBOT_DISCORD_ID
                 or message.author.id == DISCORD_ID)
                and message.channel.id == PUG_CHANNEL_ID
        ):
            if 'Game' in message.content:
                queue = message.content.split("'")[1]
                description = message.embeds[0].description
                if 'picked' in message.content:
                    teams = tuple(description.split('\n')[1:3])
                    start_time = (start_times[queue] if queue in start_times
                                  else time.time())
                    game = (queue,) + (start_time,) + teams
                    game_id = create_game(conn, game)
                    print(
                        f'Game {game_id} created in the {queue} queue:\n'
                        f'{teams[0]}\nversus\n{teams[1]}'
                    )
                elif 'finished' in message.content:
                    [result, duration] = description.split('\n')
                    duration = int(duration.split(' ')[1])
                    result_msg = ''
                    game_result = None
                    total_amounts = {}
                    winners_msg = f''
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
                        captains = [team.split(":")[0] for team in teams]
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
                            if winner == captains[0]:
                                game_result = GAME_RESULT.Team1.value
                            elif winner == captains[1]:
                                game_result = GAME_RESULT.Team2.value
                            else:
                                print(
                                    f'Winner {winner} not found in game '
                                    f'{game_id}:\n{teams[0]}\nversus\n'
                                    f'{teams[1]}'
                                )
                        if game_result != 0:
                            finish_game(conn, game_id, game_result)
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT wagers.id,user_id,prediction,amount,"
                                "nick,discord_id FROM users,wagers WHERE "
                                "game_id = ? AND users.id = wagers.user_id",
                                (game_id,)
                            )
                            wagers = cursor.fetchall()
                            for wager in wagers:
                                prediction = GAME_RESULT(wager[2]).name
                                amount = wager[3]
                                if prediction in total_amounts:
                                    total_amounts[prediction] += amount
                                else:
                                    total_amounts[prediction] = amount
                            ratio = 0
                            if (GAME_RESULT.Team1.name in total_amounts
                                    and GAME_RESULT.Team2.name in
                                    total_amounts):
                                ta_t1 = total_amounts[GAME_RESULT.Team1.name]
                                ta_t2 = total_amounts[GAME_RESULT.Team2.name]
                                if game_result == GAME_RESULT.Team1.value:
                                    ratio = (ta_t1 + ta_t2) / ta_t1
                                if game_result == GAME_RESULT.Team2.value:
                                    ratio = (ta_t1 + ta_t2) / ta_t2
                            for wager in wagers:
                                wager_id = wager[0]
                                user_id = wager[1]
                                prediction = wager[2]
                                amount = wager[3]
                                nick = wager[4]
                                discord_id = wager[5]
                                if game_result == GAME_RESULT.Tied.value:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id,
                                                 WAGER_RESULT.Canceled.value)
                                    msg = (
                                        f'Hi {nick}. The game captained by '
                                        f'{" and ".join(captains)} resulted '
                                        f'in a tie. Your bet of {amount} '
                                        f'shazbucks has been returned to you.'
                                    )
                                    await send_dm(user_id, msg)
                                elif ratio == 0:
                                    transfer = (bot_user_id, user_id, amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id,
                                                 WAGER_RESULT.Canceled.value)
                                    msg = (
                                        f'Hi {nick}. Nobody took your bet on '
                                        f'the game captained by '
                                        f'{" and ".join(captains)}. Your bet '
                                        f'of {amount} shazbucks has been '
                                        f'returned to you.'
                                    )
                                    await send_dm(user_id, msg)
                                elif prediction == game_result:
                                    win_amount = amount * ratio
                                    transfer = (bot_user_id, user_id,
                                                win_amount)
                                    create_transfer(conn, transfer)
                                    wager_result(conn, wager_id,
                                                 WAGER_RESULT.Won.value)
                                    msg = (
                                        f'Hi {nick}. You correctly predicted '
                                        f'the game captained by '
                                        f'{" and ".join(captains)}. You have '
                                        f'won {win_amount} shazbucks.'
                                    )
                                    await send_dm(user_id, msg)
                                    user = bot.get_user(discord_id)
                                    winners_msg += (
                                        # Use either user.mention or nick
                                        f'{user.mention}({win_amount}) '
                                    )
                                else:
                                    wager_result(conn, wager_id,
                                                 WAGER_RESULT.Lost.value)
                                    msg = (
                                        f'Hi {nick}. You lost your bet on '
                                        f'the game captained by '
                                        f'{" and ".join(captains)}. You have '
                                        f'lost {amount} shazbucks.')
                                    await send_dm(user_id, msg)
                    if game_result is None:
                        result_msg = '\'ERROR: Game not found\''
                    elif game_result == 0:
                        result_msg = '\'ERROR: Winner not found\''
                    elif game_result == GAME_RESULT.Tied.value:
                        if len(total_amounts) > 0:
                            result_msg = (
                                f'All wagers have been returned because the '
                                f'game resulted in a tie. '
                            )
                    elif (game_result == GAME_RESULT.Team1.value or
                          game_result == GAME_RESULT.Team2.value):
                        if len(total_amounts) == 1:
                            result_msg = (
                                f'The game only had bets on one team. All '
                                f'wagers have been returned.'
                            )
                        if len(total_amounts) == 2:
                            payout = 0
                            for value in total_amounts.values():
                                payout += value
                            result_msg = (winners_msg +
                                          f'were paid out a total of {payout} '
                                          f'shazbucks.')
                    if result_msg:
                        await message.channel.send(result_msg)
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
