#!/usr/bin/python3
# -*- coding: utf-8 -*-

from io import BytesIO
import sys

import yaml
import sqlite3

import os

os.environ['MPLCONFIGDIR'] = '/opt/shazbuckbot/www/matplotlib'
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.style.use('ggplot')
import matplotlib.dates as md

import datetime as dt

import cgi
import cgitb

cgitb.enable()

config = yaml.safe_load(open("/opt/shazbuckbot/config.yml"))
DATABASE = config['database']
PLOT_COLORS = ['b', 'r', 'g', 'c', 'm', 'y', 'w']

# get cgi object
form = cgi.FieldStorage()

if form.getlist('discord_id'):
    discord_ids = form.getlist('discord_id')
else:
    discord_ids = ['292031989773500416', '347125254050676738']

# Set up the plot
fig, ax = plt.subplots(figsize=(8, 6))
color_index = 0

nicks = []

for discord_id in discord_ids:

    if discord_id.isdigit():

        conn = sqlite3.connect(DATABASE)

        nick = str(discord_id)
        sql = ''' SELECT nick FROM users WHERE discord_id = ? '''
        cur = conn.cursor()
        cur.execute(sql, (discord_id,))
        user = cur.fetchone()
        if user:
            nick = user[0]
        nicks.append(nick)

        cur = conn.cursor()
        sql = ''' SELECT pick_time, trueskill 
                  FROM trueskills, games 
                  WHERE discord_id = ? AND games.id = game_id '''
        values = (int(discord_id),)
        cur.execute(sql, values)
        data = cur.fetchall()
        conn.close()

        if data:

            timestamps = []
            trueskills = []

            for d in data:

                pick_time: int = d[0]
                trueskill: float = d[1]

                timestamps.append(pick_time)
                trueskills.append(trueskill)

            dates = [dt.datetime.fromtimestamp(ts) for ts in timestamps]
            datenums = md.date2num(dates)

            # Plot the data
            label_txt = f'{nick} ({len(dates)} games)'
            ax.plot(datenums, trueskills, ls='-', drawstyle='steps-post', color=PLOT_COLORS[color_index],
                    label=label_txt)

            color_index = (color_index + 1) % len(PLOT_COLORS)

# Finish plot
ax.set_ylabel('TrueSkill')
plt.xticks(rotation=25)
xfmt = md.DateFormatter('%d/%m/%Y')
ax.xaxis.set_major_formatter(xfmt)
plt.title(', '.join(nicks))
ax.legend()
ax.text(1.0, 0.5, 'TrueSkill by RedFox', transform=ax.transAxes, fontsize=10, color='gray',
        alpha=0.25, ha='right', va='center', rotation='vertical')

# Save the image to buffer
buf = BytesIO()
fig.savefig(buf, format='png')

# Send the image
sys.stdout.buffer.write(b'Content-type: image/png\r\n')
sys.stdout.buffer.write(b'\r\n')
sys.stdout.buffer.write(buf.getvalue())
