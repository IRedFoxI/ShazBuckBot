#!/usr/bin/python3
# -*- coding: utf-8 -*-

from io import BytesIO
import sys

import yaml
import sqlite3

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
fig, ax = plt.subplots(figsize=(6, 5))
color_index = 0

nicks = []

for discord_id in discord_ids:

    if discord_id.isdigit():
        sql = ''' SELECT users.id, nick, sender, receiver, amount, transfer_time 
                  FROM users, transfers 
                  WHERE discord_id = ? AND (sender = users.id OR receiver = users.id) '''
        values = (int(discord_id),)
    
        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute(sql, values)
        data = cur.fetchall()
        conn.close()
    
        if data:
    
            user_id: int = data[0][0]
            nick: str = data[0][1]
            nicks.append(nick)
    
            balance = 0
            balances = []
            timestamps = []
    
            for d in data:
                sender: int = d[2]
                receiver: int = d[3]
                amount: int = d[4]
                transfer_time: int = d[5]
    
                balance = balance - amount if sender == user_id else balance + amount
                balances.append(balance)
                timestamps.append(transfer_time)
    
            dates = [dt.datetime.fromtimestamp(ts) for ts in timestamps]
            datenums = md.date2num(dates)
    
            # Plot the data
            ax.plot(datenums, balances, ls='-', drawstyle='steps-post', color=PLOT_COLORS[color_index], label=nick)
            color_index = (color_index + 1) % len(PLOT_COLORS)

# Finish plot
ax.set_ylabel('Shazbucks')
plt.xticks(rotation=25)
# xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
xfmt = md.DateFormatter('%d/%m/%Y')
ax.xaxis.set_major_formatter(xfmt)
plt.title(', '.join(nicks))
ax.legend()

# Save the image to buffer
buf = BytesIO()
fig.savefig(buf, format='png')

sys.stdout.buffer.write(b'Content-type: image/png\r\n')
sys.stdout.buffer.write(b'\r\n')
sys.stdout.buffer.write(buf.getvalue())
