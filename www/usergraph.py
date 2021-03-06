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

from distutils.util import strtobool

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

if form.getfirst('gift'):
    gift = bool(strtobool(form.getfirst('gift')))
else:
    gift = False

# Set up the plot
fig, ax = plt.subplots(figsize=(8, 6))
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
            gift_balance = 0
            gift_balances = []
            timestamps = []
    
            for d in data:
                sender: int = d[2]
                receiver: int = d[3]
                amount: int = d[4]
                transfer_time: int = d[5]
    
                if sender == user_id and receiver != user_id:
                    balance -= amount
                    if gift and receiver != 1:
                        gift_balance -= amount

                if sender != user_id and receiver == user_id:
                    balance += amount
                    if gift and sender != 1:
                        gift_balance += amount
                
                balances.append(balance)
                if gift:
                    gift_balances.append(gift_balance)
                timestamps.append(transfer_time)
    
            dates = [dt.datetime.fromtimestamp(ts) for ts in timestamps]
            datenums = md.date2num(dates)
    
            # Plot the data
            ax.plot(datenums, balances, ls='-', drawstyle='steps-post', color=PLOT_COLORS[color_index], label=nick)
            if gift:
                ax.plot(datenums, gift_balances, ls='-.', drawstyle='steps-post', color=PLOT_COLORS[color_index], label=f'{nick} Gifts')
            color_index = (color_index + 1) % len(PLOT_COLORS)

# Finish plot
ax.set_ylabel('Shazbucks')
plt.xticks(rotation=25)
xfmt = md.DateFormatter('%d/%m/%Y')
ax.xaxis.set_major_formatter(xfmt)
plt.title(', '.join(nicks))
ax.legend()
ax.text(1.0, 0.5, 'Shazbucks by RedFox', transform=ax.transAxes, fontsize=10, color='gray',
        alpha=0.25, ha='right', va='center', rotation='vertical')

# Save the image to buffer
buf = BytesIO()
fig.savefig(buf, format='png')

# Send the image
sys.stdout.buffer.write(b'Content-type: image/png\r\n')
sys.stdout.buffer.write(b'\r\n')
sys.stdout.buffer.write(buf.getvalue())
