#!/usr/bin/python3
# -*- coding: utf-8 -*-

import cgi
import yaml
import sqlite3
import json
# import cgitb
# cgitb.enable()

print("Content-Type:application/json;charset=utf-8\n")

config = yaml.safe_load(open("/opt/shazbuckbot/config.yml"))
DATABASE = config['database']

# get cgi object
form = cgi.FieldStorage()

sql = ''' SELECT discord_id, nick, balance FROM users '''
values = ()

if form.getvalue('discord_id'):
    discord_id = form.getvalue('discord_id')
    if discord_id.isdigit():
        sql = ''' SELECT discord_id, nick, balance FROM users WHERE discord_id = ? '''
        values = (int(discord_id),)

conn = sqlite3.connect(DATABASE)
cur = conn.cursor()
cur.execute(sql, values)
data = [
    {cur.description[i][0]: value for i, value in enumerate(row)}
    for row in cur.fetchall()
]
conn.close()

if data:
    if len(data) == 1:
        print(json.dumps(data[0]))
    else:
        print(json.dumps(data))
else:
    print('ERROR')
