import redis
import json
import argparse
import logging
import psycopg2
from psycopg2 import *
from datetime import datetime
from pytz import timezone
import pytz


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

try:
    conn = psycopg2.connect("dbname='postgres' host ='localhost'")
except:
    print("Can't connect to PSQL!")
try:
    redis = redis.StrictRedis(host='localhost', port=6379, db=0)
except:
    print("Can't connect to Redis!")

parser = argparse.ArgumentParser()
parser.add_argument("-q",
                    "--queries",
                    dest="query",
                    help="Give names of queries stored in Redis.",
                    default='-')

args = parser.parse_args()

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS twitter.police (id BIGINT, createdate TIMESTAMP, body VARCHAR, username VARCHAR, url VARCHAR, location VARCHAR, address VARCHAR, incident_type VARCHAR, urgency VARCHAR)")

i = redis.llen(args.query)

while i > 0:

    data = json.loads(redis.lpop(args.query)) 
    datetime_object = datetime.strptime(json.loads(data['created_at']), '%Y-%m-%dT%H:%M:%S')

    utc_datetime = pytz.utc.localize(datetime_object)

    pst_datetime = utc_datetime.astimezone(pytz.timezone("America/Los_Angeles"))
    pst_date = datetime.strftime(pst_datetime, '%Y-%m-%d %I:%M %p')
    print(pst_date)
    print(str(pst_date))
    at = 'at'
    startBracket = '['

    atIndex  = data['text'].find(at)
    startBracketIndex = data['text'].find(startBracket)

    address = data['text'][atIndex + 2:startBracketIndex].replace(', PORT','').strip()

    incidentDetails = data['text'][:atIndex - 1 ].replace('RT @pdxpolicelog: ','').strip()

    if '-' in incidentDetails:
        incidentDetailArray = incidentDetails.split('-')
        incidentType = incidentDetailArray[0].strip()
        urgency = incidentDetailArray[1].strip()
    else:
        incidentType = incidentDetails
        urgency = 'NOT PROVIDED'

    cur.execute("INSERT INTO twitter.police (id, createdate, body, username, url, location, address, incident_type, urgency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(pst_date), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), str(incidentType), str(urgency)))
    
    i = i - 1

conn.commit()
conn.close()

    