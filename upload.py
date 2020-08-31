import redis
import json
import argparse
import logging
import psycopg2
from psycopg2 import *
from datetime import datetime
from pytz import timezone
import pytz
import geocode
from geocode import geocode


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

try:
    conn = psycopg2.connect("dbname='postgres'")
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
cur.execute("CREATE TABLE IF NOT EXISTS twitter.police (id BIGINT, createdate TIMESTAMP, body VARCHAR, username VARCHAR, url VARCHAR, location VARCHAR, address VARCHAR, neighborhood VARCHAR, zipcode VARCHAR, lat NUMERIC, lng NUMERIC, incident_type VARCHAR, urgency VARCHAR)")

i = redis.llen(args.query)

while i > 0:

    data = json.loads(redis.lpop(args.query)) 

    # Check if the retweet indicator is in the text body. Do not process retweets.
    isRetweet = False
    if 'RT @pdxpolicelog: ' in str(data['text']):
        isRetweet = True

    if isRetweet != True:

        # UTC to PST conversion.
        datetime_object = datetime.strptime(json.loads(data['created_at']), '%Y-%m-%dT%H:%M:%S')

        utc_datetime = pytz.utc.localize(datetime_object)

        pst_datetime = utc_datetime.astimezone(pytz.timezone("America/Los_Angeles"))
        pst_datestring = datetime.strftime(pst_datetime, '%Y-%m-%d %I:%M %p')

        # Tweets are usually formatted like this ROBBERY - COLD at 1200 SW ALDER ST, PORT [id]
        # So everything prior to the 'at' are the incident details, everything between the 'at' and the '[' are the incident details.
        at = 'at'
        startBracket = '['

        atIndex  = data['text'].find(at)
        startBracketIndex = data['text'].find(startBracket)

        address = data['text'][atIndex + 2:startBracketIndex].replace(', PORT','').strip()

        incidentDetails = data['text'][:atIndex - 1 ].strip()

        if '-' in incidentDetails:
            incidentDetailArray = incidentDetails.split('-')
            incidentType = incidentDetailArray[0].strip()
            urgency = incidentDetailArray[1].strip()
        else:
            incidentType = incidentDetails
            urgency = 'NOT PROVIDED'

        # If geocode is successful, upload latitude, longitude, neighborhood and zip code information. Otherwise, just upload the base data.
        
        geocodeJson = geocode(address)
        try:
            cur.execute("INSERT INTO twitter.police (id, createdate, body, username, url, location, address, neighborhood, zipcode, lat, lng, incident_type, urgency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(pst_datestring), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), geocodeJson['raw']['neighborhood'], geocodeJson['postal'], geocodeJson['lat'], geocodeJson['lng'], str(incidentType), str(urgency)))
        except:
            cur.execute("INSERT INTO twitter.police (id, createdate, body, username, url, location, address, neighborhood, zipcode, lat, long, incident_type, urgency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(pst_datestring), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), '','', None, None, str(incidentType), str(urgency)))

        
    i = i - 1

conn.commit()
conn.close()

    
