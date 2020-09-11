import redis
import json
import argparse
import psycopg2
from psycopg2 import *
from datetime import datetime
from pytz import timezone
import pytz
import geocode
from geocode import geocode



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
cur.execute("CREATE TABLE IF NOT EXISTS twitter.police (id BIGINT, createdate TIMESTAMP, body VARCHAR, username VARCHAR, url VARCHAR, location VARCHAR, address VARCHAR, neighborhood VARCHAR, city VARCHAR, zipcode VARCHAR, lat NUMERIC, lng NUMERIC, incident_type VARCHAR, urgency VARCHAR)")
cur.execute("CREATE TABLE IF NOT EXISTS twitter.fire (id BIGINT, createdate TIMESTAMP, body VARCHAR, username VARCHAR, url VARCHAR, location VARCHAR, address VARCHAR, neighborhood VARCHAR, city VARCHAR, zipcode VARCHAR, lat NUMERIC, lng NUMERIC, incident_type VARCHAR, urgency VARCHAR)")

i = redis.llen(args.query)

while i > 0:

    data = json.loads(redis.lpop(args.query)) 

    # Check if the retweet indicator is in the text body. Do not process retweets. 
    isRetweet = False
    if 'RT @pdxpolicelog: ' in str(data['text']):
        isRetweet = True

    if isRetweet != True:

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
        if str(data['username']) == 'pdxpolicelog':
            try:
                cur.execute("INSERT INTO twitter.police (id, createdate, body, username, url, location, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), geocodeJson['raw']['neighborhood'], geocodeJson['city'], geocodeJson['postal'], geocodeJson['lat'], geocodeJson['lng'], str(incidentType), str(urgency)))
            except:
                cur.execute("INSERT INTO twitter.police (id, createdate, body, username, url, location, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), '','', '', None, None, str(incidentType), str(urgency)))

        if str(data['username']) == 'pdxfirelog':
            try:
                cur.execute("INSERT INTO twitter.fire (id, createdate, body, username, url, location, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), geocodeJson['raw']['neighborhood'], geocodeJson['city'], geocodeJson['postal'], geocodeJson['lat'], geocodeJson['lng'], str(incidentType), str(urgency)))
            except:
                cur.execute("INSERT INTO twitter.fire (id, createdate, body, username, url, location, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(data['url']), str(data['location']), str(address), '','', '', None, None, str(incidentType), str(urgency)))


    i = i - 1

conn.commit()
conn.close()

    
