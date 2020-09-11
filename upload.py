import redis
import json
import psycopg2
from psycopg2 import *
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

cur = conn.cursor()

i = redis.llen('event')

while i > 0:

    data = json.loads(redis.lpop('event')) 

    policeTweet = False
    if str(data['username']) == 'pdxpolicelog':
        policeTweet = True

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

            # Handle minor format difference.

            if policeTweet:
                incidentType = incidentDetailArray[0].strip()
                urgency = incidentDetailArray[1].strip()
            else:
                incidentType = incidentDetailArray[1].strip()
                urgency = incidentDetailArray[0].strip()             
        else:
            incidentType = incidentDetails
            urgency = 'NOT PROVIDED'

        # If geocode is successful, upload latitude, longitude, neighborhood and zip code information. Otherwise, just upload the base data.
        
        geocodeJson = geocode(address)
        if policeTweet:
            try:
                cur.execute("INSERT INTO twitter.police (id, createdate, body, username, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(address), geocodeJson['raw']['neighborhood'], geocodeJson['city'], geocodeJson['postal'], geocodeJson['lat'], geocodeJson['lng'], str(incidentType), str(urgency)))
            except:
                cur.execute("INSERT INTO twitter.police (id, createdate, body, username, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(address), '','', '', None, None, str(incidentType), str(urgency)))

        else:
            try:
                cur.execute("INSERT INTO twitter.fire (id, createdate, body, username, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']), str(address), geocodeJson['raw']['neighborhood'], geocodeJson['city'], geocodeJson['postal'], geocodeJson['lat'], geocodeJson['lng'], str(incidentType), str(urgency)))
            except:
                cur.execute("INSERT INTO twitter.fire (id, createdate, body, username, address, neighborhood, city, zipcode, lat, lng, incident_type, urgency) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(data['id']), str(data['text']), str(data['username']) str(address), '','', '', None, None, str(incidentType), str(urgency)))


    i = i - 1

conn.commit()
conn.close()

    
