from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient

MONGO_HOST = 'mongodb://localhost/usa_db'
# assuming you have mongoDB installed locally
# and a database called 'usa_db'


keys_file = open("data/keys.txt")
lines = keys_file.readlines()
consumer_key = lines[0].rstrip()
consumer_secret = lines[1].rstrip()
access_token = lines[2].rstrip()
access_token_secret = lines[3].rstrip()

class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = MongoClient(MONGO_HOST)

            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.usa_db

            # Decode the JSON from Twitter
            datajson = json.loads(data) 

            # grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']
            created_at = datajson['created_at']
            coordinates = datajson['coordinates']
            place_type = datajson['place']['place_type']

            # insert into colletcion twitter_location only if coordinates in not None
            if coordinates is not None and place_type == 'city':
                print("Tweet collected at " + str(created_at))
                print("Tweet coordinate is ", coordinates['coordinates'])
                location = datajson['place']['full_name'].split(",")
                state = location[1].replace(" ", "")
                city = location[0].replace(" ", "")
                datajson['state'] = state
                datajson['city'] = city
                db.usa_tweets_collection.insert(datajson)
        except Exception as e:
            print("on_data Exception: {0}".format(e))


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
streamer.filter(locations=[-127.48, 24.66, -59.05, 49.78])