import csv
import pprint
import re
from ast import literal_eval
from collections import defaultdict, Counter

import emoji
import folium
from pymongo import MongoClient
from textblob import TextBlob

# this is where the magic happens

client = MongoClient('localhost', 27017)
db = client['twitterdb']
collection = db['twitter_search']


def map_func(x):
    if x > 0:
        return 'positive'
    elif x == 0:
        return 'neutral'
    else:
        return 'negative'


def extract_emojis(str):
    return [c for c in str if c in emoji.UNICODE_EMOJI]


# ---------------------------  Part 1 A --------------------------- #
regex = re.compile(".*data.*", re.IGNORECASE)
tweets_with_data = collection.find({"text": regex})
tweets_with_data_count = tweets_with_data.count()

print("\n \n **********************\n\n")
print("The number of tweets that have 'data' : ",
      tweets_with_data_count)

print("\n \n **********************\n\n")
# --------------------------- Part 1 B and C --------------------------- #
geo_enabled_count = 0
for tweet in tweets_with_data:
    if (tweet['user']['geo_enabled']):
        geo_enabled_count = geo_enabled_count + 1
    text = tweet['text']
    tweet_blob = TextBlob(text)
    print("{0} sentiment for the tweet: {1}"
          .format(map_func(tweet_blob.sentiment.polarity), text))
print("\n \n **********************\n\n")
print("Number of geo enabled tweets ", geo_enabled_count)

# --------------------------- Part 2 --------------------------- #

db2 = client['usa_db']
collection2 = db2['usa_tweets_collection']

emoticon_dict = defaultdict(int)
state_dict = defaultdict(list)
christmas_tree_list = []
christmas_tree_unicode = b'\\U0001f384'.decode('unicode-escape')

tweets_from_collection2 = collection2.find({})

for tweet in tweets_from_collection2:
    state = tweet['state']
    emoticon_list = extract_emojis(tweet['text'])
    for emoticon in emoticon_list:
        if emoticon != '':
            emoticon_dict[emoticon] = emoticon_dict[emoticon] + 1
            state_dict[state].append(emoticon)
            if emoticon == christmas_tree_unicode:
                christmas_tree_list.append(state)

states_emoticons = {key: len(value) for key, value in state_dict.items()}

print("\n \n **********************\n\n")
print("Top 15 emojis\n", Counter(emoticon_dict).most_common(15))

print("\n \n **********************\n\n")
print("Top 5 states for {0}\n".format(christmas_tree_unicode),
      Counter(christmas_tree_list).most_common(5))

print("\n \n **********************\n\n")
print("Top 5 emojis of MA\n", Counter(state_dict['MA']).most_common(5))

print("\n \n **********************\n\n")
print("Top 5 states that use emojis\n", Counter(states_emoticons).most_common(5))

# --------------------------- PyMongo --------------------------- #

pipeline1 = [{"$group": {"_id": "$state", "totalTweets": {"$sum": 1}}},
             {"$sort": {"totalTweets": -1}},
             {"$limit": 5}]

print("\n \n **********************\n\n")
print("The top 5 states that have tweets")
pprint.pprint(list(collection2.aggregate(pipeline1)))

pipeline2 = [{"$match": {"state": "CA"}},
             {"$group": {"_id": "$city", "totalTweets": {"$sum": 1}}},
             {"$sort": {"totalTweets": -1}},
             {"$limit": 5}]

print("\n \n **********************\n\n")
print("The top 5 cities that tweet in California")
pprint.pprint(list(collection2.aggregate(pipeline2)))

# --------------------------- D  --------------------------- #


print("\n \n **********************\n\n")
m = folium.Map()

with open('data/usa_tweets.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    total = sum(1 for _ in reader)
    csvfile.seek(0)
    location_list = []
    reader = csv.DictReader(csvfile)
    completed_progress = []
    for index, row in enumerate(reader):
        name_dict = literal_eval(row['user'])
        location_dict = literal_eval(row['geo'])
        location_list.append(location_dict['coordinates'])
        folium.CircleMarker(location_dict['coordinates'],
                            radius=5,
                            popup='None',
                            color='#3186cc',
                            fill_color='#3186cc').add_to(m)

        progress = int((index * 100) / total)
        if (progress % 1 == 0) and (progress not in completed_progress):
            completed_progress.append(progress)
            print("Completed mapping....{0}%".format(progress))

m.save('output/map.html')

# --------------------------- The End  --------------------------- #