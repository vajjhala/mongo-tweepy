from pymongo import MongoClient
from operator import itemgetter
import csv
import os

db = MongoClient().usa_db

if os.path.exists('data/usa_tweets.csv'):
    os.remove('data/usa_tweets.csv')
with open('data/usa_tweets.csv', 'w') as outfile:
    field_names = ['text', 'user', 'created_at', 'geo']
    writer = csv.DictWriter(outfile, delimiter=',', fieldnames=field_names)
    writer.writeheader()

    for data in db.usa_tweets_collection.find():
        writer.writerow({
            'text': data['text'],
            'user': data['user'],
            'created_at': data['created_at'],
            'geo': data['geo'],
        })

    outfile.close()
