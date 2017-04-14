#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 20:30:17 2017

@author: GDP
"""

import urllib
from pymongo import MongoClient
import datetime as dt
import json
import pickle
import sys

# %% The functions


def google_reviews(place_id, key):
    """
    The Google Places API Web Service allows you to query for reviews of places
    https://developers.google.com/places/web-service/details
    place_id = a textual identifier that uniquely identifies a place,
               returned from a Place Search.
    key = user's API key
    """
    my_url = ('https://maps.googleapis.com/maps/api/place/details/json'
              '?placeid=%s'
              '&key=%s') % (place_id, key)
    response = urllib.urlopen(my_url)
    json_raw = response.read()
    json_data = json.loads(json_raw)
    return json_data


def connect_mongo(database):
    """
    Creates a connection to a mongo database
    database = name of the database
    """
    client = MongoClient()
    db = eval('client.%s' % database)
    return db


def store_in_mongo(db, collection, google_data, tc, place_id):
    """
    Loops through the results returned by the details Google Places API
    and adds each review to a mongo collection.
    Adds a date tag to the doc.
    db = mongo database connection
    collection = name of the mongo collection
    google_data = data returned from the Google Places API
    tc = today's date
    place_id = a textual identifier that uniquely identifies a place,
               returned from a Place Search.
    """
    result = google_data['result']
    reviews = result['reviews']
    for no_of_reviews in range(len(reviews)):
        review = reviews[no_of_reviews]
        review['date'] = tc
        review['place_id'] = place_id
        eval("db.%s.insert_one(review)" % collection)


def create_daily_count(db, collection, tc, path_file):
    """
    Connect to a mongo database and counts the docs in a collection
    with a specific date tag and stores the date and count in a csv file.
    db = mongo database connection
    collection = name of the mongo collection
    tc = today's date
    path_file = path and the file to save to
    """
    con = "db.%s.find({'date': '%s'}).count()" % (collection, tc)
    count = eval(con)
    f = open(path_file, 'a')
    f.write(tc + "," + str(count) + "\n")
    f.close()
    return count

# %% The program


def main():

    start = dt.datetime.now()
    client = MongoClient()
    db = client.FindingDonuts
    collection = 'SeattleGoogleReviews'

    t = dt.datetime.now() - dt.timedelta(days=1)
    tc = t.strftime('%m/%d/%Y')
    tn = dt.datetime.now()
    tnc = tn.strftime('%m/%d/%Y')

    if tc.split("/")[1] == '31':
        t = dt.datetime.now() - dt.timedelta(days=2)
        tc = t.strftime('%m/%d/%Y')

    places = db.SeattleGoogle.distinct("place_id", {'date': tc})
    number_of_requests = 0
    max_number_of_requests = 10000  

    if sys.platform == 'darwin':
        home = "/Users/Gretel_MacAir/"
        path = home + "Documents/NewGitHub/FindingDonuts/GooglePlacesFiles/"
    else:
        path = '/home/ec2-user/pythonscripts/'
    api_key_pickle = "GoogleAPIKey.p"
    key = pickle.load(open(path+api_key_pickle, "rb"))

    while (number_of_requests < max_number_of_requests and
           number_of_requests < len(places)):
        place_id = places[number_of_requests]
        google_data = google_reviews(place_id, key)
        try:
            store_in_mongo(db, collection, google_data, tnc, place_id)
        except:
            pass
        number_of_requests += 1

    path_file = path + "GoogleCountPerDay.csv"
    create_daily_count(db, collection, tnc, path_file)

    end = dt.datetime.now()
    print start, end, number_of_requests

# %% Standard boilerplate to call the main() function

if __name__ == "__main__":
    main()
