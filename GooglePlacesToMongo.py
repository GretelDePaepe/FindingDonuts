#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 13:39:08 2017

@author: Gretel_MacAir
"""

# %% import libs

import urllib
import json
from pymongo import MongoClient
import datetime as dt

# %% Define functions


def get_coordinates(lat_min, lat_max, lon_min, lon_max):
    """
    Creates a list with all the coordinate pairs incrementing
    the min values by 0.01 until the max values are reached.
    """
    all_coord = []
    lat = lat_min - 0.01
    lon = lon_min - 0.01
    for d_lat in range(int((lat_max - lat_min) * 100)):
        lat += 0.01
        for d_lon in range(int((lon_max - lon_min) * 100)):
            lon += 0.01
            all_coord.append((lat, lon))
    return all_coord


def google_place(lat, log, radius, types, key):
    """
    The Google Places API Web Service allows you to query for place
    information on a variety of categories, such as: establishments,
    prominent points of interest, geographic locations, and more.
    https://developers.google.com/places/web-service/search
    """
    AUTH_KEY = key
    LOCATION = str(lat) + "," + str(log)
    RADIUS = radius
    TYPES = types
    MyUrl = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json'
             '?location=%s'
             '&radius=%s'
             '&types=%s'
             '&key=%s') % (LOCATION, RADIUS, TYPES, AUTH_KEY)
    # print MyUrl
    response = urllib.urlopen(MyUrl)
    jsonRaw = response.read()
    jsonData = json.loads(jsonRaw)
    return jsonData


def google_place_next(pagetoken, key):
    """
    The Google Places API web Service returns 20 results and if there are
    more a next_page token.  The max is 60 results, 20 at a time.  This
    function looks for the next page token and if it exist, gets the next
    20 results twice.
    """
    AUTH_KEY = key
    PAGE_TOKEN = pagetoken
    MyUrl = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json'
             '?pagetoken=%s'
             '&key=%s') % (PAGE_TOKEN, AUTH_KEY)
    # print MyUrl
    response = urllib.urlopen(MyUrl)
    jsonRaw = response.read()
    jsonData = json.loads(jsonRaw)
    return jsonData


def get_cats(path):
    """
    Loads a list of categories we will search for, from a file
    """
    f = path
    o = open(f, 'r')
    f = o.read()
    cats = f.split(',\n')
    return cats


def connect_mongo(database):
    """
    Creates a connection to a mongo database
    """
    client = MongoClient()
    db = eval('client.%s' % database)
    return db


def store_in_mongo(db, collection, google_data, tc):
    """
    Loops through the results returned by the Google Places API Web Service
    and adds each doc to a mongo collection.
    Adds a date tag to the doc.
    """
    docs = google_data['results']
    for i in range(len(docs)):
        doc = docs[i]
        print doc['name']
        doc['date'] = tc
        eval("db.%s.insert_one(doc)" % collection)


def create_daily_count(db, collection, tc, path):
    """
    Connect to a mongo database and counts the docs in a collection
    with a specific date tag and stores the date and count in a csv file.
    """
    con = "db.%s.find({'date': '%s'}).count()" % (collection, tc)
    count = eval(con)
    f = open(path, 'a')
    f.write(tc + "," + str(count) + "\n")
    f.close()
    return count

# %%


def main():

    start = dt.datetime.now()

    db = connect_mongo('FindingDonuts')
    collection = 'SeattleGoogle'

    key = "TYPE YOUR OWN"
    radius = 5000

    t = dt.datetime.now()
    tc = t.strftime('%m/%d/%Y')

    cats = get_cats('/home/ec2-user/pythonscripts/GoogleTypes.csv')
    all_coord = get_coordinates(47.48, 47.75, -122.43, -122.20)

    for cat in cats:
        print cat
        for lat, lon in all_coord:
            print lat, lon
            google_data = google_place(lat, lon, radius, cat, key)
            store_in_mongo(db, collection, google_data, tc)
            for j in range(2):
                try:
                    pagetoken = google_data['next_page_token']
                except:
                    pagetoken = "No more"
                if pagetoken != "No more":
                    google_data = google_place_next(pagetoken, key)
                    store_in_mongo(db, collection, google_data, tc)

    end = dt.datetime.now()

    file_dc = 'GoogleCountPerDay.csv'
    path_dc = '/home/ec2-user/pythonscripts/' + file_dc
    create_daily_count(db, collection, tc, path_dc)

    print start, end

# %%

if __name__ == "__main__":
    main()
