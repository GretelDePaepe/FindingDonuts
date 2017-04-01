#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 13:39:08 2017
@author: GDP
"""

# %% The libs

import urllib
import json
from pymongo import MongoClient
import datetime as dt
import pickle

# %% The functions


def google_places(lat, lon, radius, types, key):
    """
    The Google Places API Web Service allows you to query for place
    information on a variety of categories, such as: establishments,
    prominent points of interest, geographic locations, and more.
    https://developers.google.com/places/web-service/search
    lat = latitude in decimal form eg. 47.573
    lon = longitude in decimal form eg. -122.284
    radius = Defines the distance (in meters) within which to return place
             results. The maximum allowed radius is 50 000 meters.
    types = Restricts the results to places matching the specified type,
            eg. 'restaurant', 'bar', etc.
    key = user's API key
    """
    location = str(lat) + "," + str(lon)
    my_url = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json'
              '?location=%s'
              '&radius=%s'
              '&types=%s'
              '&key=%s') % (location, radius, types, key)
    response = urllib.urlopen(my_url)
    json_raw = response.read()
    json_data = json.loads(json_raw)
    return json_data


def google_places_next(page_token, key):
    """
    The Google Places API web Service returns 20 results and if there are
    more a next_page token.  The max is 60 results, 20 at a time.  This
    function looks for the next page token and if it exist, gets the next
    20 results twice.
    page_token = key returned by Google Places API in case there are more
                 results which can be requested.
    key = user's API key
    """
    my_url = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json'
              '?pagetoken=%s'
              '&key=%s') % (page_token, key)
    response = urllib.urlopen(my_url)
    json_raw = response.read()
    json_data = json.loads(json_raw)
    return json_data


def get_types(types_path_file):
    """
    Loads a list of categories we will search for, from a file
    types_path_file = path and file name of the file which contains the types
                      we are going to search for, eg. 'restaurant', 'bar', etc.
    """
    file_path = types_path_file
    file_open = open(file_path, 'r')
    file_read = file_open.read()
    cats = file_read.split(',\n')
    return cats


def connect_mongo(database):
    """
    Creates a connection to a mongo database
    database = name of the database
    """
    client = MongoClient()
    db = eval('client.%s' % database)
    return db


def store_in_mongo(db, collection, google_data, tc, hood):
    """
    Loops through the results returned by the Google Places API Web Service
    and adds each doc to a mongo collection.
    Adds a date tag to the doc.
    db = mongo database connection
    collection = name of the mongo collection
    google_data = data returned from the Google Places API
    tc = today's date
    hood = neighborhood
    """
    docs = google_data['results']
    for no_of_docs in range(len(docs)):
        doc = docs[no_of_docs]
        doc['date'] = tc
        doc['hood'] = hood
        place = doc['place_id']
        con = "db.%s.find({'place_id': '%s', 'date': '%s'}).count()" % (collection, place, tc)
        count = eval(con)
        if count == 0:
            eval("db.%s.insert_one(doc)" % collection)


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
    number_of_requests = 0
    max_number_of_requests = 140000  # max number of requests in 24 hours is 150,000

    path = '/home/ec2-user/pythonscripts/'

    bookmark_pickle_file = "bookmark.p"
    bookmark = pickle.load(open(path + bookmark_pickle_file, "rb"))

    db = connect_mongo('FindingDonuts')
    collection = 'SeattleGoogle'

    key = "AIzaSyCdVApz2Qw50B62TKOOCQkH1Y3dAJM7A5M"
    radius = 100

    t = dt.datetime.now()
    tc = t.strftime('%m/%d/%Y')

    coordinates_pickle_file = "neighborhood_coordinates.p"
    seattle_coordinates = pickle.load(open(path+coordinates_pickle_file, "rb"))

    types_file = 'GoogleTypes.csv'
    types = get_types(path + types_file)

    while number_of_requests < max_number_of_requests:
        hood, lat, lon = seattle_coordinates[bookmark]
        print "--", bookmark, hood, lat, lon, number_of_requests
        for one_type in types:
            google_data = google_places(lat, lon, radius, one_type, key)
            store_in_mongo(db, collection, google_data, tc, hood)
            number_of_requests += 1
            for j in range(2):
                try:
                    pagetoken = google_data['next_page_token']
                    number_of_requests += 1
                except:
                    pagetoken = "No more"
                if pagetoken != "No more":
                    google_data = google_places_next(pagetoken, key)
                    store_in_mongo(db, collection, google_data, tc, hood)
                    number_of_requests += 1
        if bookmark < 27337:
            bookmark += 1
        else:
            bookmark = 0

    pickle.dump(bookmark, open(path + bookmark_pickle_file, "wb"))

    dc_file = 'GoogleCountPerDay.csv'
    create_daily_count(db, collection, tc, path + dc_file)

    end = dt.datetime.now()
    print start, end

# %% Standard boilerplate to call the main() function

if __name__ == "__main__":
    main()
