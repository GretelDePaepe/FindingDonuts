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

# %% The functions


def create_coordinates_list(lat_min, lat_max, lon_min, lon_max):
    """
    Creates a list with all the coordinate pairs incrementing
    the min values by 0.01 until the max values are reached.
    """
    increment = 0.01
    all_coordinates = []
    lat = lat_min - increment
    lon = lon_min - increment
    for no_of_lats in range(int((lat_max - lat_min) / increment)):
        lat += increment
        lon = lon_min - increment  # lon needs to be reset before 2nd loop!
        for no_of_lons in range(int((lon_max - lon_min) / increment)):
            lon += increment
            all_coordinates.append((round(lat, 2), round(lon, 2)))
    return all_coordinates


def google_places(lat, lon, radius, types, key):
    """
    The Google Places API Web Service allows you to query for place
    information on a variety of categories, such as: establishments,
    prominent points of interest, geographic locations, and more.
    https://developers.google.com/places/web-service/search
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
    """
    my_url = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json'
              '?pagetoken=%s'
              '&key=%s') % (page_token, key)
    response = urllib.urlopen(my_url)
    json_raw = response.read()
    json_data = json.loads(json_raw)
    return json_data


def get_cats(path):
    """
    Loads a list of categories we will search for, from a file
    """
    file_path = path
    file_open = open(file_path, 'r')
    file_read = file_open.read()
    cats = file_read.split(',\n')
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
    for no_of_docs in range(len(docs)):
        doc = docs[no_of_docs]
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

# %% The program


def main():

    start = dt.datetime.now()

    db = connect_mongo('FindingDonuts')
    collection = 'SeattleGoogle'

    key = "TYPE YOUR OWN"
    radius = 5000

    t = dt.datetime.now()
    tc = t.strftime('%m/%d/%Y')

    cats = get_cats('/home/ec2-user/pythonscripts/GoogleTypes.csv')
    all_coord = create_coordinates_list(47.48, 47.75, -122.43, -122.20)

    for cat in cats:
        print cat
        for lat, lon in all_coord:
            print lat, lon
            google_data = google_places(lat, lon, radius, cat, key)
            store_in_mongo(db, collection, google_data, tc)
            for j in range(2):
                try:
                    pagetoken = google_data['next_page_token']
                except:
                    pagetoken = "No more"
                if pagetoken != "No more":
                    google_data = google_places_next(pagetoken, key)
                    store_in_mongo(db, collection, google_data, tc)

    end = dt.datetime.now()

    file_dc = 'GoogleCountPerDay.csv'
    path_dc = '/home/ec2-user/pythonscripts/' + file_dc
    create_daily_count(db, collection, tc, path_dc)

    print start, end

# %% Standard boilerplate to call the main() function

if __name__ == "__main__":
    main()
