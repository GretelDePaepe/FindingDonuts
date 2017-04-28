#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 15:53:10 2017

@author: Gretel
"""

# %% Import required libs

import urllib
import urllib2
import oauth2
import json
import datetime as dt
from pymongo import MongoClient
import pickle
import sys

# %%


def request(host, path, key, key_secret, token, token_secret, url_params=None):
    """
    Prepares OAuth authentication and sends the request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        urllib2.HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(host, urllib.quote(path.encode('utf8')))
    consumer = oauth2.Consumer(key, key_secret)
    oauth_request = oauth2.Request(method="GET", url=url,
                                   parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': token,
            'oauth_consumer_key': key
        }
    )
    token = oauth2.Token(token, token_secret)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(),
                               consumer, token)
    signed_url = oauth_request.to_url()
    conn = urllib2.urlopen(signed_url, None)
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()
    return response


def search(term, location, limit, offset,
           api_host, api_path, key,
           key_secret, token, token_secret):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': limit,
        'offset': offset,
        'sort': 2
    }
    return request(api_host, api_path,
                   key, key_secret, token, token_secret,
                   url_params=url_params)


def connect_mongo(database):
    """
    Creates a connection to a mongo database
    database = name of the database
    """
    client = MongoClient()
    db = eval('client.%s' % database)
    return db


def store_in_mongo(db, collection, yelpdata, tc):
    """
    Adds result returned by the Yelp API as a doc to a mongo collection.
    Adds a date tag to the doc.
    db = mongo database connection
    collection = name of the mongo collection
    yelp_data = data returned from the Yelp Places API
    tc = today's date
    """
    doc = yelpdata['businesses'][0]
    doc['date'] = tc
    place = doc['id']
    con = "db.%s.find({'id': '%s', 'date': '%s'}).count()" % (collection,
                                                              place,
                                                              tc)
    count = eval(con)
    if count == 0:
        eval("db.%s.insert_one(doc)" % collection)


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

    api_host = "api.yelp.com"
    api_path = "/v2/search/"

    if sys.platform == 'darwin':
        home = "/Users/Gretel_MacAir/"
        path = home + "Documents/NewGitHub/FindingDonuts/YelpPlacesFiles/"
    else:
        path = "/home/ec2-user/pythonscripts/"

    yelp_keys_pickle = "yelp_keys.p"
    api_keys = pickle.load(open(path+yelp_keys_pickle, "rb"))
    key = api_keys[0]
    key_secret = api_keys[1]
    token = api_keys[2]
    token_secret = api_keys[3]

    client = MongoClient()
    db = client.FindingDonuts
    collection = "Seattle"

    cat_file = "YelpCats.csv"
    types_path_file = path + cat_file
    cats = get_types(types_path_file)
    city = "Seattle"
    max = 24000
    counter = 0
    t = dt.datetime.now()
    tc = t.strftime("%m/%d/%Y")

    for cat in cats:
        for i in range(40):
            if counter < max:
                try:
                    counter += 1
                    yelp_data = search(cat, city, 1, i,
                                       api_host, api_path, key,
                                       key_secret, token, token_secret)
                    store_in_mongo(db, collection, yelp_data, tc)
                except:
                    pass
            else:
                break

    dc_file = "YelpCountPerDay.csv"
    create_daily_count(db, collection, tc, path + dc_file)

    end = dt.datetime.now()
    print start, end

# %% Standard boilerplate to call the main() function

if __name__ == "__main__":
    main()
