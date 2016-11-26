# -*- coding: utf-8 -*-
"""
Created on Sat Sep 17 17:07:38 2016
@author: Gretel_MacAir
"""

# %% Import required libs

import urllib
import urllib2
import oauth2
import json
import datetime as dt
from pymongo import MongoClient

# %% Some stuff needed by the Yelp API

API_HOST = 'api.yelp.com'
SEARCH_PATH = '/v2/search/'
CONSUMER_KEY = "***"
CONSUMER_SECRET = "***"
TOKEN = "***"
TOKEN_SECRET = "***"

# %% Set op a connection to the mongodb

client = MongoClient()
db = client.FindingDonuts

# %% To authenticate the Yelp API and fire off searches
# Copied from https://www.yelp.com/developers/documentation/v2/examples
# Amended to suit this project


def request(host, path, url_params=None):
    """Prepares OAuth authentication and sends the request to the API.
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
    consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url,
                                   parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': CONSUMER_KEY
        }
    )
    token = oauth2.Token(TOKEN, TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(),
                               consumer, token)
    signed_url = oauth_request.to_url()
    print u'Querying {0} ...'.format(url)
    conn = urllib2.urlopen(signed_url, None)
    print signed_url
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()
    return response


def search(term, location, limit, offset):
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
    print url_params
    return request(API_HOST, SEARCH_PATH, url_params=url_params)

# %% Define the stuff we will search for

f = 'YelpCats.csv'
o = open(f, 'r')
f = o.read()
Cats = f.split(',\n')

# %% Set some variables

city = "Seattle"
max = 24000
counter = 0
t = dt.datetime.now()
tc = t.strftime('%m/%d/%Y')

# %% Get the Yelp data and store it in MongoDB

for cat in Cats:
    for i in range(40):
        if counter < max:
            try:
                counter += 1
                yelpdata = search(cat, city, 1, i)
                doc = yelpdata['businesses'][0]
                doc['date'] = tc
                db.Seattle.insert_one(doc)
            except:
                pass
        else:
            break

# %% check and create summary record in csv file

count = db.Seattle.find({'date': tc}).count()
f = open('YelpCountPerDay.csv', 'a')
f.write(tc + "," + str(count) + "\n")
f.close()
