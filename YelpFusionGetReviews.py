#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 12:53:29 2016

@author: Gretel_MacAir
"""

import requests
import urllib
from pymongo import MongoClient
import datetime as dt

# %% Get yesterday's businesses

client = MongoClient()
db = client.FindingDonuts

t = dt.datetime.now() - dt.timedelta(days=1)
tc = t.strftime('%m/%d/%Y')

if tc.split("/")[1] == '31':
    t = dt.datetime.now() - dt.timedelta(days=2)
    tc = t.strftime('%m/%d/%Y')
#print tc 

bus = db.Seattle.distinct("id", {"$and": [{'date': tc},
                                          {'review_count': {"$gt": 10}}]})
bus_sub = bus[0:8000] # max quota per day is 25,000
print "Number of businesses", len(bus_sub)

# %% Get 3 reviews from Yelp for each of these businesses and store in mongodb

CLIENT_ID = "Put Your Own"
CLIENT_SECRET = "Put Your Own"

API_HOST = 'api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'
TOKEN_PATH = '/oauth2/token'  # No trailing slash.
GRANT_TYPE = 'client_credentials'

def obtain_bearer_token(host, path):
    url = 'https://{0}{1}'.format(host, urllib.quote(path.encode('utf8')))
    assert CLIENT_ID, "Please supply your client_id."
    assert CLIENT_SECRET, "Please supply your client_secret."
    data = urllib.urlencode({
        'client_id' : CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type'  : GRANT_TYPE,
    })
    headers = {
        'content-type': 'application/x-www-form-urlencoded',
    }
    response = requests.request('POST', url, data=data, headers=headers)
    bearer_token = response.json()['access_token']
    return bearer_token

def request(host, path, bearer_token, url_params=None):
    """Given a bearer token, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        bearer_token (str): OAuth bearer token, obtained using client_id and client_secret.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        urllib2.HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = 'https://{0}{1}'.format(host, urllib.quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % bearer_token,
    }

    # print u'Querying {0} ...'.format(url)

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()

def get_reviews(business_id):
    """Query the Reviews API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    review_path = BUSINESS_PATH + business_id + '/reviews'
    bearer_token = obtain_bearer_token(API_HOST, TOKEN_PATH)
    return request(API_HOST, review_path, bearer_token)

tn = dt.datetime.now()
tnc = tn.strftime('%m/%d/%Y')
#print tnc

for b in bus_sub:
    try:
        response = get_reviews(b)
        for r in range(3):
            doc = response['reviews'][r]
            doc['business_id'] = b
            doc['date'] = tnc
            db.SeattleReviews.insert_one(doc)
    except:
        pass

# %% check and create summary record in csv file

count = db.SeattleReviews.find({'date': tnc}).count()
f = open('/home/ec2-user/pythonscripts/YelpCountPerDay.csv', 'a')
f.write(tnc + "," + str(count) + "\n")
f.close()

print "Finished!!!"
