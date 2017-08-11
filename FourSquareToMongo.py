#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 07:24:59 2017

@author: Gretel_MacAir
"""
# %%

import foursquare
import DonutLibrary as dl
import datetime as dt
import pickle

# %%


def four_places(lat, lon, radius, category, limit, client_id, client_secret):
    """
    The Foursquare API returns a list of venues near the current location.
    https://developer.foursquare.com/docs/venues/search
    lat = latitude in decimal form eg. 47.573
    lon = longitude in decimal form eg. -122.284
    radius = Limit results to venues within this many meters of the
    specified location.
    limit = Number of results to return, up to 50.
    client_id = Client_id linked to app.
    client_secret = Client_secret linked to app.
    """
    ll = str(lat) + "," + str(lon)
    client = foursquare.Foursquare(client_id=client_id,
                                   client_secret=client_secret)
    response = client.venues.explore(params={'ll': ll,
                                             'radius': radius,
                                             'categoryId': category,
                                             'limit': limit})
    return response


def store_four_in_mongo(db, collection, four_data, tc, hood):
    """
    Loops through the results returned by the FourSquare API
    and adds each doc to a mongo collection.
    Adds a date tag to the doc.
    db = mongo database connection
    collection = name of the mongo collection
    four_data = data returned from the FourSquare API
    tc = today's date
    hood = neighborhood
    """
    docs = four_data['groups'][0]['items']
    for no_of_docs in range(len(docs)):
        doc = docs[no_of_docs]
        doc['date'] = tc
        doc['hood'] = hood
        place = doc['venue']['id']
        con = "db.%s.find({'place_id': '%s', 'date': '%s'}).count()" % (collection, place, tc)
        count = eval(con)
        if count == 0:
            eval("db.%s.insert_one(doc)" % collection)


# %%
def main():
    start = dt.datetime.now()
    number_of_requests = 0
    max_number_of_requests = 90000  # max number of requests in 24 h is 100,000
    path = dl.get_path('FourSquare')

    bookmark_pickle_file = "bookmark_four.p"
    bookmark = dl.get_bookmark(bookmark_pickle_file, path)
    dl.get_bookmark

    db = dl.connect_mongo('FindingDonuts')
    collection = 'SeattleFour'

    client_id = dl.get_api_keys('FourAPIKey.p', path)[0]
    client_secret = dl.get_api_keys('FourAPIKey.p', path)[1]

    radius = 100
    limit = 50

    t = dt.datetime.now()
    tc = t.strftime('%m/%d/%Y')

    coordinates_pickle_file = "neighborhood_coordinates.p"
    seattle_coordinates = pickle.load(open(path+coordinates_pickle_file, "rb"))

    types_file = 'FourCats.csv'
    types = dl.get_types(path + types_file)

    while number_of_requests < max_number_of_requests:
        hood, lat, lon = seattle_coordinates[bookmark]
        # print "--", bookmark, hood, lat, lon, number_of_requests
        for one_type in types:
            four_data = four_places(lat, lon, radius,
                                    one_type, limit,
                                    client_id, client_secret)
            store_four_in_mongo(db, collection, four_data, tc, hood)
            number_of_requests += 1
        if bookmark < 27337:
            bookmark += 1
        else:
            bookmark = 0

    pickle.dump(bookmark, open(path + bookmark_pickle_file, "wb"))

    dc_file = 'FourCountPerDay.csv'
    four_places_count = dl.create_daily_count(db,
                                              collection,
                                              tc,
                                              path + dc_file)

    end = dt.datetime.now()

    subject = "FourSquareToMongo"
    count = four_places_count

    dl.send_mail(subject, count, start, end)

# %% Standard boilerplate to call the main() function

if __name__ == "__main__":
    main()
