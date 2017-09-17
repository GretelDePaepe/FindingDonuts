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
    ll = "%.3f,%.3f" % (lat, lon)
    client = foursquare.Foursquare(client_id=client_id,
                                   client_secret=client_secret)
    response = client.venues.explore(params={'ll': ll,
                                             'radius': radius,
                                             'categoryId': category,
                                             'limit': limit})
    return response


def four_reviews(venue_id, client_id, client_secret):
    """
    The Foursquare API returns a list of tips (reviews) per venue.
    https://developer.foursquare.com/docs/tips/search
    venue_id: id of venue
    client_id = Client_id linked to app.
    client_secret = Client_secret linked to app.
    """
    client = foursquare.Foursquare(client_id=client_id,
                                   client_secret=client_secret)
    response = client.venues.tips(venue_id)
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
    venues_list = []
    for no_of_docs in range(len(docs)):
        doc = docs[no_of_docs]
        doc['date'] = tc
        doc['hood'] = hood
        venue = doc['venue']['id']
        venues_list.append(venue)
        con = "db.%s.find({'venue_id': '%s', 'date': '%s'}).count()" % (collection, venue, tc)
        count = eval(con)
        if count == 0:
            eval("db.%s.insert_one(doc)" % collection)
    return venues_list


def store_four_reviews_in_mongo(db, collection, reviews, tc, venue_id):
    """
    Loops through the results returned by the details Google Places API
    and adds each review to a mongo collection.
    Adds a date tag to the doc.
    db = mongo database connection
    collection = name of the mongo collection
    google_data = data returned from the Google Places API
    tc = today's date
    venue_id = a textual identifier that uniquely identifies a place,
               returned from a Place Search.
    """
    number_of_reviews = len(reviews['tips']['items'])
    for i in range(number_of_reviews):
        review = reviews['tips']['items'][i]
        review['date'] = tc
        review['venue_id'] = venue_id
        eval("db.%s.insert_one(review)" % collection)

# %%


def main():

    start = dt.datetime.now().strftime('%H:%M')
    number_of_requests = 0
    """
    After validating your account with Foursquare the max number of requests
    is 100,000 in a 24 hour window.  This does not get rid of the 5,000
    requests per hour though it seems.  Hence this script will be scheduled to
    run every 2 hours (buffer).  I am setting the max to 4,000 since I like to
    have some buffer in case I need to test something.
    """

    environment = dl.dev_test_prod()
    if environment == 'prod':
        max_number_of_requests = 4000
    else:
        max_number_of_requests = 10
        print "Only testing"
    path = dl.get_path('FindingDonuts')

    bookmark_pickle_file = "bookmark_four.p"
    bookmark = dl.get_bookmark(bookmark_pickle_file, path)
    bookmark = 8562

    db = dl.connect_mongo('FindingDonuts')

    client_id = dl.get_api_keys('FourAPIKey.p', path)[0]
    print client_id
    client_secret = dl.get_api_keys('FourAPIKey.p', path)[1]
    print client_secret

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
        print hood, lat, lon
        for one_type in types:
            four_data = four_places(lat, lon, radius,
                                    one_type, limit,
                                    client_id, client_secret)
            venues = store_four_in_mongo(db,
                                         'SeattleFour',
                                         four_data,
                                         tc,
                                         hood)
            number_of_requests += 1
            # In this case we get the reviews immediately for each venue
            for venue in venues:
                four_reviews_data = four_reviews(venue,
                                                 client_id,
                                                 client_secret)
                store_four_reviews_in_mongo(db,
                                            'SeattleFourReviews',
                                            four_reviews_data,
                                            tc,
                                            venue)
                number_of_requests += 1
        if bookmark < 27337:
            bookmark += 1
        else:
            bookmark = 0

    pickle.dump(bookmark, open(path + bookmark_pickle_file, "wb"))

    end = dt.datetime.now().strftime('%H:%M')

    dc_places_file = 'FourCountPerDay.csv'
    dc_reviews_file = 'FourReviewsCountPerDay.csv'
    four_count = dl.create_daily_count_start_end(db,
                                                 'SeattleFour',
                                                 tc,
                                                 start,
                                                 end,
                                                 path+dc_places_file)

    four_reviews_count = dl.create_daily_count_start_end(db,
                                                         'SeattleFourReviews',
                                                         tc,
                                                         start,
                                                         end,
                                                         path+dc_reviews_file)
    if environment != 'prod':
        print "Number of places", four_count
        print "Number of reviews", four_reviews_count

# %% Standard boilerplate to call the main() function


if __name__ == "__main__":
    main()
