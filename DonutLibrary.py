#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 16:46:35 2017

@author: Gretel_MacAir
"""

import pickle
import sys
from pymongo import MongoClient
import smtplib

# %% Define functions


def get_path(sub):
    """
    Gets the path of where the required info is stored or needs to be stored.
    Since the path is different based on whether I am working on my laptop
    while developing or when the script is running from my EC2, the function
    tests from which platform it is called.
    sub = name of the subfolder (on EC2 all is stored in the same dir)
    """
    if sys.platform == 'darwin':
        home = "/Users/Gretel_MacAir/"
        path = home + "Documents/Donuts/%s/" % (sub)
    else:
        path = '/home/ec2-user/pythonscripts/'
    return path


def get_api_keys(pickle_file, path):
    """
    Reads the key and/or id and secret needed to authenticate
    pickle_file = name of the pickled file which contains the client api key
    path : location of the pickled file
    """
    api_key_pickle = "FourAPIKey.p"
    api_key = pickle.load(open(path+api_key_pickle, "rb"))
    return api_key


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


def get_bookmark(bookmark_pickle_file, path):
    """
    Reads which lat and lon coordinate we need to proceed from based on where
    the script stopped last time it ran
    """
    bookmark = pickle.load(open(path + bookmark_pickle_file, "rb"))
    return bookmark


def store_bookmark(bookmark_pickle_file, path, bookmark):
    """
    Pickles the lat lon coordinate pair we need to start from next
    time the script runs
    """
    pickle.dump(bookmark, open(path + bookmark_pickle_file, "wb"))
    return None


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


def send_mail(subject, count, start, end):
    """
    Sends mail with how long the script ran and how many records were stored
    in the mongo db
    """
    text = ("Number of records downloaded: %d. " +
            "Script ran from %s till %s.") % (count,
                                              str(start),
                                              str(end))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login("findingdonuts@gmail.com", "fomojomo")
    message = 'Subject: {}\n\n{}'.format(subject, text)
    server.sendmail("findingdonuts@gmail.com",
                    "gretel.paepe@gmail.com",
                    message)
