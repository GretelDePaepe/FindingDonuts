#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 07:20:08 2017

@author: Gretel_MacAir
"""
# %% import libs

import pandas as pd
import datetime as dt
import DonutLibrary as dl

# %% prep the counts of the Foursquare places and reviews downloaded today

path = dl.get_path('FindingDonuts')

cpd = pd.read_csv(path + 'FourCountPerDay.csv',
                  header=None, names=['Date', 'Count'])

reviews_cpd = pd.read_csv(path + 'FourReviewsCountPerDay.csv',
                          header=None, names=['Date', 'Count'])

start = dt.datetime.now()
tc = start.strftime('%m/%d/%Y')

count_today = cpd[cpd['Date'] == tc].set_index('Date')
count_reviews_today = reviews_cpd[reviews_cpd['Date'] == tc].set_index('Date')

# %% create subject and body of mail

subject = "Foursquare to Mongo"
body = ("Number of places downloaded: " +
        "\n" +
        "%s" +
        "\n" +
        "Number of reviews downloaded:  " +
        "\n" +
        "%s") % (count_today,
                 count_reviews_today)

# %% send mail

dl.send_mail_general(subject, body, path)
