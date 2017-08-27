#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 26 15:40:38 2017

@author: Gretel_MacAir
"""

# %% Import Libs

import urllib2
import string
import pickle
import DonutLibrary as dl
import smtplib

# %% Define function to mail results


def send_mail(subject, body):
    """
    Sends mail with the new words
    """
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login("findingdonuts@gmail.com", "fomojomo")
    message = 'Subject: {}\n\n{}'.format(subject, body)
    server.sendmail("findingdonuts@gmail.com",
                    "gretel.paepe@gmail.com",
                    message)

# %% Load words already pickled

path = dl.get_path('MillenialTalk')
millenial_pickle_file = 'Millenial.p'
millenial = pickle.load(open(path + millenial_pickle_file, "rb"))
new_words = []

# %% Scrape popular words by letter of the alphabet from Urban Dictionary

# New words are added to the list

for letter in string.ascii_uppercase:
    url = 'http://www.urbandictionary.com/popular.php?character=%s' % (letter)
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    temp1 = the_page.split("All popular words")[1].split("copyright")[0]
    temp2 = temp1.split('a href="/define.php?term=')
    for temp in temp2[1:]:
        urban_word = temp.split(">")[1].split("<")[0]
        urban_word_lower = urban_word.lower()
        if urban_word_lower not in millenial:
            new_words.append(urban_word)
            millenial.append(urban_word_lower)

# Pickle new enhanced list

pickle.dump(millenial, open(path + millenial_pickle_file, "wb"))

# %% Send mail

subject = 'New words from Urban Dictionary'
if len(new_words) > 0:
    body = ', '.join(new_words)
else:
    body = 'No new words'
send_mail(subject, body)
