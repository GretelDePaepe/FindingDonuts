#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 19:39:57 2017

@author: Gretel_MacAir
"""
# %% Import libs

import shapefile
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import pickle

# %% Read shapefile

path = "[PUT YOUR OWN]"
path = "/Users/Gretel_MacAir/Documents/Donuts/ZillowNeighborhoods-WA/"
shape_file = "ZillowNeighborhoods-WA.shp"
sf = shapefile.Reader(path + shape_file)

# %% Create list with Seattle shapes

counter = 0
seattle_areas = []
for rec in sf.iterRecords():
    if rec[2] == 'Seattle':
        neighboorhood = rec[3]
        seattle_areas.append((counter, neighboorhood))
    counter += 1

# %% Create coordinate pair list for rectangle slightly larger than Seattle


def create_coordinates_list(lat_min, lat_max, lon_min, lon_max):
    """
    Creates a list with all the coordinate pairs incrementing
    the min values by 0.01 until the max values are reached.
    """
    increment = 0.001
    all_coordinates = []
    lat = lat_min - increment
    lon = lon_min - increment
    for no_of_lats in range(int((lat_max - lat_min) / increment)):
        lat += increment
        lon = lon_min - increment  # lon needs to be reset before 2nd loop!
        for no_of_lons in range(int((lon_max - lon_min) / increment)):
            lon += increment
            all_coordinates.append((round(lat, 3), round(lon, 3)))
    return all_coordinates

all_coordinates = create_coordinates_list(47.480, 47.750, -122.450, -122.220)

# %% Check which coordinates are actually in Seattle and in which hood

counter = 0
seattle_coordinates = []
neighborhood_coordinates = {}

for shape in sf.shapes():
    if counter in [a[0] for a in seattle_areas]:
        position_in_list = [a[0] for a in seattle_areas].index(counter)
        neighborhood = seattle_areas[position_in_list][1]
        points = shape.points
        polygon = Polygon(points)
        for coordinate in all_coordinates:
            point = Point((coordinate[1], coordinate[0]))
            if polygon.contains(point):
                seattle_coordinates.append(coordinate)
                if neighborhood in neighborhood_coordinates:
                    neighborhood_coordinates[neighborhood].append(coordinate)
                else:
                    neighborhood_coordinates[neighborhood] = [coordinate]
    counter += 1

# %% Pickle the coordinates

pickle1 = open(path + "seattle_coordinates.p", "wb")
pickle2 = open(path + "neighborhood_coordinates.p", "wb")
pickle.dump(seattle_coordinates, pickle1)
pickle.dump(neighborhood_coordinates, pickle2)
