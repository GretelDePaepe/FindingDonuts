#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 19:39:57 2017

@author: Gretel_MacAir
"""
# %% Import libs

import shapefile
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import pandas as pd

# %% Read shapefile

path = "[PUT YOUR OWN]"
path = "/Users/Gretel_MacAir/Documents/Donuts/"
shape_file = "ZillowNeighborhoods-WA/ZillowNeighborhoods-WA.shp"
sf = shapefile.Reader(path + shape_file)

# %% Create list with Seattle shapes

counter = 0
seattle_areas = []
for rec in sf.iterRecords():
    if rec[2] == 'Seattle':
        neighboorhood = rec[3]
        seattle_areas.append((counter, neighboorhood))
    counter += 1

# %% Show plot with Seattle shapes

fig = plt.figure()
fig.suptitle("Seattle Polygons", fontsize=24, fontweight="bold")
ax = fig.add_subplot(132)
plt.xlim([-122.45, -122.22])
plt.ylim([47.48, 47.75])
ax.xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
ax.set_xlabel("longitude", fontsize='20')
ax.set_ylabel("latitute", fontsize='20')

for label in ax.xaxis.get_ticklabels():
    label.set_color("black")
    label.set_rotation(90)
    label.set_fontsize(12)
for label in ax.yaxis.get_ticklabels():
    label.set_color("black")
    label.set_rotation(0)
    label.set_fontsize(12)

counter = 0
for shape in sf.shapes():
    counter += 1
    if counter in [a[0] for a in seattle_areas]:
        points = shape.points
        ap = plt.Polygon(points, fill=False, edgecolor="#658dce")
        ax.add_patch(ap)

plt.show()

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

all_coordinates = create_coordinates_list(47.48, 47.75, -122.45, -122.22)

# %% Check which of the coordinate pairs are actually in Seattle

counter = 0
required_coordinates = []
for shape in sf.shapes():
    if counter in [a[0] for a in seattle_areas]:
        points = shape.points
        polygon = Polygon(points)
        for coordinate in all_coordinates:
            point = Point((coordinate[1], coordinate[0]))
            if polygon.contains(point):
                required_coordinates.append(coordinate)
    counter += 1

# %% Read in hipster places csv

hipster_file = "HipsterPlaces.csv"
hipster = pd.read_csv(path + hipster_file)
hipster["Coordinates"] = list(zip(hipster.Longitude, hipster.Latitude))

# %% Determine in which shape each hipster place is located


def get_neighborhood(coordinate):
    '''
    Takes a coordinate tuple in the form of (lon,lat)
    Loops through all the polygons in the shapefile
    Returns the name of the neighborhood if the coordinate is in the polygon
    '''
    point = Point(coordinate)
    counter = 0
    result = 'Not Found'
    for rec in sf.iterRecords():
        if rec[2] == 'Seattle':
            neighboorhood = rec[3]
            shape = sf.shapes()[counter]
            points = shape.points
            polygon = Polygon(points)
            if polygon.contains(point):
                result = neighboorhood
        counter += 1
    return result

hipster['Neighborhood'] = hipster['Coordinates'].apply(get_neighborhood)

# %% Plot Seattle shapes with hipster places

coordinates_list = list(hipster.Coordinates)

fig = plt.figure()
fig.suptitle("Seattle Hipster Polygons", fontsize=24, fontweight="bold")
ax = fig.add_subplot(132)
plt.xlim([-122.45, -122.22])
plt.ylim([47.48, 47.75])
ax.xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
ax.set_xlabel("longitude", fontsize='20')
ax.set_ylabel("latitute", fontsize='20')

for label in ax.xaxis.get_ticklabels():
    label.set_color("black")
    label.set_rotation(90)
    label.set_fontsize(12)
for label in ax.yaxis.get_ticklabels():
    label.set_color("black")
    label.set_rotation(0)
    label.set_fontsize(12)

counter = 0
for shape in sf.shapes():
    counter += 1
    if counter in [a[0] for a in seattle_areas]:
        points = shape.points
        ap = plt.Polygon(points, fill=False, edgecolor="#658dce")
        ax.add_patch(ap)
        for x, y in coordinates_list:
            plt.scatter(x, y, color="r")

plt.show()
