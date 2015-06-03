#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Your task is to use the iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
Fill out the count_tags function. It should return a dictionary with the 
tag name as the key and number of times this tag can be encountered in 
the map as value.

Note that your code will be tested with a different data file than the 'example.osm'
"""
import xml.etree.ElementTree as ET
import pprint

def count_tags(filename):
        # YOUR CODE HERE
    tags = {}
    with open(filename, "r") as f:
        for line in f.readlines():
            
            lineArray = line.replace("<", "").replace(">", "").split(" ")
            
            arrayIndex = 0
            while lineArray[arrayIndex] == "":
                arrayIndex = arrayIndex + 1
            
            if "/" not in lineArray[arrayIndex] and "?" not in lineArray[arrayIndex]:
                if lineArray[arrayIndex] not in tags:
                    tags[lineArray[arrayIndex]] = 1
                else:
                    tags[lineArray[arrayIndex]] = tags[lineArray[arrayIndex]] + 1
            
            #print "(" + line + ")"

        #print tags
        return tags       

def test():

    tags = count_tags('example.osm')
    pprint.pprint(tags)
    assert tags == {'bounds': 1,
                     'member': 3,
                     'nd': 4,
                     'node': 20,
                     'osm': 1,
                     'relation': 1,
                     'tag': 7,
                     'way': 1}

    

if __name__ == "__main__":
    test()