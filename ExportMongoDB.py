# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 17:44:37 2016

@author: nnorit
"""
import json
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client.DataWrangeling
db.Aix_en_Provence.drop()

def insert_data(data, db):

# Your code here. Insert the data into a collection 'Aix_en_Provence'
    for element in data:
       db.Aix_en_Provence.insert(element)
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
if __name__ == '__main__':
    with open('DataWrangeling/Aix_en_Provence.osm.json') as f:
        data = json.load(f)
        print data
        insert_data(data, db)
  
