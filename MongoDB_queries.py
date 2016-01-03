# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 20:26:46 2016

@author: nnorit
"""
import pprint 
import re
from collections import defaultdict


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db
#------------------------------------------------------------------------------    
#------------------------------------------------------------------------------   
def aggregate(db, pipeline):
    return [doc for doc in db.Aix_en_Provence.aggregate(pipeline)]
    
# Diffrent Pipelines for the queries with MongoDB
#------------------------------------------------------------------------------   
#------------------------------------------------------------------------------
def make_pipeline():
    # complete the aggregation pipeline
    pipeline1 = [{'$match' : {'amenity' : {'$exists' : 1}}},{'$group' : {'_id' :'$amenity', 'count' : {'$sum' : 1}}},
                {'$sort' : {'count' :-1}},
                {'$limit' : 30}]
    pipeline2 = [{'$match' : {'amenity' : 'telephone'}},
                {'$limit' : 7}]
    pipeline3 = [{"$match":{"address.postcode":{"$exists":1}}}, {"$group":{"_id":"$address.postcode", "count":{"$sum":1}}}, {"$sort":{'count' :-1}}]
    pipeline4 = [{'$match' : {'address.postcode' : 13626}},
                {'$limit' : 7}]
    pipeline5 = [{"$match":{"address.street":{"$exists":1}}}, {"$group":{"_id":"$address.street", "count":{"$sum":1}}}, {"$sort":{'count' :-1}}]
    pipeline6 = [{"$group":{"_id":"$created.uid", "count":{"$sum":1}}}, {'$sort' : {'count' :-1}}, {"$limit":1}]
    pipeline7 = [{"$match":{"amenity":{"$exists":1}, "amenity":"restaurant", "cuisine" : {"$exists":1}}}, {"$group":{"_id":"$cuisine", "count":{"$sum":1}}},{'$sort' : {'count' :-1}}, {"$limit":3}]
    return pipeline7
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------    
if __name__ == '__main__':
    db = get_db('DataWrangeling')
    list = defaultdict(set)
    street_type_re = re.compile(r'^\b\S+\.?', re.IGNORECASE)
    array = aggregate(db, make_pipeline())
    print array

  
    
    
    
                                   
                                   