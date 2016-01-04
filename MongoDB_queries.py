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

   # Auditing the data 
   # Checking for City names  
    pipeline_city_names = [{"$match" : {"address.city" : {"$exists" : 1}}},
                           {"$group" : {"_id" : "$address.city", "count" : {'$sum' : 1}}},
                            {"$sort" : {"count" : 1}}]
    pipeline_amenties = [{"$match" : {"amenity" : {"$exists" : 1}}},
                           {"$group" : {"_id" : "$amenity", "count" : {'$sum' : 1}}},
                            {"$sort" : {"count" : 1}}]
    return pipeline_amenties
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------    
def city_name():
     city_re = re.compile (r'Aix en Provence$', re.IGNORECASE)
     db = get_db('DataWrangeling')
     match = db.Aix_en_Provence.update_many(
     {"address.city" : { '$regex' : city_re}},
     {'$set' :
     {"address.city" : 'Aix-en-Provence'}})
     print match

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def upadate_cedex():
    db = get_db('DataWrangeling')
    cedex_re = re.compile (r'Cedex', re.IGNORECASE)
    cedex = db.Aix_en_Provence.find({"address.city" : { '$regex' : cedex_re}})
    print cedex.count()
    for r in cedex:
         r["address"]["city"] = cedex_re.sub("CEDEX",r["address"]["city"]) 
         print r["address"]["city"]
         db.Aix_en_Provence.update({'_id': r['_id']},r);
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def update_amenities():
     db = get_db('DataWrangeling')
     amenities_re = re.compile (r'_', re.IGNORECASE)
     amenity = db.Aix_en_Provence.find({"amenity" : { '$regex' : amenities_re}}) 
     
     print amenity
     for r in amenity:
         print r["amenity"]
         r["amenity"] = amenities_re.sub(" ",r["amenity"]) 
         db.Aix_en_Provence.update({'_id': r['_id']},r);
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
if __name__ == '__main__':
   
    #list = defaultdict(set)
    #street_type_re = re.compile(r'^\b\S+\.?', re.IGNORECASE)
    #city_name()
    #upadate_cedex()
    update_amenities()
    array = aggregate(db, make_pipeline())
    pprint.pprint(array)
    #update()
   

  
    
    
    
                                   
                                   