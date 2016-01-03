# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 21:36:37 2015

@author: nnorit
"""

#Start of the Project 
import xml.etree.cElementTree as ET
import pprint
import re
from collections import defaultdict
import codecs
import json



'''{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}'''
Dateiname = "DataWrangeling/Aix_en_Provence.osm"

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
#mapping dict to change the street types
mapping = { "rue": "Rue",
            "chemin": "Chemin",
            "avenue" : "Avenue"
            }

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

#------------------------------------------------------------------------------
# This function shaps the XML tags into JSON Objects
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node['created'] = {}
        address_info = {}
        for new in CREATED:
          node['created'][new]=element.attrib[new]
        if element.tag == "node":
            node['pos']= []
            node['pos'].append(float(element.attrib['lat']))
            node['pos'].append(float(element.attrib['lon']))
        else:
            node['node_refs'] = []
            for elem in element.iter('nd'): 
                node['node_refs'].append(elem.attrib['ref'])
        node['id'] = element.attrib['id']
        node['type'] = element.tag
        try:
            node['visible'] = element.attrib['visible']
        except KeyError:
            pass
        for elem in element.iter('tag'): 
           
           if lower.search(elem.attrib['k']) is not None:
                node[elem.attrib['k']] = elem.attrib['v']
                #searching for the street name
           elif lower_colon.search(elem.attrib['k']) is not None:
                    index = elem.attrib['k'].find('addr:')
                    if index >= 0:
                        name = elem.attrib['k'][index+5:len(elem.attrib['k'])]
                        if elem.attrib['k']== 'addr:postcode':
                            result = update_postalcode(elem.attrib['v'])  
                        elif elem.attrib['k']== 'addr:street':
                            result = update_streetname(elem.attrib['v'])
                        else:
                            result = elem.attrib['v']
                        address_info[name]= result
                    else:
                       node[elem.attrib['k']] = elem.attrib['v'] 
           elif elem.attrib['k'].count(':') == 1:
                       node[elem.attrib['k']] = elem.attrib['v']   
        if address_info != {}:
            node['address']={}
            for key in address_info:
                node['address'][key] = address_info[key]
        return node
    else:
        return None
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# This function creates the json file by using the "shape_element" function
def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    count = 1
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                #print data
                data.append(el)
                count += 1
                
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                   #print json.dumps(el)
                   fo.write(json.dumps(el) + "\n")
    return data

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# The following are Additional Function for the 'shape_element' function
street_type_re = re.compile(r'^\b\S+\.?', re.IGNORECASE)
nodes = set()
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def count_tags(filename):
        # YOUR CODE HERE
    dicc = {}
    with open(filename, 'r') as f:
        for event, element in ET.iterparse(f):
            if element.tag in dicc.keys():
                dicc[element.tag]+=1
            else:
                dicc[element.tag] = 1
        return dicc
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------       
def count_street(filename):
     with open(filename, 'r') as f:
         address = set()
         amenity = set()
         for event, element in ET.iterparse(f):
             if element.tag == 'node':
                 for elem in element.iter('tag'):
                     if elem.attrib['k'].find('addr:postcode')>= 0:
                         address.add(elem.attrib['v'])
                     if 'amenity' in elem.attrib['k']:
                         amenity.add(elem.attrib['v'])         
         print address   
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def update_postalcode(String):
    list = []
    index =String.find(';') 
    if index == -1:
        list.append(int(String))
    else:
        list.append(int(String[0:index]))
        list.append(int(String[index+1:len(String)]))
    return list
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------    
def update_streetname(String):
    if type(String) == str:
            String = String.decode('utf-8')
    for key in mapping:
        if String.find(key) >= 0:
            #print String
            String = mapping[key] + String[len(key):len(String)]
            #print String + '\n'
            break  
    return String
#------------------------------------------------------------------------------    
#------------------------------------------------------------------------------    
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        #if street_type not in expected:
        if type(street_name) == str:
            street_types[street_type].add(street_name.decode('utf-8'))
        else:
            street_types[street_type].add(street_name)
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    #print type(tag.attrib['v'])
                    audit_street_type(street_types, tag.attrib['v'])
    return street_types                    
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
if __name__ == '__main__':    
     process_map(Dateiname,False)



