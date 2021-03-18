# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 12:35:19 2020

@author: marks11
"""

# This script uses the requests library to get data from the U.S. Census
# Bureau GeoCoder API.
import pandas as pd
import numpy as np
import os
import requests

# Source data is the output of the Cognos report Student - PWL > Academic
# Schools > HHS > Enrollment > Enrollment List - Census - w Addresses
SES_path = 'S:/Strategic Data Manager/16 Analytics and Institutional Research/CASEY/Socioeconomic Status'
address_file = 'Modified Enrollment List for New Beginners/Fall 2011 - Fall 2019 Enrollment List - Census - w Addresses Clean.xlsx'

raw_enrl_w_addr = pd.read_excel(os.path.join(SES_path, address_file), header=6)

# Set up the dataframe for input to the Geocoder API. Multi-index is Year and
# Person UID. Other columsn are Adress, City, State, and Zip Code. Additionally,
# it specifies the benchmark and vintage of the Census geocoding data (tables 
# from different years are referred to by their "vintage"). For looking up
# the Block layer, the 2010 vintage is used. For looking up the Tract layer,
# the closest vintage to the year of interest is used (i.e., "Vintage - by
# Year")
clean_addr = raw_enrl_w_addr.copy()
clean_addr['Year'] = clean_addr['Academic Period Description'].map(lambda x: x[-4:])
clean_addr['Postal Code'] = clean_addr['Postal Code'].map(lambda x: str(x)[:5])
clean_addr = clean_addr[['Year', 'PERSON_UID', 'Street Address 1', 'City', 'State', 'Postal Code']]
clean_addr['Benchmark'] = 'Public_AR_Current'
clean_addr['Vintage - by Year'] = clean_addr.apply(lambda x: 'ACS'+x.loc['Year']+'_Current', axis=1)
clean_addr = clean_addr.set_index(['Year', 'PERSON_UID'])
clean_addr.loc[['2011','2012'],'Vintage - by Year'] = 'Census2010_Current'
clean_addr['Vintage - 2010'] = 'Census2010_Current'

# The geo_lookup function takes one record from the address dataframe, looks
# up the Census geography information in the Geocoder API, and return the
# State code, County code, Census Tract, (and Census Block Group and Census Block
# number if applicable).
# Arguments:
#   addr_Series: A series, which in this context is a row of the clean_addr
#       dataframe
#   layer: Whether to lookup the Blocks or Tracts layer of the Geocoder database.

def geo_lookup(addr_Series, layer):
    returntype = 'geographies'
    searchtype = 'address'
    api_format = 'json'
    api_layers = layer
    assert api_layers in ['Census Blocks', 'Census Tracts']
    vintage = {'Census Blocks':addr_Series.loc['Vintage - 2010'], 'Census Tracts':addr_Series.loc['Vintage - by Year']}
    url = 'https://geocoding.geo.census.gov/geocoder/'+returntype+'/'+searchtype
    arguments = {'benchmark': addr_Series.loc['Benchmark'], 'vintage': vintage[api_layers], 'street': addr_Series.loc['Street Address 1'], 'city':addr_Series.loc['City'], 'state':addr_Series.loc['State'], 'zip':addr_Series.loc['Postal Code'], 'format':api_format, 'layers':api_layers}
    response = requests.get(url, params=arguments)

    # If the request is not successful or there is not an address match, assign
    # None to all values.
    if response.status_code != 200 or response.json()['result']['addressMatches'] == []:
        State = None
        County = None
        Tract = None
        if api_layers == 'Census Blocks':
            Block_Group = None
            Block = None
    else:
        State = response.json()['result']['addressMatches'][0]['geographies'][api_layers][0]['STATE']
        County = response.json()['result']['addressMatches'][0]['geographies'][api_layers][0]['COUNTY']
        Tract = response.json()['result']['addressMatches'][0]['geographies'][api_layers][0]['TRACT']
        if api_layers == 'Census Blocks':
            Block_Group = response.json()['result']['addressMatches'][0]['geographies']['Census Blocks'][0]['BLKGRP']
            Block = response.json()['result']['addressMatches'][0]['geographies']['Census Blocks'][0]['BLOCK']
            
    if api_layers == 'Census Blocks':
        return pd.Series([addr_Series.loc['Street Address 1'], addr_Series.loc['City'], addr_Series.loc['State'], State, County, Tract, Block_Group, Block], index=['Address', 'City', 'State Text', 'State', 'County', 'Tract', 'Block Group', 'Block'])
    else:
        return pd.Series([addr_Series.loc['Street Address 1'], addr_Series.loc['City'], addr_Series.loc['State'], State, County, Tract], index=['Address', 'City', 'State Text', 'State', 'County', 'Tract'])


# Apply the geo_lookup function to every record in the address dataframe.
Census_Blocks_Output = clean_addr.apply(geo_lookup, layer = 'Census Blocks', axis=1)
Census_Tracts_Output = clean_addr.apply(geo_lookup, layer = 'Census Tracts', axis=1)

# Output results to file.
Census_Blocks_Output.to_csv(os.path.join(SES_path,'Census Block Geographies.csv'))
Census_Tracts_Output.to_csv(os.path.join(SES_path,'Census Tract Geographies.csv'))
