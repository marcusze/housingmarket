#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 11:44:23 2017
booli2 is an update of booli that retrieves all the available data from the booli listing api.
areaId for Sweden is 77104
@author: marcus
"""

import requests
import json
import time
import random
import string
import hashlib
import pandas as pd

headers = {'Accept': 'application+json'}
headers = {'Accept': 'application/vnd.booli-v2+json'}

def get_auth():
    callerId =''
    secret = ''
    unique = ''.join(random.sample(string.ascii_lowercase + string.digits,16))
    timestamp = str(int(time.time()))
    hashstamp = hashlib.sha1(str(callerId + str(timestamp) + secret + unique).encode('utf-8')).hexdigest()
    
    auth =  {'callerId': callerId,
             'time': timestamp,
             'unique': unique,
             'hash': hashstamp}
    return auth

def get_objects(search_params ,sold_or_listings = 'sold'):
    """
    
    """
    assert (sold_or_listings =='sold') or (sold_or_listings=='listings')
    urlpath = 'https://api.booli.se/' + sold_or_listings

    
    results = pd.DataFrame()
    
    if isinstance(search_params, str):
        search_params = {'q': search_params}
    assert isinstance(search_params, dict)
    
    total_count = 1e10 #something big
    limit = 500 #max json response
    offset = 0
    auth = get_auth()
    
    failed_requests = 0
    
    try:
        while offset < total_count:
            response = requests.get(urlpath, params = {**search_params, 'limit': limit, 'offset': offset, **auth}, headers = headers)
            if response.status_code != 200 and failed_requests==3:
                return response
            
            elif response.status_code != 200:
                failed_requests+=1
                print(response)
                print(response.text)
                auth = get_auth()
                
            else:
                offset = response.json()['offset']+response.json()['limit']
                total_count = response.json()['totalCount']
                results = pd.concat([results, pd.DataFrame(response.json()[sold_or_listings])])
                
                print(min(offset,total_count), 'of', total_count)
                print('Progress', int(100*min(offset,total_count)/max(total_count,1)), '%')
    except KeyboardInterrupt:
        print('paused at', offset, 'of', total_count)
        
    return results



    