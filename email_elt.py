# import packages
import _snowflake
import simplejson as json
import requests
import time
import pandas as pd
import numpy as np
from math import ceil

# py object holds processes to extract and load data from PCO API
class pco_email_elt_funct:
    '''
    pco_email_elt_funct: Py object used in Snowflake to authenticate and fetch email data from PCO API.
      __init__: Fetches the SF secrets relating to the database the function is ran in
      create_session: creates a session with PCO to monitor rate handling so the connection doesn't close
      process: authenticates, loads, and returns data from PCO
    '''
    
    def __init__(self):
        ''' Authenticate PCO API with Personal Access Token'''

        # load secrets from SF secret manager
        credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
        
        self.app_id=credentials['app_id']
        self.secret=credentials['secret']

    
    def create_session(self):

        # create session
        s = requests.Session()
        # function to monintor calls left before process must wait. Function finds how long the process must wait.
        def api_calls(r,*args, **kwargs):
            calls_left = int(r.headers['X-PCO-API-Request-Rate-Limit']) - int(r.headers['X-PCO-API-Request-Rate-Count'])
            if calls_left == 1:
                print('limit close, sleeping')
                time.sleep(int(r.headers['X-PCO-API-Request-Rate-Period'])) # pause until 0
        s.hooks["response"] = api_calls
        return s


    def process(self, query_date):
        ''' Process will either load all data or will load data from the max date in the existing SF table. This saves on cost'''

        # api to load from
        api_base='https://api.planningcenteronline.com'

        # if this is the initial load, load all data
        if query_date == 'initial':
            api = '/people/v2/emails?per_page=100&where[primary]=true'
        # if data exists in SF, load updated data since the most recent updated date in the database
        else:
            date_for_query = pd.to_datetime(query_date)
            year = date_for_query.year
            month = date_for_query.month
            day = date_for_query.day
            query_date = f'{year}-{month}-{day}T12:00:00Z'
            api = f'/people/v2/emails?per_page=100&where[primary]=true&where[updated_at][gte]={query_date}'

            
        # creates the session and auths
        sess = self.create_session()
        resp = sess.get(api_base + api,
                        auth=(self.app_id, self.secret))
        # initial response
        resp_json  = resp.json()
        
        try:
            # must handle API pagination
            # get number of pages
            total_count = ceil(resp_json['meta']['total_count']/100) + 1
            # round up pages by 1 unit to get all data 
            total_count_list = np.arange(100,100 * total_count,100)

            # get the data from the json obj
            results = resp_json['data']

            # loop through all pages in the api
            for i in total_count_list:
                resp = sess.get(f"{api_base + api}&offset={i}",
                                auth=(self.app_id, self.secret))
                
                resp_json = resp.json()
                results.extend(resp_json['data']) 
                
        except:
            # if there is only one page of data, do this. Except clause runs when there is only 1 page.
            results = resp_json['data']
                
        # create a df from the concatated json obj
        data = pd.json_normalize(results)

        # rename columns
        data = data.rename(
            columns = {
                'id': 'EMAIL_ID',
                'attributes.address': 'ADDRESS',
                'attributes.location': 'LOCATION',
                'attributes.primary': 'PRIMARY',
                'attributes.blocked': 'BLOCKED',
                'attributes.created_at': 'CREATED_AT',
                'attributes.updated_at': 'UPDATED_AT',
                'relationships.person.data.id': 'PERSON_ID',
        }
    )
        # loop through rows and return each line and insert into the SF table        
        for index, row in data.iterrows():
            yield (row["PERSON_ID"], row["ADDRESS"], row["LOCATION"], row["PRIMARY"], row["BLOCKED"], row["CREATED_AT"], row["UPDATED_AT"])
