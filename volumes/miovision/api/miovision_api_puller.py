import requests
import click
import configparser
import datetime
from datetime import date
import logging
from psycopg2 import connect

from dateutil.relativedelta import relativedelta

from parsing_utilities import validate_multiple_yyyymmdd_range

class miovision_api(object):
    '''Stores database connection and API Key to then use for iteration over
       calls to the miovision API'''
    BASEURL = 'https://api.miovision.com/intersections/'
    tmc_endpoint = '/tmc'
    xwalk_endpoint = '/crosswalkTmc'
    def __init__(self, dbsettings, api_settings):
        self.con = connect(**dbsettings)
        self.headers = {'Authorization': api_settings['api_key']}

    @staticmethod
    def get_config(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        dbsettings = config['DBSETTINGS']
        api_settings = config['API']
        return dbsettings, api_settings

    def pull_tmc(self, intersection_id, end_time, start_time = None):
        params = {'endTime': end_time, 'startTime' : start_time}
        request = requests.get(self.BASEURL+intersection_id+self.tmc_endpoint,
                            params=params,
                            headers=self.headers)
        test_response( request.response())

if __name__ == '__main__':
    main()