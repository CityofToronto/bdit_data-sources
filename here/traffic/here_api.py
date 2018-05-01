import calendar
import configparser
import datetime
import re
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from time import sleep

import requests

import click
from requests_oauthlib import OAuth1


def _get_date_yyyymmdd(yyyymmdd):
    datetime_format = '%Y%m%d'
    try:
        date = datetime.strptime(str(yyyymmdd), datetime_format)
    except ValueError:
        raise ValueError('{yyyymmdd} is not a valid year-month value of format YYYYMMDD'
                         .format(yyyymmdd=yyyymmdd))
    return date

def _validate_yyyymmdd_range(yyyymmdd_range):
    """Validate the two yyyymm command line arguments provided
    Args:
        yyyymmdd_range: List containing a start and end year-month in yyyymm format
    
    Returns:
        A nested dictionary with the processed range like {'yyyy':range(mm1,mm2+1)}
    
    Raises:
        ValueError: If the values entered are incorrect
    """

    if len(yyyymmdd_range) != 2:
        raise ValueError('{yyyymmdd_range} should contain two YYYYMMDD arguments'
                         .format(yyyymmdd_range=yyyymmdd_range))
    
    
    date1, date2 = (_get_date_yyyymmdd(date) for date in yyyymmdd_range)

    if date1 > date2:
        raise ValueError('Start date {yyyymm1} after end date {yyyymm2}'
                         .format(yyyymm1=yyyymmdd_range[0], yyyymm2=yyyymmdd_range[1]))

    years = defaultdict(dict)

    #Same YYYYMM combo
    if (date1.year == date2.year) and (date1.month == date2.month):
        years[date1.year] = {date1.month: range(date1.day, date2.day + 1)}
        return years
    #Iterate over years and months
    for year in range(date1.year, date2.year + 1):
        if(year == date1.year) and (date1.year == date2.year):
            month1, month2 = date1.month, date2.month
        elif year == date1.year:
            month1, month2 = date1.month, 12
        elif year == date2.year:
            month1, month2 = 1, date2.month
        else:
            month1, month2 = 1, 12
        for month in range(month1, month2 + 1):
            #Start of the YYYYMMDD range
            if(year == date1.year) and (month == date1.month):
                years[year][month] = range(date1.day, calendar.monthrange(year, month)[1] + 1)
            #End of the YYYYMMDD range
            elif(year == date2.year) and (month == date2.month):
                years[year][month] = range(1, date2.day + 1)
            #Full month
            else:
                years[year][month] = range(1, calendar.monthrange(year, month)[1] + 1)
 
    return years

def get_access_token(key_id, key_secret, token_url):
    '''Uses Oauth1 to get an access token using the key_id and client_secret'''
    oauth1 = OAuth1(key_id, client_secret=key_secret)
    headers = {'content-type': 'application/json'}
    payload = {'grantType':'client_credentials', 'expiresIn': 3600}

    r = requests.post(token_url, auth=oauth1, json=payload, headers=headers)

    access_token = r.json()['accessToken']
    return access_token

def query_dates(access_token, start_date, end_date, query_url, user_id, user_email):
    query= {"queryFilter": {"requestType":"PROBE_PATH",
                            "vehicleType":"ALL",
                            "adminId":21055226,
                            "adminLevel":3,
                            "isoCountryCode":"CAN",
                            "startDate":start_date,
                            "endDate":end_date,
                            "timeIntervals":[],
                            "locationFilter":{"tmcs":[]},
                            "daysOfWeek":{"U":True,"M":True,"T":True,"W":True,"R":True,"F":True,"S":True},
                            "mapVersion":"2017Q3"},
            "outputFormat":{"mean":True,
                            "tmcBased":False,
                            "epochType":5,
                            "percentiles":[5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95],
                            "minMax":True,
                            "stdDev":True,
                            "confidence":True,
                            "freeFlow":False,
                            "length_":True,
                            "gapFilling":False,
                            "speedLimit":False,
                            "sampleCount":False},
            "estimatedSize":0,
            "userId":user_id,
            'userEmail':user_email}

    query_header = {'Authorization':'Bearer '+ access_token, 'Content-Type': 'application/json'}

    query_response = requests.post(query_url, headers=query_header, json=query)
    return str(query_response.json()['requestId'])

def get_download_url(request_id, status_base_url, access_token, user_id):
    '''Pings to get status of request and then returns the download URL when it has successfully completed'''

    status='Pending'
    status_url = status_base_url + str(user_id) + '/requests/' + str(request_id)
    status_header = {'Authorization': 'Bearer ' +  access_token}

    while status != "Completed Successfully":
        sleep(60)
        query_status = requests.get(status_url, headers = status_header)
        status = str(query_status.json()['status'])

    return query_status.json()['outputUrl']

def download_data(download_url, filename):
    download = requests.get(download_url, stream=True)

    with open(filename+'.csv.gz', 'wb') as f:
        shutil.copyfileobj(download.raw, f)

def send_data_to_database(dbsetting, filename):
    subprocess.Popen('gunzip -c ' + filename +' | psql -h '+ dbsetting['host'] +r' -d bigdata -c "\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;"', shell=True)

@click.command()
@click.argument('startdate', help='Start date for pulling data (YYYYMMDD)')
@click.argument('enddate', help='End date for pulling data (YYYYMMDD)')
def main(start_date, end_date):
    config = configparser.ConfigParser()
    config.read('db.cfg')
    dbsettings = config['DBSETTINGS']
    apis = config['API']

    access_token = get_access_token(apis['key_id'], apis['client_secret'], apis['token_url'])

    dates_list = _validate_yyyymmdd_range([start_date, end_date])

    request_id = query_dates(access_token, dates_list[0], dates_list[1], apis['query_url'], apis['user_id'], apis['user_email'])

    download_url = get_download_url(request_id, apis['status_base_url'], access_token, apis['user_id'])
    filename = 'here_data_'+str(start_date)+'_'+str(end_date)
    download_data(download_url, filename)

    send_data_to_database(dbsettings, filename+'.csv.gz')


# if __name__ == '__main__':
#     main()
