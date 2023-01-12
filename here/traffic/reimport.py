'''
@author: Raphael Dumas
To pull a year of data from the HERE API by making monthly calls to the 
here_api.py script. This blocks in IO in waiting for a query on TA to 
finish (not a bad thing to not overload HERE) and in sending data to 
our database. Run with `nohup reimport.py >> reimport.log 2>&1 &`
'''

from dateutil.relativedelta import relativedelta
from datetime import date, timedelta
import subprocess
from subprocess import CalledProcessError
import json
import requests

YEAR = 2019

def send_slack_msg(text):
    headers = {'Content-type': 'application/json'}
    url = 'SLACK_URL'
    requests.post(url, headers=headers, data=json.dumps({'text':text}))

for month in range(1,13):
    mon = date(YEAR, month, 1)
    startdate = mon.strftime('%Y%m%d')
    enddate = (mon + relativedelta(months=1) - timedelta(days=1)).strftime('%Y%m%d')
    try:
        subprocess.check_call(['python','here_api.py', '-d', '/home/rdumas/here_api/db.cfg', '-s', startdate, '-e', enddate])
    except CalledProcessError as ex:
        print('Something broke')
        #send_slack_msg('@radumas, :here: pulling failed for {}'.format(mon.strftime('%Y-%m')))
