#get operating_name and licence_addr_line_1 from busyness_opendata

import psycopg2
from credentials import *
import googlemaps
import pprint
import datetime
from datetime import datetime, date
import json
import urllib.request
import populartimes

try:
    connection = get_connection()
    cursor = connection.cursor()
    select_query = """SELECT gplace_id FROM test LIMIT 100"""
    cursor.execute(select_query)
    records = cursor.fetchall()
    print("establishing connection and writing records")

except (Exception, psycopg2.Error) as error:
    print("Error while getting data", error)

# get  gplace id from postgres table and search the populartimes data from google
result = []
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
date = date.today()
key = api_key()
for row in records:
            id_no = (row[0])
            busyness_data = populartimes.get_id(key, id_no)
            result.append(busyness_data)

list_length = len(result)
fin_data = []
static_data = []
#getting required elements from the listed dictionary
i = 0
try:
    while i < list_length:
        list_item = result[i]
        id = list_item.get('id')
        time_spent = list_item.get('time_spent')
        current_pop = list_item.get('current_popularity')
        write_static_data = ([id,time_spent,current_pop,date])
        static_data.append(write_static_data)
        popular_times = list_item.get('populartimes', [{}]*7)
        wait_times = list_item.get('time_wait',[{}]*7)
        for ptime,wtime in zip(popular_times, wait_times):
            pdayname = ptime.get('name',[{'name': 'Monday', 'name': 'Tuesday', 'name': 'Wednesday', 'name': 'Thursday', 'name': 'Friday', 'name': 'Saturday'}])
            phdata = ptime.get('data', [None] * 24)
            wthdata = wtime.get('data', [None] * 24)
            for hour, h1 in enumerate(list(zip(phdata,wthdata))):
                dayname = pdayname
                hour_ly = hour
                poptime = h1[0]
                wtime = h1[1]
                write_data =([id,hour_ly, poptime,current_time, dayname, date,wtime])
                fin_data.append(write_data)
        i = i+1            
except(Exception):
    pass

#Insert the list of data set returned
insert_query1 = """INSERT INTO populartimes (gplace_id, hour_of_day, busy_by_hour, data_acq_time, day_of_week, date,wait_by_hour) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
for item in fin_data:
    try:
        cursor.executemany(insert_query1, (item,))
    except(Exception):
        pass
connection.commit()
insert_query2 = """INSERT INTO static_data (gplace_id, time_spent, current_popularity, date) VALUES (%s,%s,%s,%s)"""
try:
    for items in static_data:
        cursor.executemany(insert_query2, (items,))
except(Exception):
    pass        
connection.commit()
close_connection(connection)
