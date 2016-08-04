
# coding: utf-8

# Creating fake inrix data to test local PostgreSQL performance vs. our current server.

import datetime
from os import chdir
import csv

def daterange(start, stop, step):
    while start < stop:
        yield start
        start += step


#Generate fake TMC codes
#Source: http://stackoverflow.com/a/2257449/4047679
import string
import random
def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

NUMBER_OF_TMC = 20 

tmcs = []
for i in range (NUMBER_OF_TMC):
    tmcs.append(id_generator(9))

#WARNING, TREAD LIGHTLY THIS CAN GENERATE A LOT OF DATA
with open('sampledata.csv', 'w',newline='') as fp:
    a = csv.writer(fp, delimiter=',')
    step = datetime.timedelta(seconds=60)
    starttime = datetime.datetime(2012,7,1,0,0,0)
    stoptime = datetime.datetime(2012,7,2,0,0)
    for tx in daterange(starttime,stoptime,step):
        for tmc in tmcs:
            speed = random.randint(1,120)
            score = random.choice([10,20,30])
            a.writerow([tx,tmc,speed,score])
        


