# -*- coding: utf-8 -*-
"""
Script for the text to centreline geometry tool that takes an input CSV file
with three columns. One column is the street that the bylaw occurs on and
the other two are streets that intersect it between which the bylaw is in 
effect. The CSV must not have header column labels.


"""

from psycopg2 import connect
import configparser
import sys, os
import numpy as np
import pandas as pd
import pandas.io.sql as psql

import csv

CONFIG = configparser.ConfigParser()
CONFIG.read('db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)


def text_to_centreline(highway, fr, to): 
    df = psql.read_sql("SELECT text_to_centreline FROM crosic.text_to_centreline('{}', '{}', '{}')".format(highway, fr, to), con)
    return df['text_to_centreline'].item()


def write_to_csv(input_file_name, output_file_name): 
    """
    Takes an input CSV file
    with three columns. One column is the street that the output centreline s
    egements occur on and the other two are streets that intersect it between 
    which we want centreline segments for. 
    The CSV must not have header column labels.
    
    Outputs a new CSV file with all the contents of the original csv 
    plus a new column with the geometry of the stretc of road in between 
    the two intersections

    this function was inspired by: 
    https://stackoverflow.com/questions/11070527/how-to-add-a-new-column-to-a-csv-file
    """
    with open(input_file_name, 'r') as csv_file, open(output_file_name, 'w') as csv_output:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_writer = csv.writer(csv_output, lineterminator='\n')
            
        rows = []
            
        for row in csv_reader:
            row.append(text_to_centreline(row[0], row[1], row[2]))
            rows.append(row)
    
        csv_writer.writerows(rows)
        
        csv_writer.close()
        csv_reader.close()



write_to_csv('text_to_centreline.csv', 'text_geoms.csv')


con.close()