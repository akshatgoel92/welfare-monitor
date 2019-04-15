#--------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Create database tables according to schema
# Contact: akshat.goel@ifmr.ac.in
#--------------------------------------------------------#
import os
import json 
import pandas as pd
import numpy as np
import pymysql

from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlalchemy.schema import Index
from datetime import datetime
from common import helpers


# Check if a given table is empty
# MySQL uses an arbitrary index 
# Exists return 1 if the row exists else 0
# We take that and store it in not_empty
def check_table_empty(conn, table):
	
	result = pd.read_sql('SELECT EXISTS ' + '(SELECT 1 FROM ' + table + ')', con = conn)
	is_empty = result.iloc[0, 0]

	return(is_empty)


# Return only those entries in df_1 not in df_2
def anti_join(df_1, df_2, on):
	
	df = pd.merge(df_1, df_2, how = 'outer', on = on, indicator = True)
	df = df.loc[df['_merge'] == 'left_only']
	df.drop(['_merge'], inplace = True, axis = 1)

	return(df)


# Get data from the specified columns of the specified table
def select_data(engine, table, cols = ['*']):

	cols = '.'.join(cols)
	query = 'SELECT {} FROM {};'.format(cols, table)
	result = pd.read_sql(query, con = engine)

	return(result)


# Insert a new item into the SQL database
# First get fields which are both in the item and tha table
# Then join them in a list with the following format: [field1, field2, ...]
# Then create a list of data types qm with the following format: [%s, %s, ...]
# Then create the SQL queries
# Then construct the final query depending on the kind of insert
# The final query has the following format: "INSERT (IGNORE) INTO table_name [field1, field2] VALUES [%s, %s] "
# Then get the data from the item 
# The pipelines file will use string formatting to substitute the data values into [%s, %s]
def insert_data(item, keys, table, unique = 0):
	
	
	keys = get_keys(table) & item.keys()
	fields = u','.join(keys)
	qm = u','.join([u'%s'] * len(keys))
	
	sql = "INSERT INTO " + table + " (%s) VALUES (%s)"
	sql_unique = "INSERT IGNORE INTO " + table + " (%s) VALUES (%s)"
	
	insert = sql if unique == 0 else sql_unique
	sql = insert % (fields, qm)
	data = [item[k] for k in keys]

	return(sql, data)


# Update FTO type in fto_queue SQL table as scrape runs
# There are multiple types of FTOs
# The two main types are wage FTOs for NREGA workers and materials FTOs for vendors
def update_fto_type(fto_no, fto_type, table):
	
	sql = "UPDATE " + table + " SET fto_type = %s WHERE fto_no = %s"
	data = [fto_type, fto_no]
	
	return(sql, data)


# Get a table's keys
# This is used by the pipelines file to decide where each field will should be sent
def get_keys(table):
	
	with open('./backend/db/table_keys.json') as file:
		
		tables = json.load(file)
		keys = tables[table]
	
	return(keys)