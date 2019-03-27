#-------------------------#
# Import packages
#-------------------------#
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import argparse
import pandas as pd
import numpy as np

from sqlalchemy import *
from common import helpers
from backend.db import db_schema

pymysql.install_as_MySQLdb()

#-------------------------------------#
# Update current stage table
#-------------------------------------#
def get_new_ftos(engine, file_to):

	fto_stages = db_schema.select_data(engine, 'fto_current_stage', ['fto_no'])
	fto_queue = db_schema.select_data(engine, 'fto_queue', cols = ['fto_no'])
	
	new_ftos = db_schema.anti_join(fto_stages, fto_queue, on = ['fto_no'])
	new_ftos.to_csv(file_to, index = False)

	return

#-------------------------------------------------#
# Get the new FTOs which have not been scraped yet
#-------------------------------------------------#
def put_fto_nos(table, engine, path, if_exists):

    fto_nos = pd.read_csv(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    print(fto_nos)
    fto_nos.to_sql(table, con = engine, index = False, if_exists = if_exists, chunksize = 1000)

    return

#-------------------------------------#
# Put the execution here
#-------------------------------------#
def main(): 

	# Set up
	stages = db_schema.load_stage_table_names()
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
		
	# Get the new FTOs and put them in the FTO queue
	get_new_ftos(engine, './output/fto_queue.csv')
	put_fto_nos('fto_queue', engine, './output/fto_queue.csv', 'append')


if __name__ == "__main__": 

	main()