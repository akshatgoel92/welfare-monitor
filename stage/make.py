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
# Update stage tables
#-------------------------------------#
def get_empty_table_names(engine, stages):

	conn = engine.connect()
	trans = conn.begin()
	
	# First get a 1-0 flag for whether the stage table is empty
	# Then keep only those stages in dictionary which are empty as a list
	empty_stages = {stage: db_schema.check_table_empty(engine, tables[stage]) for stage in stages}
	empty_stages = [stage for stage in empty_stages.keys() if empty_stages[stage] == 1]
	non_empty_stages = [stage for stage in empy_stages.keys() if empty_stages[stage] == 0]
	
	return(empty_stages, non_empty_stages)


#----------------------------------------------------------------------#
# Don't attempt merge for empty stage tables
# This will result in a pandas 'no data found' error
# Just put the data in directly to the tables identified as empty above
#----------------------------------------------------------------------#
def put_data_empty_tables(engine, empty_stages, stage_table_names):

	conn = engine.connect()
	trans = conn.begin()

	try: 
		
		for stage in empty_stages:

			df =  pd.read_csv('./output/{}.csv'.format(stage))
			df.to_sql(stage_table_names[stage], 
					  con = conn, index = False, 
					  if_exists = 'append', chunksize = 1000)

		trans.commit()

	except Exception as e: 

		print(e)
		print('There was a failure creating one of the empty stage tables...exiting.')
		trans.rollback()
		sys.exit()

		
#----------------------------------------------------#
# Make sure that data types are the same before merge
#----------------------------------------------------#
def get_scraped_data_csv(stage, cols):

	scraped_ftos = pd.read_csv(filepath = './output/{}.csv'.format(stage))

	for col in cols:

		scraped_ftos[col] = scraped_ftos[col].astype(object)

	return(scraped_ftos)


#----------------------------------------------------#
# Update the stage tables for those stages which 
# already have data in them 
#----------------------------------------------------#
def update_stage_tables(engine, stages, cols):

	conn = engine.connect()
	trans = conn.begin()

	for stage in stages:

		try: 
			
			scraped_ftos = get_scraped_data_csv(stage, ['transact_date'])
			existing_ftos = db_schema.select_data(stages[stage], con = conn)
			
			# We have written a function which does an anti join in the db_schema script
			new_ftos = db_schema.anti_join(scraped_ftos, existing_ftos)
			new_ftos.to_sql(stage, con = conn, index = False, if_exists = 'append', chunksize = 1000)
			
			print('Done...:{}'.format(stage))
			trans.commit()
		
		except pd.errors.EmptyDataError as e:
			
			print(e)
			print('This {} .csv does not have any data....!'.format(stage))
			continue
		
		except Exception as e:
			
			print(e)
			
			message = 'There was an uncaught error in the creation of the SQL table for stage: {}'
			print(message.format(stage))
			
			trans.rollback()
			sys.exit()

	
#-------------------------------------#
# Prepare the current stage table
#-------------------------------------#
def prep_current_stage_table(engine, stages):

	fto_stages = [db_schema.select_data(engine, stages[stage]) for stage in stages]
	fto_stages = pd.concat(fto_stages)
	fto_stages.columns = ['fto_no', 'fto_stage']
	fto_stages = fto_stages.groupby(['fto_no']).size().reset_index()
	
	return(fto_stages)
	

#-------------------------------------#
# Update current stage table
#-------------------------------------#
def update_current_stage_table(engine, fto_stages):

	conn = engine.connect()
	trans = conn.begin()

	try:
		
		fto_stages.to_sql('fto_current_stage', con = conn, index = False, if_exists = 'replace')
		trans.commit()

	except Exception as e:

		print(e)
		trans.rollback()
		sys.exit()

	return(fto_stages)


#-------------------------------------#
# Update current stage table
#-------------------------------------#
def get_new_fto_nos(engine, fto_stages, file_to):

	new_ftos = db_schema.anti_join(fto_stages, on = 'fto_no')
	new_ftos.to_csv(file_to)

	return


#-------------------------------------------------#
# Get the new FTOs which have not been scraped yet
#-------------------------------------------------#
def put_fto_nos(table, engine, path, if_exists):

    fto_nos = pd.read_csv(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql(table, con = engine, index = False, if_exists = if_exists)

    return


#-------------------------------------#
# Put the execution here
#-------------------------------------#
def main(): 

	# Get stage table names from .json
	stages = db_schema.load_stage_table_names()
	
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	make_stage_tables(engine, stages, cols)
	update_stage_tables(engine, stages, cols)
	update_current_stage_table(engine, stages)
	
	get_new_ftos(engine)
	put_fto_nos('fto_queue', engine, './output/fto_queue.csv', 'append')


#-------------------------------------#
# Put the execution here for main
#-------------------------------------#
if __name__ == "__main__": 

	main()