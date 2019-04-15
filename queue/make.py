#-------------------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Merge and download scraped FTO data to Dropbox every day
# Contact: akshat.goel@ifmr.ac.in
#-------------------------------------------------------------------#
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
from backend.db import schema

pymysql.install_as_MySQLdb()


# Prepare the stage wise data for insert into data-base
def prep_csv(stage):
	
	try: 
		
		df=pd.read_csv('./output/{}.csv'.format(stage))

	except pd.errors.EmptyDataError as e:

		print(e)
		print(stage)

		cols=['block_code', 'district_code', 'fto_no', 'scrape_date', 'scrape_time', 
			  'state_code', 'transact_date', 'url']
		df=pd.DataFrame([], columns=cols)

	return(df)


# Create a list of all FTOs by stage	
def get_csv(stages):
	
	scraped_ftos = pd.concat(map(prep_stage, stages))
	unique_stages = fto_stages['stage'].unique().tolist()
	missing_stages = [stage for stage in stages if stage not in unique_stages]

	return(scraped_ftos, missing_stages)


# Calculate the current stage
def get_pivoted_stage(fto_stages):
	
	# Create dummy variables to get numeric values
	fto_stages_dum = pd.get_dummies(fto_stages, columns = ['stage'])
	fto_stages_dum.drop(['fto_no'], inplace = True, axis = 1)
	fto_stages = pd.concat([fto_stages, fto_stages_dum], axis = 1)

	# Reshape the data-set
	fto_stages['total'] = 1	
	fto_stages = fto_stages.pivot_table(index='fto_no', columns='stage', values='total', fill_value=0)

	return(fto_stages)


# Prep the queue for insert
def prep_queue_for_insert(fto_stages, stages, missing_stages):
	
	# Create the missing columns columns
	for col in missing_stages:
		fto_stages[col] = 0

	# Store the stages in order because it matters here
	# This will go in a .config file
	stages=['fst_sig_not', 'fst_sig', 'sec_sig_not', 'sec_sig','sb', 'pb', 'pp', 'P']

	# Correct the index
	fto_stages.reset_index(inplace=True)
	fto_stages['current_stage'] = ''

	# Allocate the current stage
	for stage in stages: fto_stages.loc[fto_stages[stage] == 1, 'current_stage'] = stage

	fto_stages=fto_stages(['fto_no','current_stage'], axis=1)
	
	return(fto_stages)


# Now we get the new FTOs along with their current stage
def get_new_ftos(engine, fto_stages, file_to):
	
	fto_queue = db_schema.select_data(engine, 'fto_queue', cols = ['fto_no'])
	new_ftos = db_schema.anti_join(fto_stages, fto_queue, on = ['fto_no'])
	new_ftos.to_csv(file_to, index = False)

	return


# Put the net FTO nos. in the queue
def put_fto_nos(engine, path, if_exists):
	
    fto_nos = pd.read_csv(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql('fto_queue', con = engine, index = False, if_exists = if_exists, chunksize = 100)

    return


def main(): 

	# Set up
	# Need to change this JSON
	stages = db_schema.load_stage_table_names()
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	fto_stages = clean_fto_stage(engine, stages)
	fto_stages = get_current_stage(fto_stages, stages)	
	update_current_stage_table(engine, fto_stages)
	
	get_new_ftos(engine, './output/fto_queue.csv')
	put_fto_nos('fto_queue', engine, './output/fto_queue.csv', 'append')



if __name__ == "__main__": 

	main()