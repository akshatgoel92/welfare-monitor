import os
import json
import sys
import dropbox
import pymysql
import smtplib
import argparse
import datetime
import pandas as pd
import numpy as np

from sqlalchemy import *
from common import helpers
from db import schema
from db import update
from common import errors as er

pymysql.install_as_MySQLdb()


# Prepare the stage wise data for insert into data-base
def prep_csv(stage):
	
	try: 
		
		df = pd.read_csv('./output/{}.csv'.format(stage))
		df['stage'] = stage

	except pd.errors.EmptyDataError as e:
        
		er.handle_error(error_code ='2', data = {'stage': stage})
		cols = ['block_code', 'district_code', 'fto_no', 'state_code', 'transact_date', 'url', 'stage']
		df = pd.DataFrame([], columns = cols)
    
	return(df)


# Create a long format data-frame of all FTOs by stage	
def get_csv(stages):
	
	scraped_ftos = pd.concat(map(prep_csv, stages))
	unique_stages = scraped_ftos['stage'].unique().tolist()
	missing_stages = [stage for stage in stages if stage not in unique_stages]
    
	return(scraped_ftos, missing_stages)


# Calculate the current stage
# Create dummy variables to get numeric values for whether an FTO has been finished at each stage
def get_current_stage(fto_stages, stages, missing_stages):
	
	fto_stages_dum = pd.get_dummies(fto_stages, columns = ['stage'])
	fto_stages_dum.drop(['fto_no'], inplace = True, axis = 1)
	fto_stages = pd.concat([fto_stages, fto_stages_dum], axis = 1)
	
	fto_stages['total'] = 1	
	fto_stages = fto_stages.pivot_table(index='fto_no', columns='stage', values='total', fill_value=0)
	fto_stages.columns.name = ''
	fto_stages['stage'] = ''
	
	for col in missing_stages: fto_stages[col] = 0
	for stage in stages: fto_stages.loc[fto_stages[stage] == 1, 'stage'] = stage

	return(fto_stages)


# Prep the queue for insert
# Create the missing columns
# Create a current stage column
# Iterate through the data-set and create the current stage variable
# Return the data-frame ready to be merged with the original data-set
def format_queue_for_insert(fto_stages, stages, missing_stages):
    
	fto_stages.reset_index(inplace = True)
	fto_stages['done'] = '0'
	fto_stages = fto_stages[['fto_no', 'done', 'stage']]
	
	msg = ""
	subject = "GMA Update 1: Ready to insert new FTOs"
	helpers.send_email(subject, msg)
	
	return(fto_stages)


def add_fto_type(fto_stages):
	
	get_fto_types = "SELECT fto_no, fto_type FROM fto_queue;"
	engine = helpers.db_engine()
	conn = engine.connect()
	trans = conn.begin()
	
	try: fto_queue = pd.read_sql(get_fto_types, con = engine)
	except Exception as e: 
		trans.rollback()
		print(e)
    
	fto_stages = pd.merge(fto_stages, fto_queue, on = ['fto_no'], how = 'left')
	fto_stages.rename(columns = {'fto_type_y': 'fto_type'}, inplace = True)
	fto_stages = fto_stages[['fto_no', 'done', 'fto_type', 'stage']]
    
	return(fto_stages)


# Update the FTO queue table
# This can be optimized using executemany or to_sql
# When optimizing make sure that we are handling: 
# 1) updates to stage of existing FTOs
# 2) case where connection to database drops during insert 
def insert_ftos(fto_stages, test):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	trans = conn.begin()

	try:
		
		fto_stages.to_sql('fto_queue', con = engine, index = False, if_exists = 'replace', chunksize = 100,
						  dtype = {'fto_no': String(100), 'fto_type': String(15), 'done': SmallInteger(), 
						  			'stage': String(15)})
					
		if test == 0: 
			
			trans.commit()
			msg = ""
			subject = "GMA Update 2: Finished inserting new FTOs"
			helpers.send_email(subject, msg)

	except Exception as e:
        
		print(e)
		er.handle_error(error_code ='3', data = {})
		trans.rollback()
		sys.exit()


def insert_ftos_history(fto_stages, test):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	trans = conn.begin()
	
	fto_stages['action_time'] = str(datetime.datetime.now().date())
	
	try: fto_stages_history = pd.read_sql('fto_queue_history', con = engine)
	except Exception as e: print(e)
		
	fto_stages = update.anti_join(fto_stages, fto_stages_history, on = ['fto_no', 'stage'])
	fto_stages = fto_stages[['fto_no', 'stage', 'action_time_x']]
	fto_stages.rename(columns = {'action_time_x': 'action_time'}, inplace = True)
		
	try:
		
		fto_stages.to_sql('fto_queue_history', con = engine, index = False, if_exists = 'append', chunksize = 100,
						  dtype = {'fto_no': String(100), 'stage': String(15), 'action_time': String(50)})
					
		if test == 0: 
			
			trans.commit()
			msg = ""
			subject = "GMA Update 3: Finished inserting new FTOs into history table"
			helpers.send_email(subject, msg)

	except Exception as e:
        
		print(e)
		er.handle_error(error_code ='3', data = {})
		trans.rollback()
		sys.exit()


def main(test = 0): 
	
	stages = schema.load_stage_table_names()
	fto_stages, missing_stages = get_csv(stages)
	fto_stages = get_current_stage(fto_stages, stages, missing_stages)
	fto_stages = format_queue_for_insert(fto_stages, stages, missing_stages)
	fto_stages = add_fto_type(fto_stages)
	insert_ftos(fto_stages, test)
	insert_ftos_history(fto_stages, test)
	

if __name__ == "__main__": 
	
	main()