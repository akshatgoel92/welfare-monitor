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
from backend.db import update
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
def get_pivoted_stage(fto_stages):
	
	fto_stages_dum = pd.get_dummies(fto_stages, columns = ['stage'])
	fto_stages_dum.drop(['fto_no'], inplace = True, axis = 1)
	fto_stages = pd.concat([fto_stages, fto_stages_dum], axis = 1)
	
	fto_stages['total'] = 1	
	fto_stages = fto_stages.pivot_table(index='fto_no', columns='stage', values='total', fill_value=0)
	fto_stages.columns.name = ''

	return(fto_stages)


# Prep the queue for insert
# Create the missing columns
# Create a current stage column
# Iterate through the data-set and create the current stage variable
# Return the data-frame ready to be merged with the original data-set
def prep_stages_for_insert(fto_stages, stages, missing_stages):
	
	for col in missing_stages: fto_stages[col] = 0

	fto_stages.reset_index(inplace = True)
	fto_stages['current_stage'] = ''

	for stage in stages: fto_stages.loc[fto_stages[stage] == 1, 'current_stage'] = stage
	fto_stages = fto_stages[['fto_no','current_stage']]
	print('Done')

	fto_stages['done'] = '0'
	fto_stages['fto_type'] = ''
	
	fto_stages['stage'] = fto_stages['current_stage']
	fto_stages = fto_stages[['fto_no', 'done', 'fto_type', 'current_stage', 'stage']]
	
	fto_stages = fto_stages.values.tolist()
	fto_stages = [tuple(row) for row in fto_stages]
	print(fto_stages[0:5])

	return(fto_stages)


# Update the FTO queue table
# This can be optimized using executemany or to_sql
# When optimizing make sure that we are handling: 
# 1) updates to stage of existing FTOs
# 2) case where connection to database drops during insert 
def insert_ftos(engine, fto_stages):

	sql = update.upsert_data('fto_queue', ['current_stage'])	
	conn = engine.connect()
	trans = conn.begin()

	try: 
		
		for row in fto_stages:
			conn.execute(sql, row)

		trans.commit()

	except Exception as e: 
		
		er.handle_error(error_code ='3', data = {})
		trans.rollback()
		sys.exit()

	try: 

		update.create_primary_key(engine, 'fto_queue', ['fto_no'], is_string = 1)

	except Exception as e:

		er.handle_error(error_code ='4', data = {})
		pass


# Now we get the new FTOs along with their current stage
def get_new_ftos(engine, fto_stages, file_to):
	
	fto_queue = update.select_data(engine, 'fto_queue', cols = ['fto_no'])
	new_ftos = update.anti_join(fto_stages, fto_queue, on = ['fto_no'])
	new_ftos.to_csv(file_to, index = False)


def main(): 

	stages = schema.load_stage_table_names()
	engine = helpers.db_engine()
	
	fto_stages, missing_stages = get_csv(stages)
	fto_stages_dum = get_pivoted_stage(fto_stages)	
	fto_stages_dum = prep_stages_for_insert(fto_stages_dum, stages, missing_stages)

	insert_ftos(engine, fto_stages_dum)


if __name__ == "__main__": 
	main()