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
from backend.db import schema

pymysql.install_as_MySQLdb()


#-------------------------------------#
# Update stage tables
#-------------------------------------#
def get_empty_table_names(engine, stages):

	conn = engine.connect()
	trans = conn.begin()
	
	# First get a 1-0 flag for whether the stage table is empty
	# Then keep only those stages in dictionary which are empty as a list
	empty_stages = {stage: db_schema.check_table_empty(engine, stages[stage]) for stage in stages}
	empty_stages = [stage for stage in empty_stages.keys() if empty_stages[stage] == 1]
	non_empty_stages = {stage:stages[stage] for stage in stages if stage not in empty_stages}
	
	return([empty_stages, non_empty_stages])

#----------------------------------------------------#
# Make sure that data types are the same before merge
#----------------------------------------------------#
def get_and_change_dtype_csv(stage, cols):

	scraped_ftos = pd.read_csv('./output/{}.csv'.format(stage))

	for col in cols:

		scraped_ftos[col] = scraped_ftos[col].astype(object)

	return(scraped_ftos)


#----------------------------------------------------------------------#
# Don't attempt merge for empty stage tables
# This will result in a pandas 'no data found' error
# Just put the data in directly to the tables identified as empty above
#----------------------------------------------------------------------#
def put_data_empty_tables(engine, empty_stages, stage_table_names):

	conn = engine.connect()
	trans = conn.begin()

	for stage in empty_stages:

		try:

			df =  pd.read_csv('./output/{}.csv'.format(stage))
			df.to_sql(stage_table_names[stage], con = conn, index = False, if_exists = 'append', 
					  chunksize = 1000)
			print('Done putting data in empty stage table for {}'.format(stage))
	
		except pd.errors.EmptyDataError as e:
			
			send_email(e, "GMA Warning 1: This {} .csv does not have any data....!'.format(stage)")
			continue

		except Exception as e: 

			send_email(e, 'GMA Error 1: There was a failure creating one of the empty stage tables...exiting.')
			trans.rollback()
			sys.exit()

	trans.commit()
	return


#----------------------------------------------------#
# Update the stage tables for those stages which 
# already have data in them 
#----------------------------------------------------#
def update_stage_tables(engine, stages, cols):

	conn = engine.connect()
	trans = conn.begin()

	for stage in stages.keys():

		try: 
			
			scraped_ftos = get_and_change_dtype_csv(stage, ['transact_date'])
			existing_ftos = db_schema.select_data(engine, stages[stage], cols = ['fto_no'])
			
			new_ftos = db_schema.anti_join(scraped_ftos, existing_ftos, on = ['fto_no'])
			new_ftos.to_sql(stages[stage], con = conn, index = False, if_exists = 'append', chunksize = 1000)
			
			msg = 'Done updating the following non-empty stage...:{}'.format(stage)
			send_email(msg, 'The no. of new FTOs is {} for stage {}'.format(str(len(new_ftos)), stage))
			
		except pd.errors.EmptyDataError as e:
			
			send_email(e, 'GMA Warning 1: This {} .csv does not have any data....!'.format(stage))
			continue
		
		except Exception as e:
			
			message = 'GMA Error 2: There was an error in the creation of the SQL table for stage: {}'
			send_email(e, message.format(stage))
			
			trans.rollback()
			sys.exit()

	trans.commit()


#----------------------------------------------#
# Allocate the correct stage using this function
#----------------------------------------------#
def get_fto_stage(df, stage):
		
	df['fto_stage'] = stage 

	return(df)


#----------------------------------------------#
# Clean FTO stage  using this function
#----------------------------------------------#
def clean_fto_stage(engine, stages):
	
	fto_stages = [db_schema.select_data(engine, stages[stage], ['fto_no']) for stage in stages]
	fto_stages = pd.concat([get_fto_stage(df, stage) for df, stage in zip(fto_stages, stages.keys())])
	fto_stages.columns = ['fto_no', 'fto_stage']

	return(fto_stages)


#----------------------------------------------#
# Clean FTO stage  using this function
#----------------------------------------------#	
def get_current_stage(fto_stages, stages):

	# Get the stages which have no FTOs
	unique_stages = fto_stages['fto_stage'].unique().tolist()
	all_stages = stages.keys()
	missing_stages = [stage for stage in all_stages if stage not in unique_stages]

	# Create dummy variables to get numeric values
	fto_stages_dum = pd.get_dummies(fto_stages, columns = ['fto_stage'])
	fto_stages_dum.drop(['fto_no'], inplace = True, axis = 1)
	fto_stages = pd.concat([fto_stages, fto_stages_dum], axis = 1)

	# Reshape the data-set
	fto_stages['total'] = 1	
	fto_stages = fto_stages.pivot_table(index='fto_no', columns='fto_stage', values='total', fill_value=0)

	# Create the missing columns columns
	for col in missing_stages:
		fto_stages[col] = 0

	# Correct the index
	fto_stages.reset_index(inplace=True)

	# Create the current stage column
	fto_stages['current_stage'] = ''
	
	# First signature
	fto_stages.loc[fto_stages['fst_sig'] == 1, 'current_stage'] = 'fst_sig'
	fto_stages.loc[fto_stages['fst_sig_not'] == 1, 'current_stage'] = 'fst_sig_not'
	
	# Second signature
	fto_stages.loc[fto_stages['sec_sig'] == 1, 'current_stage'] = 'sec_sig'
	fto_stages.loc[fto_stages['sec_sig_not'] == 1, 'current_stage'] = 'sec_sig_not'
	fto_stages.loc[fto_stages['sb'] == 1, 'current_stage'] = 'sb'
	
	# Bank/PFMS processing steps
	fto_stages.loc[fto_stages['pp'] == 1, 'current_stage'] = 'P'
	fto_stages.loc[fto_stages['pp'] == 1, 'current_stage'] = 'pp'
	fto_stages.loc[fto_stages['pb'] == 1, 'current_stage'] = 'pb'

	# Keep only the FTO no and the current stage to write to the database
	fto_stages = fto_stages[['fto_no', 'current_stage']]

	return(fto_stages)



#-------------------------------------#
# Update current stage table
#-------------------------------------#
def update_current_stage_table(engine, fto_stages):

	conn = engine.connect()
	trans = conn.begin()

	try:
		
		fto_stages.to_sql('fto_current_stage', con = conn, index = False, if_exists = 'replace', chunksize = 1000)
		
	except Exception as e:

		print(e)
		send_email(e, "GMA Error 3: There was an error in the creation of the FTO current stage tracker!")
		trans.rollback()
		sys.exit()

	trans.commit()

	return(fto_stages)


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
def put_fto_nos(engine, path, if_exists):

    fto_nos = pd.read_csv(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql('fto_queue', con = engine, index = False, if_exists = if_exists, chunksize = 100)

    return


#-------------------------------------#
# Put the execution here
#-------------------------------------#
def main(): 

	# Set up
	stages = db_schema.load_stage_table_names()
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	# Get empty and non-empty tables respectively
	current_stage_tables = get_empty_table_names(engine, stages)
	empty_stages = current_stage_tables[0]
	non_empty_stages = current_stage_tables[1]

	# Update the stage wise tables
	put_data_empty_tables(engine, empty_stages, stages)
	update_stage_tables(engine, stages, cols = ['transact_date'])
	
	# Update the current stage table
	fto_stages = clean_fto_stage(engine, stages)
	fto_stages = get_current_stage(fto_stages, stages)	
	update_current_stage_table(engine, fto_stages)
	
	# Get the new FTOs and put them in the FTO queue
	get_new_ftos(engine, './output/fto_queue.csv')
	put_fto_nos('fto_queue', engine, './output/fto_queue.csv', 'append')



if __name__ == "__main__": 

	main()