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

#-------------------------#
# Backend
#-------------------------#
pymysql.install_as_MySQLdb()

#-------------------------------------#
# Make stage table
#-------------------------------------#
def make_stage_tables(engine, stages, cols): 

	conn = engine.connect()
	trans = conn.begin()
	
	all_tables = db_schema.get_table_names(conn)
	stage_tables = [stage_table for stage_table in all_tables if stage_table in stages]

	if len(stage_tables) > 0:
		empty_tables = [db_schema.check_table_empty(conn, stage) for stage in stage_tables]

	elif len(stage_tables) == 0:
		empty_tables = [1]*8

	for stage, is_empty in zip(stages, empty_tables):
		if stage not in stage_tables or is_empty == 1:
			try: 
				db_schema.create_stage(conn, stage)
			except Exception as e:
				print(e)
				print('Error making the stage table for...:' + stage)
				trans.rollback()
				sys.exit()

	trans.commit()


#-------------------------------------#
# Update stage tables
#-------------------------------------#
def update_stage_tables(engine, stages, cols):

	conn = engine.connect()
	trans = conn.begin()

	for stage in stages:
		
		try: 
			
			scraped_ftos = pd.read_csv('./output/' + stage + '.csv')
			scraped_ftos['transact_date'] = scraped_ftos['transact_date'].astype(object)
			existing_ftos = pd.read_sql("SELECT * FROM " + stage, con = conn)
			new_ftos = pd.merge(scraped_ftos, existing_ftos, how = 'outer', indicator = True)
			new_ftos = new_ftos.loc[new_ftos['_merge'] == 'left_only']
			new_ftos.drop(['_merge'], inplace = True, axis = 1)
			new_ftos.to_sql(stage, con = conn, index = False, if_exists = 'append', chunksize = 1000)
			print('Done...:' + stage)
		
		except pd.errors.EmptyDataError as e:
			print(e)
			print('This ' + stage + '.csv does not have any data....!')
			continue
		
		except Exception as e:
			print(e)
			print('There was an uncaught error in the creation of the SQL table for stage: ' + stage)
			trans.rollback()
			sys.exit()
	
	trans.commit()


#-------------------------------------#
# Update current stage table
#-------------------------------------#
def update_current_stage_table(engine, stages):

	conn = engine.connect()
	trans = conn.begin()

	try:
		
		fto_stages = pd.concat([pd.read_sql("SELECT fto_no FROM " + stage + ";", con = conn) for stage in stages])
		fto_stages = fto_stages.groupby(['fto_no']).size().reset_index()
		fto_stages.columns = ['fto_no', 'fto_stage']
		fto_stages.to_sql('fto_current_stage', con = conn, index = False, if_exists = 'replace')

	except Exception as e:

		print(e)
		trans.rollback()
		sys.exit()

	trans.commit()

	return(fto_stages)


#-------------------------------------------------#
# Get the new FTOs which have not been scraped yet
#-------------------------------------------------#
def get_new_ftos(engine): 

	conn = engine.connect()

	try: 
		
		current_stage = pd.read_sql("SELECT fto_no FROM fto_current_stage", con = conn)
		fto_queue = pd.read_sql("SELECT * FROM fto_queue", con = conn) 
		
		new_ftos = pd.merge(current_stage, fto_queue, how = 'outer', indicator = True)
		new_ftos = new_ftos.loc[new_ftos['_merge'] == 'left_only']
		new_ftos.drop(['_merge'], inplace = True, axis = 1)
		new_ftos.to_csv('./output/fto_queue.csv', index = False)

	except Exception as e:
		print(e)

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
	
	stages = ['fst_sig', 'fst_sig_not', 'sec_sig', 
			  'sec_sig_not', 'sb', 'pp', 'pb', 'P'] 
		
	cols = ['state_code', 'district_code', 'block_code', 
			'fto_no', 'fto_stage', 'transact_date', 
			'scrape_date', 'scrape_time', 'url']

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" 
							+ password + "@" + host + "/" + db)

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