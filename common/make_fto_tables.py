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
# Make stage table
#-------------------------------------#
def make_stage_tables(conn, stages, cols): 

	get_tables = "SELECT table_name FROM information_schema.tables"
	tables = [stage for stage in pd.read_sql(get_tables, con = conn).values.tolist() if stage in stages]
	empty_tables = [db_schema.check_table_empty(conn, stage) for stage in tables]

	for stage, is_empty in zip(stages, empty_tables):
		
		if stage not in tables or is_empty == 1:
			try: 
				
				db_schema.create_stage(engine, stage)

			except Exception as e:
				
				print(e)
				print('Error making the stage table for...:' + stage)
				sys.exit(10000)


def update_stage_tables(conn, stages, cols):

	for stage in stages:

		try: 
				
			scraped_ftos = pd.read_csv('./output/' + stage + '.csv')
			existing_ftos = pd.read_sql("SELECT * FROM " + stage, con = conn)
			new_ftos = pd.merge(scraped_ftos, existing_ftos, how = 'left')
			new_ftos.to_sql(stage, con = conn, index = False, if_exists = 'append', chunksize = 1000)
			print('Done...' + stage)

		except pd.errors.EmptyDataError as e:

			print(e)
			print('This ' + stage + '.csv does not have any data....!')
			continue

		except Exception as e:

			print(e)
			print('There was an uncaught error in the creation of the SQL table for stage: ' + stage)
			continue


def update_current_stage_table(stages, conn):

	pass

def update_fto_queue(conn): 

	fto_queue = pd.read_sql("SELECT * FROM fto_queue", con = conn) 
	current_stage = pd.read_sql("SELECT * FROM current_stage", con = conn)
	new_ftos = pd.merge(fto_queue, current_stage, how = 'right', on = 'fto_no')
	new_ftos.to_sql(fto_queue, if_exists = 'append', con = conn, chunksize = 100)


def main(): 
	
	stages = ['fst_sig', 'fst_sig_not', 'sec_sig', 'sec_sig_not', 
			  'sb', 'pp', 'pb', 'P'] 
		
	cols = ['state_code', 'district_code', 'block_code', 'fto_no', 
			'fto_stage', 'transact_date', 'scrape_date', 'scrape_time', 
			'url']

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()
	trans = conn.begin()
	
	try:

		make_stage_tables(conn, stages, cols)
		make_current_stage_table(con, stages, cols) 
		trans.commit()

	except Exception as e:

		print(e)
		trans.rollback()


if __name__ == "__main__": 

	main()