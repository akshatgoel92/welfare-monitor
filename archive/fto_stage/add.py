from sqlalchemy import *
from sqlalchemy.engine import reflection
from common import helpers

import sys
import os
import pandas as pd
import numpy as np
import pymysql
import argparse

pymysql.install_as_MySQLdb()


# Now we get the new FTOs along with their current stage
def get_new_ftos(engine, fto_stages, file_to):
	
	fto_queue = update.select_data(engine, 'fto_queue', cols = ['fto_no'])
	new_ftos = update.anti_join(fto_stages, fto_queue, on = ['fto_no'])
	new_ftos.to_csv(file_to, index = False)


# Put the new FTO nos. in the queue
def put_fto_nos(engine, path, if_exists):
	
    fto_nos = pd.read_csv(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql('fto_queue', con = engine, index = False, if_exists = if_exists, chunksize = 100,
    			   dtype = {'fto_no': String(100), 'fto_type': String(15), 'done': SmallInteger(), 
    			   			'stage': String(15)})


# Update the FTO queue table
# This can be optimized using executemany or to_sql
# When optimizing make sure that we are handling: 
# 1) updates to stage of existing FTOs
# 2) case where connection to database drops during insert 
def insert_ftos(engine, fto_stages, test):

	sql = update.upsert_data('fto_queue', ['current_stage'])	
	conn = engine.connect()
	trans = conn.begin()

	try:
		
		fto_stages.to_sql('fto_queue', con = engine, index = False, if_exists = 'replace', chunksize = 100,
						  dtype = {'fto_no': String(100), 'fto_type': String(15), 'done': SmallInteger(), 
						  			'current_stage': String(15)})
					
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


def main():

	parser = argparse.ArgumentParser(description = 'Append to or replace the queue?')
	parser.add_argument('if_exists', type = str, help = 'Append or replace?')

	args = parser.parse_args()
	if_exists = args.if_exists
	path = os.path.abspath('./output/fto_queue.csv')
	
	engine = helpers.db_engine()
	put_fto_nos(engine, path, if_exists)

if __name__ == '__main__':

	main()