'''
	Author: Akshat Goel
	Purpose: Create MySQL database tables according to schema file
	Contact: akshat.goel@ifmr.ac.in
'''

from backend.db import schema
from common import helpers
import sys


def db_execute(branch):
	# Create the DB tables that are listed in the schema file.

	engine = helpers.db_engine()

	if branch == 1: schema.create_branch_transactions(engine)
	elif branch == 0: schema.create_bank_transactions(engine)

	schema.create_wage_list(engine)
	schema.create_accounts(engine)
	schema.create_banks(engine)
	
	schema.create_fto_queue(engine)
	schema.create_fto_current_stage(engine)
	schema.send_keys_to_file(engine)

	return


def stage_names_execute():
	# Create the JSON which stores the stage table names.

	engine = helpers.db_engine()
	db_schema.create_stage_table_names()

	return


def stage_tables_execute():
	# Create stage wise tables.

	stages = db_schema.load_stage_table_names()
	engine = helpers.db_engine() 
	conn = engine.connect()
	trans = conn.begin()
	
	for stage in stages:
		
		try: schema.create_stage(conn, stage)
		
		except Exception as e:
				
			print(e)
			print('Error making the stage table for...:' + stage)
			trans.rollback()
			sys.exit()

	trans.commit()

	return


def primary_key_execute():
	# This adds primary keys to the specified variables.

	engine = helpers.db_engine()
	schema.make_primary_key(engine, 'banks', 'ifsc_code')

	return


def main():

	db_execute()
	stage_names_execute()
	stage_tables_execute()

	return


if __name__ == '__main__':

	main()