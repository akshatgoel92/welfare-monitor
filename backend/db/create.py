#--------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Create database tables according to schema
# Contact: akshat.goel@ifmr.ac.in
#--------------------------------------------------------#
from backend.db import schema
from common import helpers
import sys


# Create the DB tables that are listed in the schema file
def db_execute(branch):
	
	engine = helpers.db_engine()

	if branch == 1: schema.create_branch_transactions(engine)
	elif branch == 0: schema.create_bank_transactions(engine)

	schema.create_wage_list(engine)
	schema.create_accounts(engine)
	schema.create_banks(engine)
	
	schema.create_fto_queue(engine)
	schema.create_fto_current_stage(engine)
	schema.send_keys_to_file(engine)


# Create the JSON which stores the stage table names
def stage_names_execute():
	
	engine = helpers.db_engine()
	db_schema.create_stage_table_names()


# Call the functions
def main():

	db_execute()
	stage_names_execute()


if __name__ == '__main__':

	main()