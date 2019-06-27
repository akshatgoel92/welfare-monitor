import sys
from db import schema
from common import helpers


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


def stage_names_execute():
	
	engine = helpers.db_engine()
	db_schema.create_stage_table_names()


def main():

	db_execute()
	stage_names_execute()


if __name__ == '__main__':

	main()