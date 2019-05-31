import os
import json 
import pandas as pd
import numpy as np
import pymysql

from sqlalchemy import *
from sqlalchemy.engine import reflection
from sqlalchemy.schema import Index
from datetime import datetime
from common import helpers

pymysql.install_as_MySQLdb()


def create_bank_transactions(engine):
	
	metadata = MetaData()
	transactions = Table('transactions', metadata, 
										Column('block_name', String(50)), 
										Column('jcn', String(50)), 
										Column('transact_ref_no', String(50), primary_key = True),
										Column('transact_date', String(50)),
										Column('app_name', String(50)),
										Column('wage_list_no', String(50)),
										Column('acc_no', String(50)),
										Column('ifsc_code', String(50)),
										Column('credit_amt_due', Integer),
										Column('credit_amt_actual', Integer),
										Column('status', String(50)),
										Column('processed_date', String(50)),
										Column('utr_no', String(50)), 
										Column('rejection_reason', String(50)),
										Column('fto_no', String(50)), 
										Column('scrape_date', String(50)),
										Column('scrape_time', String(50)))

	metadata.create_all(engine)


def create_branch_transactions(engine):
	
	# The branch scrape does not have bank account numbers
	metadata = MetaData()
	transactions = Table('transactions', metadata, 
										Column('block_name', String(50)), 
										Column('jcn', String(50)), 
										Column('transact_ref_no', String(50), primary_key = True),
										Column('transact_date', String(50)),
										Column('app_name', String(50)),
										Column('wage_list_no', String(50)),
										Column('ifsc_code', String(50)),
										Column('credit_amt_due', Integer),
										Column('credit_amt_actual', Integer),
										Column('status', String(50)),
										Column('processed_date', String(50)),
										Column('utr_no', String(50)), 
										Column('rejection_reason', String(50)),
										Column('fto_no', String(50)), 
										Column('scrape_date', String(50)),
										Column('scrape_time', String(50)))

	metadata.create_all(engine)


def create_wage_list(engine):

	metadata = MetaData()
	wage_list = Table('wage_lists', metadata, 
										Column('wage_list_no', String(50), primary_key = True), 
										Column('block_name', String(50)))

	metadata.create_all(engine)


def create_accounts(engine):
	
	metadata = MetaData()
	accounts = Table('accounts', metadata, 
										Column('jcn', String(50), primary_key = True), 
										Column('acc_no', String(50), primary_key = True),
										Column('ifsc_code', String(50), primary_key = True),
										Column('prmry_acc_holder_name', String(50)))

	metadata.create_all(engine)


def create_banks(engine):
	
	metadata = MetaData()
	banks = Table('banks', metadata, 
										Column('ifsc_code', String(50), primary_key = True), 
										Column('bank_code', String(30)))

	metadata.create_all(engine)


def create_stage(engine, stage):

	tables = load_stage_table_names()
	metadata = MetaData()
	stage = Table(tables[stage], metadata, 
						Column('state_code', BigInteger()),
						Column('district_code', BigInteger()), 
						Column('block_code', BigInteger()), 
						Column('fto_no', String(100)),
						Column('transact_date', String(20)),
						Column('scrape_date', String(20)), 
						Column('scrape_time', String(20)), 
						Column('url', String(500)))

	metadata.create_all(engine)


def create_fto_queue(engine):

	metadata = MetaData()
	fto_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('done', Integer()), 
										Column('fto_type', String(20)),
										Column('current_stage', String(20)))
	
	metadata.create_all(engine)


def create_fto_current_stage(engine):

	metadata = MetaData()
	fto_stage = Table('fto_stage', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('current_stage', Integer()),
										Column('updated_at', String(20)))
	metadata.create_all(engine)


def create_audit_trigger(trigger_name, old_table_action, old_table, audit_table):

	create_trigger = '''
	
	CREATE TRIGGER {} BEFORE {} ON {}
	FOR EACH ROW BEGIN
	INSERT INTO {} () VALUES ();
	END; '''.format(trigger_name, old_table_action, old_table, audit_table)
	
	return(create_trigger)


def create_audit_table(engine, old_table):

	audit_table = old_table + '_history'
	create_table = "CREATE TABLE {} LIKE {};".format(audit_table, old_table)
	drop_old_primary_key = "ALTER TABLE {} DROP PRIMARY KEY;".format(audit_table)
	add_id_column = "ALTER TABLE {} ADD COLUMN {} INT UNSIGNED NOT NULL;".format(audit_table, 'history_id')
	add_primary_key = "ALTER TABLE {} ADD CONSTRAINT PRIMARY KEY {};".format(audit_table, 'history_id')
	make_auto_increment = "ALTER TABLE {} MODIFY {} INT UNSIGNED NOT NULL AUTO_INCREMENT;".format(audit_table, 'history_id')

	conn = engine.connect()
	trans = conn.begin()

	try: 

		conn.execute(create_table)
		conn.execute(drop_old_primary_key)
		conn.execute(add_id_column)
		
		conn.execute(add_primary_key)
		conn.execute(make_auto_increment)
		conn.execute(add_trigger)
		
		trans.commit()

	except Exception as e:
		
		trans.rollback()		
		print(e)
		conn.close()

	return(transactions, banks, accounts)


def get_table_names(engine):

	inspector = reflection.Inspector.from_engine(engine)
	tables = inspector.get_table_names()

	return(tables)


def create_stage_table_names():

	# fst_sig: First sign
	# fst_sig_not: First sign pending 
	# sec_sig: Second sign 
	# sec_sig_not: Second sign pending 
	# sb: Sent to bank 
	# pp: Partially processed by bank 
	# pb: Processed by bank 
	# P: Pending for bank bank

	tables = ['fst_sig_not', 'fst_sig', 'sec_sig_not', 'sec_sig', 'sb', 'P', 'pp', 'pb']

	with open('./backend/db/stage_names.json', 'w') as file:

		json.dump(tables, file, sort_keys = True, indent = 4)

	return


def load_stage_table_names():

	with open('./backend/db/stage_names.json') as file:
		
		tables = json.load(file)

	return(tables)

 
def send_keys_to_file(engine):
	
	inspector = reflection.Inspector.from_engine(engine)
	tables = dict.fromkeys(inspector.get_table_names(), '')

	for table in tables:
		
		tables[table] = [column['name'] for column in inspector.get_columns(table)]
						
	with open('./backend/db/table_keys.json', 'w') as file:
		
		json.dump(tables, file, sort_keys = True, indent = 4)

	return