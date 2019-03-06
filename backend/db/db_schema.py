# Import packages
import os
import json 

# Import SQL modules
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import *
from sqlalchemy.engine import reflection
pymysql.install_as_MySQLdb()

# Import date and time
from datetime import datetime
from common import helpers

# Creates the data-base
def create_transactions(engine):
	
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

	metadata = MetaData()

	stage = Table(stage, metadata, 
						Column('state_code'),
						Column('district_code'), 
						Column('block_code'), 
						Column('fto_no'), 
						Column('fto_stage'),
						Column('transact_date'),
						Column('scrape_date'), 
						Column('scrape_time'), 
						Column('url'))

	metadata.create_all(engine)


def create_fto_queue(engine):

	metadata = MetaData()

	fto_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('done', Integer()), 
										Column('fto_type', String(20)))


	metadata.create_all(engine)

  
def get_table_names(engine):

	inspector = reflection.Inspector.from_engine(engine)

	tables = dict.fromkeys(inspector.get_table_names(), '')

	return(inspector, tables)


def check_table_empty(conn, table):

	# MySQL uses an arbitrary index 
	# Exists return 1 if the row exists else 0
	# We take that and store it in not_empty	
	is_empty = 1 - int(conn.execute("SELECT EXISTS (SELECT 1 FROM " + table))

	return(is_empty)
 

def send_keys_to_file(inspector, tables):
	
	for table in tables:
		
		tables[table] = [column['name'] for column in inspector.get_columns(table) 
						if 'autoincrement' not in column.keys()]
		
	with open('./backend/db/table_keys.json', 'w') as file:
		
		json.dump(tables, file, sort_keys = True, indent = 4)



