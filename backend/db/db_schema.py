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

# Import other modules
from common import helpers

# Creates the data-base
def create_db(engine):
	
	metadata = MetaData()
	
	# Transactions table
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
	# FTO content table
	wage_list = Table('wage_lists', metadata, 
										Column('wage_list_no', String(50), primary_key = True), 
										Column('block_name', String(50)))
	
	# Accounts table
	# Assumption: Primary account holder name and IFSC 
	# code is unique within a JCN and account number
	accounts = Table('accounts', metadata, 
										Column('jcn', String(50), primary_key = True), 
										Column('acc_no', String(50), primary_key = True),
										Column('ifsc_code', String(50), primary_key = True),
										Column('prmry_acc_holder_name', String(50)))
	
	# Banks table
	banks = Table('banks', metadata, 
										Column('ifsc_code', String(50), primary_key = True), 
										Column('bank_code', String(30)))


	fto_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('done', Integer()), 
										Column('fto_type', String(20)))
	
	# Create the tables
	metadata.create_all(engine)
  
def send_keys_to_file(engine):
	
	inspector = reflection.Inspector.from_engine(engine)
	
	tables = dict.fromkeys(inspector.get_table_names(), '')

	for table in tables:
		
		tables[table] = [column['name'] for column in inspector.get_columns(table) if 'autoincrement' not in column.keys()]
		
	with open('./backend/db/table_keys.json', 'w') as file:
		
		json.dump(tables, file, sort_keys=True, indent=4)
		
if __name__ == '__main__':
	
	# Create the DB engine here
	# Then create the data-base using the schema defined above
	user, password, host, db = helpers.sql_connect().values()
	
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	create_db(engine)
	
	send_keys_to_file(engine)