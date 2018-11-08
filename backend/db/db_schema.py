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

# Import item files
from nrega_scrape.items import FTOItem
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from common.helpers import sql_connect

# Creates the data-base
def create_db(engine):
	
	# Create engine
	# Create meta-data object
	metadata = MetaData()
	
	# Transactions table
	transactions = Table('transactions', metadata, 
										Column('block_name', String(50)), 
										Column('jcn', String(50)), 
										Column('transact_ref_no', String(50), 
												primary_key = True),
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
										Column('fto_no', String(50)))
	# FTO content table
	wage_list = Table('wage_lists', metadata, 
										Column('wage_list_no', String(50), 
												primary_key = True), 
										Column('block_name', String(50)))
	
	# Accounts table
	# Assumption: Primary account holder name and IFSC code is unique within a JCN and 
	# account number
	accounts = Table('accounts', metadata,
										Column("id", Integer, 
												primary_key = True, 
												autoincrement = True), 
										Column('jcn', String(50)), 
										Column('acc_no', String(50)),
										Column('ifsc_code', String(50)),
										Column('prmry_acc_holder_name', 
												String(50)))
	
	# Banks table
	banks = Table('banks', metadata, 
										Column('ifsc_code', String(50), 
												primary_key = True), 
										Column('bank_code', String(30)))
	
	
	# Create the tables
	metadata.create_all(engine)

# Take the Excel list of FTO nos
# Put it in the SQL data-base using pandas
# Add a column to the Excel file which tells you whether it has been scraped
def put_data(table, engine, path):
    
    data_types = {'fto_no': str, 'scrape_date': str, 'scrape_time': str}
    data = pd.read_excel(path, dtype = data_types).drop_duplicates()
    data.to_sql(table, con = engine, if_exists = 'replace')

# Create a JSON file with keys
# Get the table names and store them as a dictionary
# Now for each table:
# First store the columns and then put them in the table dictionary
# Dump the table dictionary to a JSON file  
def send_keys_to_file(engine):
	
	inspector = reflection.Inspector.from_engine(engine)
	tables = dict.fromkeys(inspector.get_table_names(), '')

	for table in tables:
		tables[table] = [column['name'] for column in inspector.get_columns(table)
						if 'autoincrement' not in column.keys()]
		
	with open('./backend/db/table_keys.json', 'w') as file:
		json.dump(tables, file, sort_keys=True, indent=4)
		
# Execute
if __name__ == '__main__':
	
	# Get values
	# Get engine
	# Create data-base
	# Send keys to file
	user, password, host, db = sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	create_db(engine)
	
	# Send target FTOs to data-base by block
	block_list = ['gwalior']
	paths = [os.path.abspath('./fto_nos/' + block + '.xlsx') for block in block_list]
	
	# Iterate over these two lists and put the tables in the
	# data-base
	for block, path in zip(block_list, paths):
		put_data(block, engine, path)

	# Send keys to file
	send_keys_to_file(engine)