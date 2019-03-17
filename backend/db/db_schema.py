#------------------------#
# Import packages
#------------------------#
import os
import json 
import pandas as pd
import numpy as np
import pymysql

from sqlalchemy import *
from sqlalchemy.engine import reflection
from datetime import datetime
from common import helpers

#------------------------#
# Use PyMySQL
#------------------------# 
pymysql.install_as_MySQLdb()

#-------------------------------#
# Creates the transactions table
#-------------------------------#
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

#-----------------------------#
# Creates the wage list table
#-----------------------------#
def create_wage_list(engine):

	metadata = MetaData()

	wage_list = Table('wage_lists', metadata, 
										Column('wage_list_no', String(50), primary_key = True), 
										Column('block_name', String(50)))

	metadata.create_all(engine)

#---------------------------#
# Creates the accounts table
#---------------------------#
def create_accounts(engine):
	
	metadata = MetaData()

	accounts = Table('accounts', metadata, 
										Column('jcn', String(50), primary_key = True), 
										Column('acc_no', String(50), primary_key = True),
										Column('ifsc_code', String(50), primary_key = True),
										Column('prmry_acc_holder_name', String(50)))

	metadata.create_all(engine)


#------------------------#
# Create tha banks table
#------------------------#
def create_banks(engine):
	
	metadata = MetaData()

	banks = Table('banks', metadata, 
										Column('ifsc_code', String(50), primary_key = True), 
										Column('bank_code', String(30)))

	metadata.create_all(engine)


#------------------------#
# Create the stage table
#------------------------#
def create_stage(engine, stage):

	metadata = MetaData()

	stage = Table(stage, metadata, 
						Column('state_code', BigInteger()),
						Column('district_code', BigInteger()), 
						Column('block_code', BigInteger()), 
						Column('fto_no', String(100)),
						Column('transact_date', String(20)),
						Column('scrape_date', String(20)), 
						Column('scrape_time', String(20)), 
						Column('url', String(500)))

	metadata.create_all(engine)

#----------------------------#
# Creates the FTO queue table
#----------------------------#
def create_fto_queue(engine):

	metadata = MetaData()

	fto_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('done', Integer()), 
										Column('fto_type', String(20)))


	metadata.create_all(engine)

#----------------------------------#
# Creates a list of the table names
#----------------------------------#  
def get_table_names(engine):

	inspector = reflection.Inspector.from_engine(engine)
	
	tables = inspector.get_table_names()

	return(tables)

#----------------------------------------#
# Check if a given table is empty
# MySQL uses an arbitrary index 
# Exists return 1 if the row exists else 0
# We take that and store it in not_empty	
#----------------------------------------#
def check_table_empty(conn, table):

	result = pd.read_sql('SELECT EXISTS ' + '(SELECT 1 FROM ' + table + ')', con = conn)

	return(result.values.tolist()[0])


#-----------------------------------------------#
# Return only those entries in df_1 not in df_2 	
#-----------------------------------------------#
def left_join(df_1, df_2, on):

	df = pd.merge(df_1, df_2, how = 'outer', on = on, indicator = 'True')

	df = df.loc[df['_merge'] == 'left_only']

	df.drop(['_merge'], inplace = True)

	return(df)

#-----------------------------------------------#
# Add a trigger	
#-----------------------------------------------#
def make_trigger():


	pass


#-----------------------------------------------#
# Add an index
#-----------------------------------------------#
def make_index():

	pass


#-----------------------------------------------#
# Make primary key
#-----------------------------------------------#
def make_primary_key():

	pass


#-----------------------------------------------#
# Check data types same
#-----------------------------------------------#
def check_data_types():

	pass



#---------------------------------------------------------------------# 
# Insert a new item into the SQL data-base
#---------------------------------------------------------------------# 	
def insert_data(item, keys, table, unique = 0):

	#---------------------------------------------------------------------# 
	# Only insert fields which are both in the item and the table
	#---------------------------------------------------------------------# 	
	keys = get_keys(table) & item.keys()
	fields = u','.join(keys)
	
	qm = u','.join([u'%s'] * len(keys))
	sql = "INSERT INTO " + table + " (%s) VALUES (%s)"
	sql_unique = "INSERT IGNORE INTO " + table + " (%s) VALUES (%s)"
	
	insert = sql if unique == 0 else sql_unique
	sql = insert % (fields, qm)
	data = [item[k] for k in keys]

	return(sql, data)

#---------------------------------------------------------------------# 
# Update FTO type
#---------------------------------------------------------------------# 
def update_fto_type(fto_no, fto_type, table):

	sql = "UPDATE " + table + " SET fto_type = %s WHERE fto_no = %s"
	data = [fto_type, fto_no]
	return(sql, data)

 
#------------------------#
# Send keys to file
#------------------------#
def send_keys_to_file(engine, tables):
	
	inspector = reflection.Inspector.from_engine(engine)
	
	tables = dict.fromkeys(inspector.get_table_names(), '')

	for table in tables:
		
		tables[table] = [column['name'] for column in inspector.get_columns(table) 
						if 'autoincrement' not in column.keys()]
		
	with open('./backend/db/table_keys.json', 'w') as file:
		
		json.dump(tables, file, sort_keys = True, indent = 4)



