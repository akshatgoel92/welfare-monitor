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
from sqlalchemy.schema import Index
from datetime import datetime
from common import helpers

#------------------------#
# Use PyMySQL
#------------------------# 
pymysql.install_as_MySQLdb()

#---------------------------------------------------#
# Creates the bank account source transactions table
#---------------------------------------------------#
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


#-------------------------------------------------#
# Creates the transactions table for branch scrape
# The branch scrape does not have bank account numbers
#-------------------------------------------------#
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
# Create the banks table
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


#----------------------------#
# Creates the FTO queue table
#----------------------------#
def create_fto_queue(engine):

	metadata = MetaData()
	fto_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('done', Integer()), 
										Column('fto_type', String(20)), 
										Column('scrape_date', String(20)), 
										Column('scrape_time', String(20)))


	metadata.create_all(engine)


#------------------------------------#
# Creates the FTO current stage table
#------------------------------------#
def create_fto_current_stage(engine):

	metadata = MetaData()
	fto_stage = Table('fto_stage', metadata, 
										Column('fto_no', String(50), primary_key = True),
										Column('current_stage', Integer()),
										Column('updated_at', String(20)))
	metadata.create_all(engine)



#-----------------------------------------------#
# Make primary key
#-----------------------------------------------#
def create_primary_key(engine, table, key):

	engine.execute('ALTER TABLE ' + table + ' ADD PRIMARY KEY(' + key + ')')

	return


#-----------------------------------------------#
# Add an index
#-----------------------------------------------#
def make_index(engine, table, col, name):

	index = Index(name, table.c.col)
	index.create(engine)

	return

#----------------------------------#
# Creates a list of the table names
#----------------------------------#  
def get_table_names(engine):

	inspector = reflection.Inspector.from_engine(engine)
	tables = inspector.get_table_names()

	return(tables)

def create_stage_table_names():

	tables = {'fst_sig': 'fto_fst_sig', 'fst_sig_not': 'fto_fst_sig_not', 'sec_sig': 'fto_sec_sig', 
						 'sec_sig_not': 'fto_sec_sig_not', 'sb': 'fto_sent_to_bank', 
						 'pp': 'fto_partial_processed_bank', 'pb': 'fto_processed', 
						 'P': 'fto_pending_bank'}

	with open('./backend/db/stage_table_names.json', 'w') as file:

		json.dump(tables, file, sort_keys = True, indent = 4)

	return


#----------------------------------------------#
# Create a .json file with stage table names
#----------------------------------------------#
def load_stage_table_names():

	with open('./backend/db/stage_table_names.json') as file:
		
		tables = json.load(file)

	return(tables)

 
#------------------------#
# Send keys to file
#------------------------#
def send_keys_to_file(engine):
	
	inspector = reflection.Inspector.from_engine(engine)
	tables = dict.fromkeys(inspector.get_table_names(), '')

	for table in tables:
		
		tables[table] = [column['name'] for column in inspector.get_columns(table)]
						
	with open('./backend/db/table_keys.json', 'w') as file:
		
		json.dump(tables, file, sort_keys = True, indent = 4)

	return






