import pandas as pd
import numpy as np
import argparse
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *

# Seed should be in global scope
random.seed(13029)
np.random.seed(110498)


def get_camp_data(pilot):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = '''SELECT id, phone, jcn, time_pref, time_pref_label FROM enrolment_record WHERE pilot = {};'''.format(pilot)
	
	try: 
		
		gens_field = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field = pd.concat([gen for gen in gens_field])
		conn.close()
		
	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		sys.exit()
		conn.close()
		
	return(df_field)


def get_transactions(start_date, end_date):
	
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_joined_tables = '''SELECT a.jcn, a.transact_ref_no, a.transact_date, a.processed_date, a.credit_amt_due, 
						   a.credit_amt_actual, a.status, a.rejection_reason, a.fto_no, b.stage 
						   FROM transactions a INNER JOIN fto_queue b ON a.fto_no = b.fto_no 
						   WHERE a.transact_date BETWEEN '{}' and '{}';'''.format(start_date, end_date)

	try: 
		
		gens_transactions = pd.read_sql(get_joined_tables, con = conn, chunksize = 1000)
		transactions = pd.concat([gen for gen in gens_transactions])	
		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		sys.exit()
		conn.close()

	return(transactions)


def format_transactions_jcn(transactions):
	
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(utils.format_jcn)
	
	return(transactions)
	

def format_camp_jcn(df_field):
		
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field['jcn'] = df_field[['CH-033' not in x for x in df_field['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:] if x != '' else '')
	df_field['jcn'] = df_field['jcn'].apply(utils.format_jcn)
	
	return(df_field)


def get_alternate_transactions(local = 1, filepath = './output/transactions_alt.csv'):
	
	
	if local == 1: 
		
		transactions_alt = pd.read_csv(filepath)
		transactions_alt = transactions_alt[['transact_ref_no', 'status', 'processed_date', 'rejection_reason']]
	
	elif local == 0: 
		
		engine = helpers.db_engine()
		conn = engine.connection()
		
		get_joined_tables = '''SELECT a.jcn, a.transact_ref_no, a.transact_date, a.processed_date, a.credit_amt_due, 
						   	a.credit_amt_actual, a.status, a.rejection_reason, a.fto_no, b.stage 
						   	FROM transactions_alternate a INNER JOIN fto_queue b ON a.fto_no = b.fto_no 
						   	WHERE a.transact_date BETWEEN {} and {};'''.format(start_date, end_date)
		
		try: 
		
			gens_transactions_alt = pd.read_sql(get_joined_tables, con = conn, chunksize = 1000)
			transactions_alt = pd.concat([gen for gen in gens_transactions_alt])	
			conn.close()

		except Exception as e:
		
			er.handle_error(error_code ='5', data = {})
			conn.close()
		
	return(transactions_alt)


def add_status_data(transactions, transactions_alt):
	
	
	cols = transactions.columns.tolist()
	cols.remove('transact_ref_no')
	
	transactions = pd.merge(transactions, transactions_alt, how = 'left', on = ['transact_ref_no'], indicator = True)
	transactions['rejection_reason_x'] = transactions['rejection_reason_y']
	transactions['processed_date_x'] = transactions['processed_date_y']
	transactions['status_x'] = transactions['status_y']
	
	drop_cols = ['processed_date_y', 'transact_ref_no', 'status_y', 'rejection_reason_y', '_merge']
	transactions.drop(drop_cols, inplace = True, axis = 1)
	transactions.columns = cols
	
	return(transactions)


def merge_camp_data(transactions, df_field_data):

	
	df = pd.merge(transactions, df_field_data, on='jcn', how='outer', indicator=True)
	df = df.loc[df['_merge'] != 'left_only']
	
	df = df[['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
			 'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
			 'time_pref_label', '_merge']]
	
	df.columns = ['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
				  'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
				  'time_pref_label', '_merge']
	
	if df.empty: er.handle_error(error_code ='12', data = {})
	
	return(df)


def get_static_script_indicators(df):
	
	# Look-back for welcome script
	df = utils.check_welcome_script(df)
	# Look back for static NREGA introduction
	df = utils.check_static_nrega_script(df)
	# Look back for formatting indicators
	df = utils.format_indicators(df)
	# Return statement
	return(df)


def get_call_script(df):
	
	# Allocate static scripts
	df = utils.set_static_scripts(df)
	# Allocate dynamic scripts
	df = utils.set_nrega_scripts(df)
	# Aggregate amounts to household level
	df = utils.set_nrega_hh_amounts(df)
	# Aggregate dates to household level
	df = utils.set_nrega_hh_dates(df)
	# Allocate rejection reason - still need to complete
	df = utils.set_nrega_rejection_reason(df)
	# Keep only columns that are relevant to BTT
	df = utils.format_df(df)
	# Add test calls 
	df = utils.add_test_calls(df)
	# Resturn statement
	return(df)

	
def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('pilot', type = int, help = 'Whether to make script for pilot data or production data')
	parser.add_argument('local', type = int, help ='Whether to get processed data from local')
	parser.add_argument('window_length', type = int, help ='Time window in days from today for NREGA lookback')
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	pilot = args.pilot
	local = args.local
	
	# Set window lengths
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	# Set output paths
	local_output_path = './output/callsequence_{}.csv'.format(today)
	merge_output_path = './output/nregamerge_{}.csv'.format(today)
	s3_output_path = 'scripts/callsequence_{}.csv'.format(today)
	
	# Get transactions and camp data
	transactions_alt = get_alternate_transactions(local = local)
	transactions = get_transactions(start_date, today)
	camp = get_camp_data(pilot)
	
	# Add payment status data
	transactions = add_status_data(transactions, transactions_alt)
	
	# Format JCNs to prepare for merge
	transactions = format_transactions_jcn(transactions)
	camp = format_camp_jcn(camp)
	
	# Merge data and create call script
	df_merged = merge_camp_data(transactions, camp)
	df = get_static_script_indicators(df_merged)
	df = get_call_script(df)
	
	# Output the .csvs and send them to S3
	df_merged.to_csv(merge_output_path, index = False)
	df.to_csv(local_output_path, index = False)
	

if __name__ == '__main__':
	
	main()
		
# Pending
# Ask about 5 people who got static NREGA instead of welcome script
# Change file name convention to call date
# Check why nobody got A in this one
# Add in new error messages
# Do Alembic migrations
# Add rejection reason
# Add update e-mails
# Add test calls