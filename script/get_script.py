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
np.random.seed(110498)
random.seed(13029)


def get_camp_data(pilot):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = '''SELECT id, phone, jcn, time_pref, time_pref_label FROM enrolment_record WHERE pilot = {};'''.format(pilot)
	
	try: 
		
		gens_field = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field = pd.concat([gen for gen in gens_field])
		conn.close()
		
	except Exception as e:
		
		er.handle_error(error_code ='26', data = {})
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
		
		er.handle_error(error_code ='27', data = {})
		sys.exit()
		conn.close()

	return(transactions)


def get_alternate_transactions(local = 1, filepath = './output/transactions_alt.csv'):
	
	transactions_alt = pd.read_csv(filepath)
	transactions_alt = transactions_alt[['transact_ref_no', 'status', 'processed_date', 'rejection_reason']]
			
	return(transactions_alt)


def format_transactions_jcn(transactions):
	
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(utils.format_jcn)
	
	return(transactions)
	

def format_camp_jcn(df_field):
		
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field['jcn'] = df_field[['CH-033' not in x for x in df_field['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:] if x != '' else '')
	df_field['jcn'] = df_field['jcn'].apply(utils.format_jcn)
	
	return(df_field)


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
	
	if df.empty: er.handle_error(error_code ='29', data = {})
	
	return(df)


def set_static_scripts(df):
			
	# Not got welcome script and not got static NREGA introduction so should get the welcome script
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 0), 'day1'] = "P0 P1 P2 00 P0"
	
	# Got welcome script but not got static NREGA introduction so should get static NREGA introduction
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 1), 'day1'] = "P0 P1 P2 P3 Q A P0 Z1 Z2"
	
	# Proportional wages
	df.loc[(df['got_static_nrega'] == 1), 'day1'] = 'P0 P1 P2 P3 Q B P0 Z1 Z2'
	
	return(df)


def set_nrega_scripts(df):
	
	# Dynamic NREGA scripts for FTOs at the block office
	df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	df.loc[(df['stage']=='sec_sig_not') | (df['stage']=='sec_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	
	# Dynamic NREGA scripts for unprocessed FTOs at the bank
	df.loc[(df['stage']=='sb') | (df['stage']=='pp') | (df['stage']=='P'), 'day1'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
		
	# Dynamic NREGA scripts for transactions which have been processed
	df.loc[(df['status']=='Processed') & (df['stage']=='pb'), 'day1'] = 'P0 P1 P2 P3 R EA EB EC P0 Z1 Z2'
	df.loc[(df['status']=='Rejected') & (df['stage']=='pb'), 'day1'] = 'P0 P1 P2 P3 R FA FB FC P0 Z1 Z2'
	
	return(df)


def set_nrega_hh_amounts(df):
	
	# Replace amount with actual amount if it exists else with the amount due
	df['amount'] = np.where(df['credit_amt_actual'] != 0, df['credit_amt_actual'], df['credit_amt_due'])
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	return(df)
	
	
def set_nrega_hh_dates(df):
	
	# Replace transact_date with processed_date if transaction has been processed and then format
	df['transact_date'] = np.where(~df['processed_date'].isna(), df['processed_date'], df['transact_date'])
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	return(df)


def get_static_script_look_backs(df):
	
	# Look-back for welcome script
	df = utils.check_welcome_script(df)
	# Look back for static NREGA introduction
	df = utils.check_static_nrega_script(df)
	# Look back for formatting indicators
	df = utils.format_indicators(df)
	# Return statement
	return(df)


def get_static_call_script(df):
	
	# Initialize script
	df['amount'] = ''
	df['day1'] = ''
	df['transact_date'] = ''
	df['rejection_reason'] = ''
	# Allocate static scripts
	df = utils.set_static_scripts(df)
	# Keep only columns that are relevant to BTT in static data
	df = utils.format_df(df, 1)
	# Add test calls to static script
	df = utils.add_test_calls(df)
		
	return(df)


def get_dynamic_call_script(df):
	
	# Initialize script
	df['day1'] = ''
	# Separate into dynamic
	df = df.loc[df['_merge'] == 'both']
	# Allocate dynamic scripts
	df = utils.set_nrega_scripts(df)
	# Aggregate amounts to household level
	df = utils.set_nrega_hh_amounts(df)
	# Aggregate dates to household level
	df = utils.set_nrega_hh_dates(df)
	# Allocate rejection reason - still need to complete
	df = utils.set_nrega_rejection_reason(df)
	# Keep only columns that are relevant to BTT in dynamic data
	df = utils.format_df(df, 0)
	# Add test calls to dynamic script
	df = utils.add_test_calls(df)
	
	return(df)


def send_update_email():
	
	helpers.send_mail('GMA Update: Finished executing the script creation. Please check the previous mails for any errors.')
	
	return
	
def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('window_length', type = int, help ='Time window in days from today for NREGA lookback')
	parser.add_argument('pilot', type = int, help = 'Whether to make script for pilot data or production data')
	parser.add_argument('dynamic', type = int, help ='Whether to make dynamic script')
	parser.add_argument('static', type = int, help ='Whether to make static script')
	parser.add_argument('local', type = int, help ='Get transactions from local')
	parser.add_argument('join', type = int, help ='Whether to join the two or not...')
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	dynamic = args.dynamic
	static = args.static
	pilot = args.pilot
	local = args.local
	
	# Set window lengths
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	# Set output paths
	local_output_path = './output/callsequence_{}.csv'.format(today)
	merge_output_path = './output/nregamerge_{}.csv'.format(today)
	s3_output_path = 'scripts/callsequence_{}.csv'.format(today)
	
	# Prepare camp data
	camp = get_camp_data(pilot)
	camp = format_camp_jcn(camp) 
	
	# Get scripts
	if static == 1:
		df_static = get_static_script_look_backs(camp)
		df_static = get_static_call_script(df_static)
		print('Static script done!')
	
	if dynamic == 1:
		# Prepare transactions data
		transactions_alt = get_alternate_transactions(local = local)
		transactions = get_transactions(start_date, today)
	
		# Add additional data about processed payments and format
		transactions = add_status_data(transactions, transactions_alt)
		transactions = format_transactions_jcn(transactions)
		
		# Merge camp and transactions data 
		df_merged = merge_camp_data(transactions, camp)
		df_dynamic = get_dynamic_call_script(df_merged)
		

if __name__ == '__main__':
	
	main()		

# Pending
# Add rejection reason
# Add output function call and S3 upload S3 upload
# Change file name convention to call date
# Do Alembic migrations