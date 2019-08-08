import pandas as pd
import numpy as np
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *


def get_camp_data(pilot):
	
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = '''SELECT id, phone, jcn, time_pref, time_pref_label, amount, transact_date, 
						rejection_reason, day1 FROM field_data WHERE pilot = {};'''.format(pilot)
	
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


def check_jcn_format_transactions(transactions, df_field):
	'''
	Check and if needed fix format of the JCN.
	'''
	
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(utils.ensure_jcn_format)
	
	return(transactions, df_field)
	

def check_jcn_format_camp(df_field):

	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field['jcn'] = df_field.apply(lambda row: 'CH-033' + row['jcn'][2:] if 'CH-033' not in row['jcn'] and row['jcn'] !='' else '', axis = 1)
	df_field['jcn'] = df_field['jcn'].apply(utils.ensure_jcn_format)
	
	return(df_field)


def merge_field_data(transactions, df_field_data):
	'''Merge the two data-sets: 
	  Get all observations in the merged data-set which are either only in the field data-set or in both the
	  transactions table and the field data-set.
	'''
	
	df_full = pd.merge(transactions, df_field_data, on='jcn', how='outer', indicator=True)
	df_full = df_full.loc[df_full['_merge'] != 'left_only']
	
	df_full = df_full[['jcn', 'transact_date_x', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
					   'status', 'rejection_reason_x', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
					   'time_pref_label', 'amount', 'day1', '_merge']]
	
	df_full.columns = ['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
					   'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
					   'time_pref_label', 'amount', 'day1', '_merge']
	
	if df_full.empty: er.handle_error(error_code ='12', data = {})
	
	return(df_full)


def get_alternate_transactions(local = 1, filepath = './output/transactions_alt.csv'):
	
	if local == 1: 
		
		transactions_alt = pd.read_csv(filepath)
		transactions_alt = transactions_alt[['transact_ref_no', 'status', 'processed_date', 'rejection_reason']]
	
	elif local == 0: 
		
		engine = helpers.db_engine()
		
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
	
	# Transactions alternate
	cols = transactions.columns.tolist()
	cols.remove('transact_ref_no')
	
	transactions = pd.merge(transactions, transactions_alt, how = 'left', on = ['transact_ref_no'], indicator = True)
	transactions['rejection_reason_x'] = transactions['rejection_reason_y']
	transactions['processed_date_x'] = transactions['processed_date_y']
	transactions['status_x'] = transactions['status_y']
	
	transactions.drop(['processed_date_y', 'transact_ref_no', 'status_y', 'rejection_reason_y', '_merge'], inplace = True, axis = 1)
	transactions.columns = cols
	
	return(transactions)


def get_stage_letter(status, stage, merge):
	'''
	This gets called by set_call_script to allocate the sequence of 
	audio file names to each recipient.
	'''
	
	# Store block stages
	block_stages = ["fst_sig_not", "fst_sig", "sec_sig", "sec_sig_not"]
	# Add introduction: All non-matches should get this
	script = 'P0 P1 P2 P3'
	
	if pd.isna(status) and pd.isna(stage) and merge == 'right_only': script += ' Q A'
	# Processed payments should get this
	if status == 'Processed' or stage == 'pb': script += ' R EA EB EC'
	# Rejected payments should get this
	elif status == 'Rejected': script += ' R FA FB FC'
	# Tell those whose payments are pending that they are with the block office
	elif stage in block_stages: script += ' R CA CB CC'
	# Else tell those whose payments are pending that the payments are with the bank
	else: script += ' R DA DB DC'
	# Add the conclusion
	script += ' P0 Z1 Z2'
	
	return(script)


def set_call_script(df):
	'''Set this variable as actual credit amount where it is available else set it to the credit amount due 
	Then give each row in the data-set the calling sequence
	Print the data-frame to the screen as a check
	Return the data-frame'''
	
	df['transact_date'] = np.where(~df['processed_date'].isna(), df['processed_date'], df['transact_date'])
	df['amount'] = np.where(df['credit_amt_actual'] != 0, df['credit_amt_actual'], df['credit_amt_due'])
	
	df['script'] = df.loc[(df['stage'].isna() & df['status'].is.na() & df['_merge'] == 'right_only', 'script'] = 'P0 P1 P2 P3 Q A'
	df['script'] = df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'script'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	
	df['script'] = df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'script'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	df['script'] = df.loc[(df['stage']=='sec_sig_not') | (df['stage']=='sec_sig'), 'script'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
		
	df['script'] = df.loc[(df['stage']=='sb') | (df['stage']=='pp'), 'script'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
	df['script'] = df.loc[(df['stage']=='P'), 'script'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
	
	df['script'] = df.loc[(df['status']=='Processed') & (df['stage']=='pb'), 'script'] = 'P0 P1 P2 P3 R EA EB EC P0 Z1 Z2'
	df['script'] = df.loc[(df['status']=='Rejected') & (df['stage']=='pb'), 'script'] = 'P0 P1 P2 P3 R FA FB FC P0 Z1 Z2'

	# df_full['day1'] = np.vectorize(get_stage_letter)(df_full['status'], df_full['stage'], df_full['_merge'])
	
	return(df_full)


def get_hh_level_data(df):
	'''Get the household totals for the final data-set.'''
	
	# First get household level total amounts
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	# Storing date
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
		
	# We can drop duplicate IDs so we end up with a data-set unique at the ID level
	df = df[['id', 'phone', 'jcn', 'transact_date', 'time_pref', 'time_pref_label', 'amount', 'transact_date', 'rejection_reason', 'day1']] 
	df.drop_duplicates(['id'], inplace = True)
	df.reset_index(inplace = True)
	
	return(df)


def df_shuffle(df):
	
	df = df.sample(frac=1)
	
	return(df) 


def main():
	
	pilot = 0
	local = 1
	window_length = 11
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	local_output_path = './output/script_{}.csv'.format(today)
	s3_output_path = 'scripts/script_{}.csv'.format(today)
	
	transactions_alt = get_alternate_transactions(local = local)
	transactions = get_transactions(start_date, today)
	transactions = add_status_data(transactions, transactions_alt)
	
	df_field = get_field_data_table(pilot)
	transactions, df_field = check_jcn_format(transactions, df_field)
	
	df_full = merge_field_data(transactions, df_field)
	df_full = set_call_script(df_full)
	df_full = get_hh_level_data(df_full)
	df_full.to_csv(local_output_path, index = False)
	

if __name__ == '__main__':
	
	main()

# Pending
# Sort out processed date
# Add in shuffling of data frame
# Add in rejection reasons
# Add in new error messages
# Make this into a routine with command line arguments and adjust the shell script accordingly
# Make helper functions into separate file
# Do Alembic migrations