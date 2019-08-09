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


def format_transactions_jcn(transactions, df_field):
	
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(utils.format_jcn)
	
	return(transactions, df_field)
	

def format_camp_jcn(df_field):
	
	def fill_ch(row):
	
		if 'CH-033' not in row['jcn'] and row['jcn'] !='': row['jcn'] = 'CH-033' + row['jcn'][2:]  
		else: row['jcn'] = ''
		
		return(row)
	
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field['jcn'] = df_field.apply(fill_ch, axis = 1)
	df_field['jcn'] = df_field['jcn'].apply(utils.format_jcn)
	
	return(df_field)


def merge_camp_data(transactions, df_field_data):

	
	df = pd.merge(transactions, df_field_data, on='jcn', how='outer', indicator=True)
	df = df.loc[df_full['_merge'] != 'left_only']
	
	df = df[['jcn', 'transact_date_x', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
			 'status', 'rejection_reason_x', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
			 'time_pref_label', 'amount', 'day1', '_merge']]
	
	df.columns = ['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
				  'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
				  'time_pref_label', 'amount', 'day1', '_merge']
	
	if df.empty: er.handle_error(error_code ='12', data = {})
	
	return(df)


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

# Still need to complete
def get_welcome_script(df):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	trans = conn.begin()
	
	try: 
		welcome_script = pd.read_sql("SELECT id, script FROM scripts where day1 = P0 P1 P2", con = engine)
		conn.close()
	except Exception as e:
		print(e)
		trans.rollback()
		conn.close()
	
	df = pd.merge(df, welcome_script, how = 'outer', on = 'id', indicator = 'recieved_welcome')
	
	return(df)


# Still need to complete
def get_rejection_reason(df):
	
	engine = helpers_db.engine()
	conn = engine.connect()
	trans = conn.begin()
	
	try: 
		
		rejection_reasons = pd.read_sql("SELECT * FROM rejection_reason", con = engine)
		conn.close()
	
	except Exception as e:
		
		trans.rollback()
		conn.close()
		print(e)
	
	df = pd.merge(df, rejection_reason, how = 'left', on = 'rejection_reason', indicator = True)
	
	return(df)


def get_call_script(df):
		
	
	# Welcome script
	df['script'] = df.loc[(df['recieved_welcome'] != 'both'), 'script'] = "P0 P1 P2"
	
	# Static NREGA scripts
	df['script'] = df.loc[df['stage'].isna() & df['status'].isna() & df['_merge'] == 'right_only', 'script'] = 'P0 P1 P2 P3 Q A P0 Z1 Z2'
	
	# Dynamic NREGA scripts for FTOs at the block office
	df['script'] = df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'script'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	df['script'] = df.loc[(df['stage']=='sec_sig_not') | (df['stage']=='sec_sig'), 'script'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	
	# Dynamic NREGA scripts for unprocessed FTOs at the bank
	df['script'] = df.loc[(df['stage']=='sb') | (df['stage']=='pp'), 'script'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
	df['script'] = df.loc[(df['stage']=='P'), 'script'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
		
	# Dynamic NREGA scripts for trnasactions which have been processed
	df['script'] = df.loc[(df['status']=='Processed') & (df['stage']=='pb'), 'script'] = 'P0 P1 P2 P3 R EA EB EC P0 Z1 Z2'
	df['script'] = df.loc[(df['status']=='Rejected') & (df['stage']=='pb'), 'script'] = 'P0 P1 P2 P3 R FA FB FC P0 Z1 Z2'

	return(df)


def get_hh_amounts(df):
	
	# Replace amount with actual amount if it exists else with the amount due
	df['amount'] = np.where(df['credit_amt_actual'] != 0, df['credit_amt_actual'], df['credit_amt_due'])
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	return(df)
	
	
def get_hh_dates(df):
	
	# Replace transact_date with processed_date if transaction has been processed and then format
	df['transact_date'] = np.where(~df['processed_date'].isna(), df['processed_date'], df['transact_date'])
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	return(df)


def clean_df(df):
	
	# Keep only relevant columns and drop duplicates
	df = df[['id', 'phone', 'jcn', 'transact_date', 'time_pref', 'time_pref_label', 'amount', 'transact_date', 'rejection_reason', 'day1']] 
	df.drop_duplicates(['id'], inplace = True)
	
	df.reset_index(inplace = True)
	df = df.sample(frac=1)
	
	return(df)


def main():
	
	pilot = 0
	local = 1
	window_length = 7
	
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	local_output_path = './output/callsequence_{}.csv'.format(today)
	s3_output_path = 'scripts/callsequence_{}.csv'.format(today)
	
	transactions_alt = get_alternate_transactions(local = local)
	transactions = get_transactions(start_date, today)
	transactions = add_status_data(transactions, transactions_alt)
	camp = get_field_data_table(pilot)
	
	transactions, camp = check_jcn_format(transactions, df_field)
	df = merge_field_data(transactions, camp)
	
	df = set_call_script(df)
	df = get_hh_amaounts(df)
	df = get_hh_dates(df)
	
	df.to_csv(local_output_path, index = False)
	
'''
if __name__ == '__main__':
	
	main()
		
# Pending
# Add command line arguments
# Add in new error messages
# Do Alembic migrations'''