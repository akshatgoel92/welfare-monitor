import pandas as pd
import numpy as np
import sys

from sqlalchemy import *
from datetime import datetime
from common import helpers
from common import errors as er
from process import process_chunks
from process import ensure_jcn_format


def add_field_data(filepath):
	
	df_field = pd.read_csv(filepath)
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	data_types = {'id': Integer(), 'respondent_name': VARCHAR(50), 'sky_phone': VARCHAR(50), 
				  'jcn': VARCHAR(50), 'jcn_extra': VARCHAR(50), 'time_pref': VARCHAR(50)}
	
	try: 
		df_field.to_sql('field_data', con = conn, index = False, if_exists = 'append', 
						dtype = data_types)

	except Exception as e:
		er.handle_error(error_code ='15', data = {})
	
	try: conn.execute('ALTER TABLE field_data ADD PRIMARY KEY (id);')
	except Exception as e: er.handle_error(error_code ='16', data = {})
	
	return 


def get_field_data():
	
	engine = helpers.db_engine()
	conn = engine.connect()

	get_field_data = "SELECT * from field_data;"
	
	try: 
		gens = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field_data = process_chunks(gens)
		conn.close()
		
	except Exception as e:
		er.handle_error(error_code ='17', data = {})
		sys.exit()
		conn.close()
	
	try: df_field_data['jcn'] = df_field_data['jcn'].apply(ensure_jcn_format)
	except Exception as e: er.handle_error(error_code ='18', data = {})
		
	return(df_field_data)

 
def get_scraped_data(start_date, end_date):

	def format_jcns(transactions): 

		transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].\
						  	  apply(lambda x: 'CH-033' + x[2:])
		transactions['jcn'] = transactions['jcn'].apply(ensure_jcn_format)

		return transactions

	engine = helpers.db_engine()
	conn = engine.connect()
	
	fields = ', '.join(['jcn', 'transact_ref_no', 'transact_date', 'credit_amt_due', 
						'credit_amt_actual', 'status', 'rejection_reason', 'fto_no'])
	
	get_transactions = "SELECT {} FROM transactions WHERE transact_date BETWEEN '{}' AND '{}';".\
						format(fields, start_date, end_date)
	get_fto_queue = "SELECT fto_no FROM fto_queue;"

	try: 
		gens_fto_queue = pd.read_sql(get_fto_queue, con = conn, chunksize = 1000)
		fto_queue = process_chunks(gens_fto_queue)
	
	except Exception as e:
		er.handle_error(error_code ='19', data = {})
		
	
	try: 
		gens_transactions = pd.read_sql(get_transactions, con = conn, chunksize = 1000)
		transactions = process_chunks(gens_transactions)
		
		conn.close()

	except Exception as e:
		er.handle_error(error_code ='20', data = {})
		conn.close()
	
	try: transactions = format_jcns(transactions)
	except Exception as e: er.handle_error(error_code ='12', data = {})
		
	tables = pd.merge(transactions, fto_queue, on='fto_no')
	
	return tables

# This doesn't need it's own separate function
# Combine this with above
def merge_field_data(db_tables, df_field_data):

	# Merge the two data-sets
	df_full = pd.merge(db_tables, df_field_data, on='jcn', how='outer', indicator=True)
	df_full = df_full.loc(df_full['_merge'] != "left_only"')
	
	if df_full.empty: er.handle_error(error_code ='21', data = {})
	
	return df_full

# Change inside function to sequence of apply statements
def set_call_script(df_full):

	def get_stage_letter(status, current_stage):

		# Store the introduction
		# Store the block stages
		# Allocate all scripts for non-matches
		# Allocate all scripts for processed payments
		# Allocate all scripts for rejected payments
		# Allocate all scripts which are at the block office
		# Allocate all scripts which are at the bank
		# Create the conclusion	
		# Add the conclusion
		
		script = 'P0 P1 P2 P3'
		block_stages = ["fst_sig_not", "fst_sig", "sec_sig", "sec_sig_not"]
	
		if pd.isna(status) and pd.isna(current_stage): script += ' Q A B'
		if status == 'Processed': script += ' R EA EB EC'
	
		elif status == 'Rejected': script += ' R FA FB FC'
		elif current_stage in block_stages: script += ' R CA CB CC'
		else: script += ' R DA DB DC'
	
		conclusion = ' P0 Z1 Z2'
		script += conclusion
	
		return script

	df_full['amount'] = np.where(df_full['credit_amt_actual'] != 0, df_full['credit_amt_actual'], 
								 df_full['credit_amt_due'])
	df_full['day 1'] = np.vectorize(get_stage_letter)(df_full['status'], df_full['current_stage'])
	
	return df_full


def get_hh_level_data(df):
	
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	cols_to_keep = ['id','sky_phone', 'amount', 'transact_date', 'time_pref', 'rejection_reason', 'day 1']
	df.drop_duplicates(['id'], inplace = True) 
	df.reset_index(inplace = True)
	df = df[cols_to_keep]
	
	return(df)


def replace_stage_letter(df_full):

	df_nrega = df_full.loc[~df_full['amount'].isna()]
	
	df = df_full.loc[df_full['amount'].isna()]
	df = df.sample(frac=1)
	
	df_1 = df.iloc[0:len(df)//2, :]
	df_2 = df.iloc[len(df)//2:, :]
	
	df_1['day 1'] = df_1['day 1'].apply(lambda x: x.replace("Q A B", "Q A"))
	df_2['day 1'] = df_2['day 1'].apply(lambda x: x.replace("Q A B", "Q B"))
	df = pd.concat([df_1, df_2, df_nrega])
	
	return(df)


def main():
	
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)

	parser = argparse.ArgumentParser(description='Script upload parser')
	parser.add_argument('window_length', type=int, help='Source file path')
	parser.add_argument('file_from', type=str, help='Source file path')
	parser.add_argument('file_to', type=str, help='Destination file path')
			
	args = parser.parse_args()
	window_length = args.window_length
	file_from = args.file_from
	file_to = args.file_to + '_' + str(today) + '.csv'
	
	_ = add_field_data(file_from)
	df_field = get_field_data()
	
	transactions, fto_queue = get_scraped_data(start_date, today)
	df_db = merge_scraped_data(transactions, fto_queue)
	
	df_full = merge_field_data(db_tables, df_field_data)
	df_full = set_call_script(df_full)
	df_full = get_hh_level_data(df_full)
	
	if pilot == 1: df_final = replace_stage_letter(df_full)
	
	df_final.to_csv('./output/script_{}.csv'.format(today), index = False)
	

if __name__ == '__main__':
	main()

# Pending
# Add in S3 upload/download
# Transition select statements to use db module in repository
# Add test calls to team
# Talk to Anwesha about '--'
# Confirm rejected payments scripts
# Test
# Do Alembic migrations
