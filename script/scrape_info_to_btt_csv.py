import pandas as pd
from sqlalchemy import *
import helpers 
import errors as er
import numpy as np


def process_chunks(gens):

	dflist = []

	for gen in gens:
		dflist.append(gen)

	return pd.concat(dflist)


def get_field_data_table():
	
	engine = helpers.db_engine()
	conn = engine.connect()

	# Check if field_data table exists
	check = engine.has_table('field_data')

	if check:
		
		get_field_data = "SELECT * from field_data;"

		try: 
			# Grab field_data table
			gens = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
			df_field_data = process_chunks(gens)

			conn.close()

			# Reformat the JCNs
			df_field_data['jcn'] = df_field_data['jcn'].apply(ensure_jcn_format)

			return df_field_data

		except Exception as e:
		
			#er.handle_error(error_code ='5', data = {})
			print('IN EXCEPTIONS')
			print(e)
			exit()
			conn.close()
	else:
		# field_data table does not already exist
		return create_field_data_table()


def create_field_data_table():
	
	# Get from local path
	df_field = pd.read_csv('/Users/jliebs20/Downloads/jared_test.csv')

	# Add to db as new table
	engine = helpers.db_engine()
	conn = engine.connect()
	
	df_field.to_sql('field_data', con = conn, index = False, if_exists = 'replace',
		dtype = {'id': Integer(), 'respondent_name': VARCHAR(50), 'sky_phone': VARCHAR(50),
		'jcn': VARCHAR(50), 'jcn_extra': VARCHAR(50)})

	conn.execute('ALTER TABLE field_data ADD PRIMARY KEY (id);')

	return df_field


def get_db_tables():

	# Can easily get other needed tables in this function 
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_transactions = "SELECT * FROM transactions;"
	get_fto_queue = "SELECT * FROM fto_queue;"

	try: 

		# Grab transactions and fto_queue tables
		gens_transactions = pd.read_sql(get_transactions, con = conn, chunksize = 100000)
		transactions = process_chunks(gens_transactions)

		gens_fto_queue = pd.read_sql(get_fto_queue, con = conn, chunksize = 10000)
		fto_queue = process_chunks(gens_fto_queue)

		conn.close()

	except Exception as e:
		
		#er.handle_error(error_code ='5', data = {})
		print('IN EXCEPTIONS')
		conn.close()

	return transactions, fto_queue


def ensure_jcn_format(old_jcn):
	# This function loops through the original jcn and appends to a new string
	# with the appropriate values - it then returns the new string

	if not old_jcn: 
		return None
	
	old_jcn.replace(" ", "")

	new_jcn = ''

	for index, char in enumerate(old_jcn):
		
		if char.isalpha() or char.isdigit():
			new_jcn += char
		elif char == '/':
			# at household number
			# append / and all remaining chars
			new_jcn += old_jcn[index:]
			break
		elif char == '-':
			new_jcn += char
			# difference between indices should be 4 -> if not, add zeros
			next_dash_index = old_jcn.find('-', index + 1)

			index_difference = next_dash_index - index
			# -1 is returned if no more dashes exist -> so near "/"			
			if next_dash_index == -1:
				index_dash_difference = old_jcn.find('/') - index
				if index_dash_difference != 4:
					for iter in range(4 - index_dash_difference):
						new_jcn += '0'
			elif index_difference != 4:
				for iter in range(4 - index_difference):
					new_jcn += '0'
	
	return new_jcn


def merge_db_tables(transactions, fto_queue):

	# data prep before merge
	transactions.drop(columns=['block_name', 'transact_ref_no', 'app_name',
							   'wage_list_no', 'acc_no', 'ifsc_code',
							   'processed_date', 'utr_no', 'scrape_date', 
							   'scrape_time'], inplace=True)

	# Add CH-033 to the beginning of jcn's that don't have it
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	
	# Ensure JCN format is correct
	transactions['jcn'] = transactions['jcn'].apply(ensure_jcn_format)

	fto_queue.drop(columns=['done', 'fto_type', 'stage'], inplace=True)
	
	db_tables = pd.merge(transactions, fto_queue, on='fto_no')
	
	return db_tables


def merge_field_data(db_tables, df_field_data):

	df_full = pd.merge(db_tables, df_field_data, on='jcn', how='outer', indicator=True)
	
	# Get merged and field data non-matches
	df_full = df_full.query('_merge != "left_only"')

	if df_full.empty:
		er.handle_error(error_code ='12', data = {})
		return df_full
	
	return df_full


def set_call_script(df_full):

	def get_stage_letter(status, current_stage):

		script = 'P0 P1 P2 P3'

		if pd.isna(status) and pd.isna(current_stage):
			# here is where the non-matches should be
			# need to add on to script
			script += ' Q A B'

		elif status:
			# Processed
			if status == 'Processed':
				script += ' R EA EB EC'
			else:
				# Rejected
				script += ' R FA FB FC'
			
		elif current_stage == "fst_sig" or current_stage == "fst_sig_not" or \
				current_stage == "sec_sig" or current_stage == "sec_sig_not":
			script += ' R CA CB CC'

		else:
			script += ' R DA DB DC'
			
		conclusion = ' P0 Z1 Z2'

		script += conclusion

		return script


	df_full['time_pref'] = None
	df_full['amount'] = np.where(df_full['credit_amt_actual'] != 0, \
								 df_full['credit_amt_actual'], \
								 df_full['credit_amt_due'])

	df_full['day 1'] = np.vectorize(get_stage_letter)(df_full['status'], \
													  df_full['current_stage'])

	df_full = df_full[['id','sky_phone', 'amount', 'transact_date', 'time_pref',
					   'rejection_reason', 'day 1']]
	print(df_full)

	return df_full


def main():

	df_field_data = get_field_data_table()

	transactions, fto_queue = get_db_tables()

	db_tables = merge_db_tables(transactions, fto_queue)

	df_full = merge_field_data(db_tables, df_field_data)

	df_final = set_call_script(df_full)

	df_final.to_csv('dummy_calls_btt_pilot_3.csv', index=False)



if __name__ == '__main__':
	main()