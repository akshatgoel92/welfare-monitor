import pandas as pd
import sys
from sqlalchemy import *
from datetime import datetime
from common import helpers
from common import errors as er
import numpy as np


def process_chunks(gens):
	# Initialize an empty list
	df_list = pd.concat([gen for gen in gens])	
	# Return statement
	return(df_list)


def ensure_jcn_format(old_jcn):
	# This function loops through the original JCN and appends to a new string
	# with the appropriate values - it then returns the new string

	# We only want to reformat old JCNs
	if not old_jcn: return None
	# Replace whitespace in old JCN
	old_jcn.replace(" ", "")
	# Initialize the new JCN
	new_jcn = ''
	# Loop through each index and character of the old JCN
	for index, char in enumerate(old_jcn):
		# Check for alphabetic and numeric characters
		if char.isalpha() or char.isdigit(): 
			# And then append them to the new JCN
			new_jcn += char
		# Check to see if we have reached '/'
		elif char == '/':
			# If we have then just add the remaining part of the old JCN to the new JCN
			new_jcn += old_jcn[index:]
			# We are done
			break
		# Else if we have found a '-' we know that this is not the end of the JCN
		elif char == '-':
			# Difference between indices should be 4 for the region code -> if not, add zeros
			new_jcn += char
			# Get the index of the next dash starting from the current index + 1
			next_dash_index = old_jcn.find('-', index + 1)
			# Calculate the index difference
			index_difference = next_dash_index - index
			# -1 is returned if no more dashes exist -> so near "/"			
			if next_dash_index == -1: 
				# Find the occurence of the '/'
				index_dash_difference = old_jcn.find('/') - index
				# Add the zeros
				if index_dash_difference != 4:
					# Fill up the zeros
					for iter in range(4 - index_dash_difference): new_jcn += '0'
			# Deal with the other case
			elif index_difference != 4:
				# Add in new zeros until the next index??
				for iter in range(4 - index_difference): new_jcn += '0'
	# Return statement
	return new_jcn


# Change this to S3 download
# Remove the hard-coded file path
def create_field_data_table(filepath):
	
	# Get from local path
	df_field = pd.read_csv(filepath)
	# Replace row '--' with '-'
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	# Add to DB as new table
	engine = helpers.db_engine()
	# Extract connection object
	conn = engine.connect()
	# Write to SQL database
	df_field.to_sql('field_data', con = conn, index = False, if_exists = 'append',
					dtype = {'id': Integer(), 'respondent_name': VARCHAR(50), 'sky_phone': VARCHAR(50),
					'jcn': VARCHAR(50), 'jcn_extra': VARCHAR(50), 'time_pref': VARCHAR(50)})
	# Add the primary key
	conn.execute('ALTER TABLE field_data ADD PRIMARY KEY (id);')
	# Return statement to return control to calling function
	return 


# Add error handling
def get_field_data_table():
	
	# Create engine
	engine = helpers.db_engine()
	# Extract the connection object
	conn = engine.connect()
	# Check if field_data table exists
	check = engine.has_table('field_data')
	# Explicit is better than implicit
	# Create the field table
	if check is False: create_field_data_table()
	# Run this SQL query
	get_field_data = "SELECT * from field_data;"
	# Try to get the field data-set
	try: 
		# Grab field_data table
		gens = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		# Read SQL with chunks returns an iterator so we concatenate this to get one data-frame
		df_field_data = process_chunks(gens)
		# Close the connection
		conn.close()
		# Reformat the JCNs
		df_field_data['jcn'] = df_field_data['jcn'].apply(ensure_jcn_format)
	# Exception handling
	except Exception as e:
		er.handle_error(error_code ='5', data = {})
		# Print the traceback
		print(e)
		# Exit in the case of an exception
		exit()
		# Close the connnection
		conn.close()
	# Return the field data	
	return(df_field_data)


# Put in exception handling by changing error messages in the script
# Get actual data 
def get_db_tables(start_date, end_date):

	# Can easily get other needed tables in this function 
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_transactions = "SELECT * FROM transactions WHERE transact_date BETWEEN '{}' AND '{}';".format(start_date, end_date)
	get_fto_queue = "SELECT * FROM fto_queue;"

	try: 

		# Grab transactions and fto_queue tables
		gens_fto_queue = pd.read_sql(get_fto_queue, con = conn, chunksize = 1000)
		fto_queue = process_chunks(gens_fto_queue)
		print('queue got')
		gens_transactions = pd.read_sql(get_transactions, con = conn, chunksize = 1000)
		transactions = process_chunks(gens_transactions)
		print('transactions_got')

		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		conn.close()

	return transactions, fto_queue


def merge_db_tables(transactions, fto_queue):

	# Drop unneccesary columns from the transactions data
	transactions.drop(columns=['block_name', 'transact_ref_no', 'app_name', 'wage_list_no', 'acc_no', 
							   'ifsc_code', 'processed_date', 'utr_no', 'scrape_date', 'scrape_time'], 
							   inplace=True)

	# Add CH-033 to the beginning of JCN's that don't have it
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].\
						  apply(lambda x: 'CH-033' + x[2:])
	
	# Ensure JCN format is correct
	transactions['jcn'] = transactions['jcn'].apply(ensure_jcn_format)
	# Drop unneeded columns from queue
	fto_queue.drop(columns=['done', 'fto_type', 'stage'], inplace=True)
	# Merge this with the database transactions tables
	db_tables = pd.merge(transactions, fto_queue, on='fto_no')
	# Return statement
	return db_tables


def merge_field_data(db_tables, df_field_data):

	# Merge the two data-sets
	df_full = pd.merge(db_tables, df_field_data, on='jcn', how='outer', indicator=True)
	# Get all observations in the merged data-set which are either only in the field data-set or in both the
	# transactions table and the field data-set
	df_full = df_full.query('_merge != "left_only"')
	# If the data-frame is empty 
	if df_full.empty:
		# We return error code 12
		er.handle_error(error_code ='12', data = {})
	# Return statement
	return df_full


def get_stage_letter(status, current_stage):
	'''This gets called by set_call_script to allocate the sequence of 
	   audio file names to each recipient.'''

	# Store the introduction
	script = 'P0 P1 P2 P3'
	# Store the block stages
	block_stages = ["fst_sig_not", "fst_sig", "sec_sig", "sec_sig_not"]
	# All non-matches should get this
	if pd.isna(status) and pd.isna(current_stage): script += ' Q A B'
	# Processed payments should get this
	if status == 'Processed': script += ' R EA EB EC'
	# Rejected payments should get this
	elif status == 'Rejected': script += ' R FA FB FC'
	# Tell those whose payments are pending that they are with the block office
	elif current_stage in block_stages: script += ' R CA CB CC'
	# Else tell those whose payments are pending that the payments are with the bank
	else: script += ' R DA DB DC'
	# Create the conclusion	
	conclusion = ' P0 Z1 Z2'
	# Add the conclusion
	script += conclusion
	# Finally we return
	return script


def set_call_script(df_full):

	# Set this variable as actual credit amonut where it is available else set it to the credit amount due
	df_full['amount'] = np.where(df_full['credit_amt_actual'] != 0, df_full['credit_amt_actual'], df_full['credit_amt_due'])
	# Then give each row in the data-set the calling sequence
	df_full['day 1'] = np.vectorize(get_stage_letter)(df_full['status'], df_full['current_stage'])
	# Print the data-frame to the screen as a check
	print(df_full)
	# Return the data-frame
	return df_full


def get_hh_level_data(df):
	'''Get the household totals for the final data-set.'''
	
	# First get household level total amounts
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	# Convert into date time object
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	# Group by ID
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	# Now we can drop duplicate IDs so we end up with a data-set unique at the ID level
	df.drop_duplicates(['id'], inplace = True) 
	# Reset index
	df.reset_index(inplace = True)
	# Then keep only relevant columns
	df = df[['id','sky_phone', 'amount', 'transact_date', 'time_pref', 'rejection_reason', 'day 1']]
	# Then convert 0s in amount columns to NAs
	df['amount'] = df['amount'].replace(0, np.nan)
	# Return statement
	return(df)


def replace_stage_letter(df_full):
	'''Replace the stage letter just for the final pilot.'''

	# Store only NREGA data-frame
	df_nrega = df_full.loc[~df_full['amount'].isna()]
	# Store only those observations which are not NREGA matches
	df = df_full.loc[df_full['amount'].isna()]
	# Shuffle the data-frame
	df = df.sample(frac=1)
	# Store the length of the data-frame
	total_obs = len(df)
	# Replace the first half of the
	df_1 = df.iloc[0:total_obs//2, :]
	# Store the second half of the data-frame
	df_2 = df.iloc[total_obs//2:, :]
	# Replace the first half 
	df_1['day 1'] = df_1['day 1'].apply(lambda x: x.replace("Q A B", "Q A"))
	# Replace the second half
	df_2['day 1'] = df_2['day 1'].apply(lambda x: x.replace("Q A B", "Q B"))
	# Now put the data-frames back together again
	df = pd.concat([df_1, df_2, df_nrega])
	# Return statement
	return(df)


def main():

	# Specify window length from today 
	window_length = 30
	# Store today's date
	today = str(datetime.today().date())
	# Use helper function to get the start date which is window_length days before today
	start_date = helpers.get_time_window(today, window_length)
	# Get the field data table
	df_field_data = get_field_data_table()
	# Get the transactions and FTO queue
	transactions, fto_queue = get_db_tables(start_date, today)
	# Merge the database tables with each other - do this join in SQL eventually
	db_tables = merge_db_tables(transactions, fto_queue)
	# Now merge the database information with the field data-set
	df_full = merge_field_data(db_tables, df_field_data)
	# Allocate the calling script
	df_full = set_call_script(df_full)
	# Now aggregate to the household level
	df_full = get_hh_level_data(df_full)
	# Randomize stage letter
	if pilot == 1: df_final = replace_stage_letter(df_full)
	# Send this to S3 instead whenever ready instead of .csv
	df_final.to_csv('./output/script_{}.csv'.format(today), index = False)
	# S3 upload
	helpers.s3_upload('./output/script_{}.csv'.format(today), 'scripts/script_{}.csv'.format(today))

# Execute the script
if __name__ == '__main__':
	main()

# Pending
# Add in new error messages
# Make this into a routine with command line arguments and adjust the shell script accordingly
# Confirm rejected payments scripts
# Make helper functions into separate file
# Add test calls to team if possible
# Think about how to structure field data tables
# Talk to Anwesha about '--'
# Do Alembic migrations