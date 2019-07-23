import pandas as pd
import sys
from sqlalchemy import *
from datetime import datetime
from common import helpers
from common import errors as er
import numpy as np

# Initialize an empty list
# Return statement
def process_chunks(gens):
	
	df_list = pd.concat([gen for gen in gens])	
	
	return(df_list)


def ensure_jcn_format(old_jcn):
	# This function loops through the original JCN and appends to a new string
	# with the appropriate values - it then returns the new string
	# It proceeds as follows:
	# We only want to reformat old JCNs so filter out new JCNS
	# Replace whitespace in old JCN
	# Initialize the new JCN
	# Loop through each index and character of the old JCN
	# Check for alphabetic and numeric characters and then append them to the new JCN
	# If we have then just add the remaining part of the old JCN to the new JCN and then we are done
	# Else if we have found a '-' we know that this is not the end of the JCN
	# Difference between indices should be 4 for the region code -> if not, add zeros
	# Get the index of the next dash starting from the current index + 1
	# Calculate the index difference
	# -1 is returned if no more dashes exist -> so near "/"
	# Find the occurence of the '/'
	# Add the zeros
	# Fill up the zeros
	# Deal with the other case
	# Add in new zeros until the next index??
	# Return statement
	
	if not old_jcn: return None
	old_jcn.replace(" ", "")
	new_jcn = ''
	
	for index, char in enumerate(old_jcn):
		
		if char.isalpha() or char.isdigit(): 
			new_jcn += char
		
		elif char == '/': 
			
			new_jcn += old_jcn[index:]
			break
		
		elif char == '-':
			
			new_jcn += char
			next_dash_index = old_jcn.find('-', index + 1)
			index_difference = next_dash_index - index
			
			if next_dash_index == -1: 
				index_dash_difference = old_jcn.find('/') - index
				
				if index_dash_difference != 4: 
					for iter in range(4 - index_dash_difference): new_jcn += '0'
			
			elif index_difference != 4:
				for iter in range(4 - index_difference): new_jcn += '0'
	
	return(new_jcn)


# Change this to S3 download
# Remove the hard-coded file path
# Get from local path
# Replace row '--' with '-'
# Add to DB as new table
# Extract connection object
# Write to SQL database
# Add the primary key
# Return statement to return control to calling function
def create_field_data_table(filepath):
	
	engine = helpers.db_engine()
	conn = engine.connect()

	df_field = pd.read_csv(filepath)
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field.to_sql('field_data', con = conn, index = False, if_exists = 'append',
					dtype = {'id': Integer(), 'respondent_name': VARCHAR(50), 'sky_phone': VARCHAR(50),
					'jcn': VARCHAR(50), 'jcn_extra': VARCHAR(50), 'time_pref': VARCHAR(50)})
	
	conn.execute('ALTER TABLE field_data ADD PRIMARY KEY (id);')
	
	return 


# Add error handling
# Create engine
# Extract the connection object
# Check if field_data table exists
# Explicit is better than implicit
# Create the field table
# Run this SQL query
# Try to get the field data-set
# Grab field_data table
# Read SQL with chunks returns an iterator so we concatenate this to get one data-frame
# Close the connection
# Reformat the JCNs
def get_field_data_table():
	
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = "SELECT * from field_data;"
	if check is False: create_field_data_table()

	
	try: 
		
		gens = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field_data = process_chunks(gens)
		conn.close()
		df_field_data['jcn'] = df_field_data['jcn'].apply(ensure_jcn_format)
	
	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		print(e)
		exit()
		conn.close()
		
	return(df_field_data)


# Put in exception handling by changing error messages in the script
# Get actual data
# Can easily get other needed tables in this function
# Grab transactions and fto_queue tables 
def get_db_tables(start_date, end_date):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_transactions = "SELECT * FROM transactions WHERE transact_date BETWEEN '{}' AND '{}';".format(start_date, end_date)
	get_fto_queue = "SELECT * FROM fto_queue;"

	try: 
		
		gens_fto_queue = pd.read_sql(get_fto_queue, con = conn, chunksize = 1000)
		fto_queue = process_chunks(gens_fto_queue)
		gens_transactions = pd.read_sql(get_transactions, con = conn, chunksize = 1000)
		
		transactions = process_chunks(gens_transactions)
		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		conn.close()

	return transactions, fto_queue


# Drop unneccesary columns from the transactions data
# Add CH-033 to the beginning of JCN's that don't have it
# Ensure JCN format is correct
# Drop unneeded columns from queue
# Merge this with the database transactions tables
def merge_db_tables(transactions, fto_queue):

	
	transactions.drop(columns=['block_name', 'transact_ref_no', 'app_name', 'wage_list_no', 'acc_no', 
							   'ifsc_code', 'processed_date', 'utr_no', 'scrape_date', 'scrape_time'], 
							   inplace=True)

	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(ensure_jcn_format)
	
	fto_queue.drop(columns=['done', 'fto_type', 'stage'], inplace=True)
	db_tables = pd.merge(transactions, fto_queue, on='fto_no')
	
	return db_tables


# Merge the two data-sets
# Get all observations in the merged data-set which are either only in the field data-set or in both the
# transactions table and the field data-set
# If the data-frame is empty we return error code 12
def merge_field_data(db_tables, df_field_data):

	
	df_full = pd.merge(db_tables, df_field_data, on='jcn', how='outer', indicator=True)
	df_full = df_full.query('_merge != "left_only"')
	if df_full.empty: er.handle_error(error_code ='12', data = {})
	
	return df_full


# This gets called by set_call_script to allocate the sequence of 
# audio file names to each recipient.
# Store the introduction
# Store the block stages
# All non-matches should get this
# Processed payments should get this
# Rejected payments should get this
# Tell those whose payments are pending that they are with the block office
# Else tell those whose payments are pending that the payments are with the bank
# Create the conclusion	
# Add the conclusion
# Finally we return
def get_stage_letter(status, current_stage):

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


def set_call_script(df_full):

	# Set this variable as actual credit amount where it is available else set it to the credit amount due
	# Then give each row in the data-set the calling sequence
	# Print the data-frame to the screen as a check
	# Return the data-frame
	df_full['amount'] = np.where(df_full['credit_amt_actual'] != 0, df_full['credit_amt_actual'], df_full['credit_amt_due'])
	df_full['day 1'] = np.vectorize(get_stage_letter)(df_full['status'], df_full['current_stage'])
	print(df_full)
	return df_full


# Get the household totals for the final data-set.
# First get household level total amounts
# Convert into date time object
# Group by ID
# Now we can drop duplicate IDs so we end up with a data-set unique at the ID level
# Reset index
# Then keep only relevant columns
# Then convert 0s in amount columns to NAs
# Return statement
def get_hh_level_data(df):
	
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	df.drop_duplicates(['id'], inplace = True) 
	df.reset_index(inplace = True)
	df = df[['id','sky_phone', 'amount', 'transact_date', 'time_pref', 'rejection_reason', 'day 1']]
	df['amount'] = df['amount'].replace(0, np.nan)
	
	return(df)


def add_rejected_payments(df_full):
	
	pass


def tag_welcome_recipients(df_full):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	
def add_welcome_script(df_full):
	
	engine = helpers.db_engine()
	conn = engine.con()
	trans = engine.trans()
	
	

def add_test_calls():
	
	pass


# Replace the stage letter just for the final pilot
# Store only NREGA data-frame
# Store only those observations which are not NREGA matches
# Shuffle the data-frame
# Store the length of the data-frame
# Replace the first half of the matches with one letter
# Store the second half of the data-frame
# Replace the first half
# Replace the second half
# Now put the data-frames back together again
# Return statement
def replace_stage_letter(df_full):
	
	df_nrega = df_full.loc[~df_full['amount'].isna()]
	df = df_full.loc[df_full['amount'].isna()]
	df = df.sample(frac=1)
	
	total_obs = len(df)
	
	df_1 = df.iloc[0:total_obs//2,:]
	df_2 = df.iloc[total_obs//2:,:]
	
	df_1['day 1'] = df_1['day 1'].apply(lambda x: x.replace("Q A B", "Q A"))
	df_2['day 1'] = df_2['day 1'].apply(lambda x: x.replace("Q A B", "Q B"))
	
	df = pd.concat([df_1, df_2, df_nrega])
	
	return(df)


# Specify window length from today 
# Store today's date
# Use helper function to get the start date which is window_length days before today
# If check is table
# Get the field data table
# Get the transactions and FTO queue
# Merge the database tables with each other - do this join in SQL eventually
# Now merge the database information with the field data-set
# Allocate the calling script
# Now aggregate to the household level
# Randomize stage letter
# S3 upload
def main():
	
	window_length = 30
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	if engine.has_table('field_data') is False: sys.exit()
	
	df_field_data = get_field_data_table()
	transactions, fto_queue = get_transactions_and_queue(start_date, today)
	db_tables = merge_db_tables(transactions, fto_queue)
	
	df_full = merge_field_data(db_tables, df_field_data)
	df_full = set_call_script(df_full)
	df_full = get_hh_level_data(df_full)
	
	if pilot == 1: df_final = replace_stage_letter(df_full)
	
	df_full.to_csv('./output/script_{}.csv'.format(today))
	helpers.s3_upload('./output/script_{}.csv'.format(today), 'scripts/script_{}.csv'.format(today))

# Execute the script
if __name__ == '__main__':
	main()

# Pending
# Add in new error messages
# Make this into a routine with command line arguments and adjust the shell script accordingly
# Make helper functions into separate file
# Think about how to structure field data tables
# Do Alembic migrations