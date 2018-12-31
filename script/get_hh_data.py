# Import packages
import pandas as pd
import numpy as np
import argparse
import xlwings as xw
import re
import random

# Set seed 
random.seed(90123)

# Load data
def load_data(path = './nrega_output/arang_pilot_full_v3.csv'):

	return(pd.read_csv(path, low_memory = False))

def clean_data(data):

	pass

# Restrict to a particular time window
def get_time_window(data):

	start_date = '2018-11-01'
	end_date = '2018-12-16'

	data['date'] = pd.to_datetime(data['transact_date'])
	mask = (data['date'] > start_date) & (data['date'] <= end_date)
	data = data.loc[mask]

	return(data)

# Load field
def load_field(field = '/Users/akshatgoel/Desktop/20Dec_Bhansoj_jcn.xlsx'):

	wb = xw.Book(field)
	sheet = wb.sheets['16Dec_Tekari1_jcn.xlsx']
	field = sheet['A1:E43'].options(pd.DataFrame, index = False, header = True).value
	
	return(field)

# Clean field
def clean_field(field, village_code = 'CH-16-015-014-001'):

	# Drop missing values
	field = field.loc[~field['jcn'].isna()]

	# Clean variables
	field['sky_phone'] = field['sky_phone'].astype(int).astype(str)
	field['id'] = field['id'].astype(int).astype(str)
	field['time_pref'] = field['time_pref'].astype(int).astype(str)
	
	# Generate field JCN
	field.loc[field['jcn'].isna(), 'jcn'] = ''
	field['jcn_field'] = field['jcn'].apply(lambda x: village_code + '/' + re.split('\W+', x)[-1].lstrip('0'))
	field.loc[field['jcn_field'] == village_code + '/', 'jcn_field'] = ''
	
	# Drop old JCN
	field.drop('jcn', axis = 1, inplace = True)
	field.rename({'jcn_field': 'jcn'}, inplace = True, axis = 1)
	
	return(field)

# Get transaction dates
def get_transaction_dates(data):

	data['date'] = data['processed_date']
	data.loc[data['date'].isna(), 'date'] = data['transact_date']
	# data['date'] = pd.to_datetime(data['date'])
	
	transact_date = data.groupby('jcn')['date'].max()
	transact_date = transact_date.astype(str)

	return(transact_date)

# Get household totals
def get_hh_totals(data):
	
	total_amount_due = data.groupby('jcn')['credit_amt_due'].sum()
	total_amount_actual = data.groupby('jcn')['credit_amt_due'].sum()

	return(total_amount_due, total_amount_actual)


# Get FTO stage
def get_fto_stage(data):

	fto_stage = pd.read_csv('/Users/akshatgoel/Documents/Code/fto-scrape/nrega_output/fto_stage.csv')
	data = pd.merge(data, fto_stage, how = 'left', on = ['fto_no'])

	return(data)


# Get stage
# 1: Block
# 2: Bank
# 3: Rejected
# We want to be conservative with HH information
def get_stage(data):

	data.loc[data['status'].isna(), 'stage'] = 1
	data.loc[data['status'] == 'Processed', 'stage'] = 2
	data.loc[data['status'] == 'Rejected', 'stage'] = 3
	
	stage = data.groupby('jcn')['stage'].min()
	
	return(stage)


# Randomize health
def get_health(script):

	scenes = ['P', 'Q', 'R', 'S', 'T', 'U', 'V']
	options = ['Y Z ' + scene for scene in scenes]
	sequence = pd.DataFrame([random.sample(options, k = len(options)) for call in range(len(script))], 
							index = script.index)
	sequence = sequence.rename(columns = lambda x : 'day' + str(x + 1))
	script = pd.concat([script, sequence], axis = 1)
	
	return(script)

# Get calling script
def get_nrega(date, amount, stage):

	data = pd.concat([date, amount, stage], axis = 1)
	data.columns = ['transact_date', 'amount', 'stage']
	data.rename_axis('jcn')
	
	data.loc[data['stage'] == 1, 'stage'] = "D E"
	data.loc[data['stage'] == 2, 'stage'] = "D F"
	data.loc[data['stage'] == 3, 'stage'] = "D G"
	data = data.reset_index()

	return(data)

# Merge with field JCN
def merge_field(field, script):

	script = pd.merge(field, script, how = 'left', on = ['jcn'], indicator = 'script_merge')

	return(script)

# Put NREGA script
def put_nrega_script(script):

	mask = ~script['stage'].isna()
	script.loc[mask, 'day1'] = script['stage']

	return(script)

def clean_final_script(script):

	cols = ['id', 'sky_phone', 'amount', 'transact_date', 'time_pref', 
			'time_pref_label', 'day1', 'day2', 'day3', 'day4', 'day5', 'day6', 'day7']

	script = script[cols]

	return(script)

def add_dummy_calls(field, dummy = './nrega_output/dummy_pilot_arang_pilot.csv'):

	dummy = pd.read_csv(dummy)
	field = pd.concat([field, dummy])
	
	return(field)

# Add rejected payments for testing
def add_rejects(script):

	mask = (script['id'].astype(int) > 8000) & (script['day1'] == 'D E')
	script.loc[mask, 'day1'] = 'D G'
	
	return(script)

# Execute the script
def execute(path = '', file_to ='/Users/akshatgoel/Desktop/btt_script.csv', 
		   field = ''):
	
	# Household data preparation
	hh_data = load_data()
	# hh_data = get_time_window(hh_data)

	# Clean field data
	field = load_field()
	field = clean_field(field)
	field = add_dummy_calls(field = field)

	# Prepare NREGA data
	amount_due, amount_actual = get_hh_totals(hh_data)
	date = get_transaction_dates(hh_data)
	stage = get_stage(hh_data)
	nrega = get_nrega(date, amount_due, stage)

	# Script preparation
	script = merge_field(field, nrega)
	script = get_health(script)
	script = put_nrega_script(script)
	script = clean_final_script(script)
	script = add_rejects(script)

	# Output script
	script.to_csv(file_to, index = False)
	
	return(script)




# Execute the script
if __name__ == '__main__':

	# Create parser
    parser = argparse.ArgumentParser(description='Parse file paths for stage')
    parser.add_argument('file_from', type=str, help='Block name')
    parser.add_argument('file_to', type=str, help='Whether to write to Dropbox')
    # parser.add_argument('file_field', type=str, help='Whether to write to Dropbox')
    
    # Parse arguments
    args = parser.parse_args()
    file_from = args.file_from
    file_to = args.file_to
    # file_field = args.file_field() 
    
    # Create SQL data-base connection 
   	# user, password, host, db = helpers.sql_connect().values()
    # engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
    
    # Call functions
    execute(path = file_from, file_to = file_to)

	
