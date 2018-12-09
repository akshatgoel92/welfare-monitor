# Import packages
import pandas as pd
import numpy as np
import random

# Load data
def load_data(path = '/Users/Akshat/Desktop/arang_bank_full.csv'):

	return(pd.read_csv(path, low_memory = False))

# Merge filed JCN
def merge_field_jcn(script, field_path):

	pass

# Get transaction dates
def get_transaction_dates(data):

	data['date'] = data['processed_date']
	data.loc[data['date'].isna(), 'date'] = data['transact_date']
	
	transact_date = data.groupby('jcn')['date'].min()

	return(transact_date)

# Get household totals
def get_hh_totals(data):
	
	total_amount_due = data.groupby('jcn')['credit_amt_due'].sum()
	total_amount_actual = data.groupby('jcn')['credit_amt_due'].sum()

	return(total_amount_due, total_amount_actual)

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

# Check to make sure that stage is correct
# This function will call the scrape
def check_stage(data):

	pass

# Randomize health
def get_health(script):

	scenes = ['P', 'Q', 'R', 'S', 'T', 'U', 'V']
	options = ['Y Z ' + scene for scene in scenes]
	k = len(options)
	
	sequence = pd.DataFrame([random.choices(options, k = k) for call in range(len(script))], 
							index = script.index)
	sequence = sequence.rename(columns = lambda x : 'day' + str(x + 1))

	script = pd.concat([script, sequence], axis = 1)
	script.loc[~script['stage'].isna(), 'day1'] = script['stage']
	script.drop(columns = ['stage'], inplace = True)
	print(script.columns)

	return(script)

# Get calling script
def get_nrega(date, amount, stage):

	script = pd.concat([date, amount, stage], axis = 1)
	script.columns = ['transact_date', 'amount', 'stage']
	script.rename_axis('jcn')
	
	script.loc[script['stage'] == 1, 'stage'] = "D E"
	script.loc[script['stage'] == 2, 'stage'] = "D F"
	script.loc[script['stage'] == 3, 'stage'] = "D G"

	return(script)

# Execute the script
def execute(path = './arang_bank_full.csv'):

	hh_data = load_data(path)
	amount_due, amount_actual = get_hh_totals(hh_data)
	
	date = get_transaction_dates(hh_data)
	stage = get_stage(hh_data)
	
	script = get_nrega(date, amount_due, stage)
	script = get_health(script)
	
	return(script)

# 
if __name__ == '__main__':

	script = execute(path)
	script.to_csv('/Users/Akshat/Desktop/go_vivace_sample_script.csv')
