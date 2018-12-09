# Import packages
import pandas as pd
import numpy as np 

# Load the bank data 
def load_data(path):

	data = pd.read_csv(path, low_memory = False)

	return(data)

# Get common FTOs
def get_common_ftos(bank, branch):

	bank_ftos = pd.DataFrame(bank['fto_no'].unique())
	bank_ftos.columns = ['fto_no']

	branch_ftos = pd.DataFrame(branch['fto_no'].unique())
	branch_ftos.columns = ['fto_no']

	common_ftos = pd.merge(branch_ftos, bank_ftos, how = 'inner', on = 'fto_no')
	common_ftos.columns = ['fto_no']
	
	bank = pd.merge(bank, common_ftos, how = 'inner', on = ['fto_no'])
	branch = pd.merge(branch, common_ftos, how = 'inner', on = ['fto_no'])

	return(bank_ftos, branch_ftos)

# Merge data-sets
def merge_data(bank, branch):

	merged = pd.merge(bank, branch, how = 'outer', on = ['transact_ref_no'], indicator = 'check')
	
	return(merged)

# Get discrepancies
def check_data(merged):

	bank = merged.loc[merged['check'] == 'left_only'] 
	branch = merged.loc[merged['check'] == 'right_only']
	both = merged.loc[merged['check'] == 'both']
	
	return(bank, branch, both)

# Store columns to check later
def get_common_cols(bank, branch):
	
	bank_cols = bank.columns.values.tolist()
	branch_cols = branch.columns.values.tolist()
	
	cols = list(set(branch_cols) & set(bank_cols))
	cols.remove('transact_ref_no')

	return(cols)

# Check to make sure the same 
# Transaction reference no. contains the same information
def check_common_transactions(both, cols):
	
	# Count the number of discrepant transactions
	checked = [(col, (both[col + '_x'] != both[col + '_y']).sum()) for col in cols]
	checked = pd.DataFrame(checked, columns = ['Variable', '# Obs. in branch source but not in bank'])
	checked = checked.loc[checked['# Obs. in branch source but not in bank'] !=0]
	checked.to_csv('/Users/Akshat/Desktop/sources_check.csv')
	
	return(checked)

# Check common FTOs
def check_common_ftos():

	return

# Execute scripts
def execute(bank_data_path, branch_data_path):

	# Load the bank data
	bank_data = load_data(bank_data_path)
	branch_data = load_data(branch_data_path)

	# Get common columns
	cols = get_common_cols(bank_data, branch_data)
	
	# Restrict both data-sets to common FTOs
	bank_ftos, branch_ftos = get_common_ftos(bank_data, branch_data)

	# Merge both data-sets and get discrepancies
	merged_data = merge_data(bank_data, branch_data)
	only_bank, only_branch, both = check_data(merged_data)

	# Merged data checks
	checked = check_common_transactions(both, cols)

	# Keep only relevant variable


	return(only_bank, only_branch, both, checked)

# Execute this script
if __name__ == '__main__':
	
	# Store paths
	bank_data_path = '/Users/Akshat/Desktop/arang_bank_full.csv'
	branch_data_path = '/Users/Akshat/Desktop/arang_branch_sample.csv'
	only_bank, only_branch, both, checked = execute(bank_data_path, branch_data_path)


