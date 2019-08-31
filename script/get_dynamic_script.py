import pandas as pd
import numpy as np
import argparse
import random
import json
import sys

from common import errors as er
from datetime import datetime
from common import helpers

from script.utils import formatters
from script.utils import checks
from script.utils import joins
from script.utils import gets
from script.utils import sets
from sqlalchemy import *

# Seed should be in global scope
np.random.seed(110498)
random.seed(13029)

def get_dynamic_call_script(local_output_path, s3_output_path, pilot = 0, local = 1):
	
	# Prepare camp data
	camp = gets.get_camp_data(pilot)
	# Format JCNs
	camp = formatters.format_camp_jcn(camp)
	
	# Get alternate transactions
	transactions_alt = gets.get_alternate_transactions(local = local)
	# Get transactions data
	transactions = gets.get_transactions(start_date, today)
	
	# Add additional data about processed payments and format
	transactions = joins.join_alternate_transactions(transactions, transactions_alt)
	# Format JCN in transactions data
	transactions = formatters.format_transactions_jcn(transactions)
	
	# Merge camp and transactions data 
	df = joins.join_camp_data(transactions, camp)
	# Separate into dynamic
	df = df.loc[df['_merge'] == 'both']
	# Initialize script
	df['day1'] = ''

	# Allocate dynamic scripts
	df = sets.set_nrega_scripts(df)
	# Aggregate dates to household level
	df = sets.set_nrega_hh_dates(df)
	# Aggregate amounts to household level
	df = sets.set_nrega_hh_amounts(df)
	
	# Get rejection reasons data
	rejection_reasons = gets.get_rejection_reasons()
	# Allocate rejection reason - still need to complete
	df = sets.set_nrega_rejection_reason(df, rejection_reasons)
	
	# Keep only columns that are relevant to BTT in dynamic data
	df = formatters.format_final_df(df)
	# Add test calls to dynamic script
	df = sets.set_dynamic_test_calls(df)
	
	# Output
	df_dynamic.to_csv(local_output_path, index = False)
	# S3 upload
	helpers.upload_s3(local_output_path, s3_output_path)
	
	return(df)

def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('--window_length', type = int, help ='Time window in days from today for NREGA lookback', default = 7)
	parser.add_argument('--pilot', type = int, help = 'Whether to make script for pilot data or production data', default = 0)
	parser.add_argument('--static', type = int, help ='Whether to make static script', default = 1)
	parser.add_argument('--local', type = int, help ='Get transactions from local', default = 1)
	
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	pilot = args.pilot
	local = args.local
	
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	file_name_today = datetime.strptime(today, '%Y-%m-%d').strftime('%d%m%Y')
	local_output_path = './output/callsequence_{}.csv'.format(file_name_today)
	
	merge_output_path = './output/nregamerge_{}.csv'.format(file_name_today)
	s3_output_path = 'tests/callsequence_{}.csv'.format(file_name_today)
	
	if static == 1: get_static_call_script(local_output_path, s3_output_path, pilot)
	if dynamic == 1: get_dynamic_call_script(local_output_path, s3_output_path, pilot, local)
	if join == 1: df = joins.join_dynamic_static(local_output_path, s3_output_path, pilot, local)
	
	subject = 'GMA Update: The dynamic script was successfully created and uploaded ...take a break. Relaaaaax. See you tomorrow!'
	message = 'The file name is {}'.format(s3_output_path)
	helpers.send_email(subject, message)

if __name__ == '__main__':
	
	main()	