import pandas as pd
import numpy as np
import argparse
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *

# Seed should be in global scope
np.random.seed(110498)
random.seed(13029)


def get_dynamic_call_script(df):
	
	# Initialize script
	df['day1'] = ''
	# Separate into dynamic
	df = df.loc[df['_merge'] == 'both']
	# Allocate dynamic scripts
	df = utils.set_nrega_scripts(df)
	# Aggregate amounts to household level
	df = utils.set_nrega_hh_amounts(df)
	# Aggregate dates to household level
	df = utils.set_nrega_hh_dates(df)
	# Allocate rejection reason - still need to complete
	df = utils.set_nrega_rejection_reason(df)
	# Keep only columns that are relevant to BTT in dynamic data
	df = utils.format_df(df, 0)
	# Add test calls to dynamic script
	df = utils.add_test_calls(df)
	
	return(df)

	
def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('window_length', type = int, help ='Time window in days from today for NREGA lookback')
	parser.add_argument('pilot', type = int, help = 'Whether to make script for pilot data or production data')
	parser.add_argument('dynamic', type = int, help ='Whether to make dynamic script')
	parser.add_argument('static', type = int, help ='Whether to make static script')
	parser.add_argument('local', type = int, help ='Get transactions from local')
	parser.add_argument('join', type = int, help ='Whether to join the two or not...')
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	dynamic = args.dynamic
	static = args.static
	pilot = args.pilot
	local = args.local
	
	# Set window lengths
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	# Set output paths
	local_output_path = './output/callsequence_{}.csv'.format(today)
	merge_output_path = './output/nregamerge_{}.csv'.format(today)
	s3_output_path = 'scripts/callsequence_{}.csv'.format(today)
	
	# Prepare camp data
	camp = gets.get_camp_data(pilot)
	camp = formatters.format_camp_jcn(camp) 
	
	# Get scripts
	if static == 1:
		df_static = get_static_script_look_backs(camp)
		df_static = get_static_call_script(df_static)
		df_static.to_csv(local_output_path, index = False)
		
	if dynamic == 1:
		# Prepare transactions data
		transactions_alt = gets.get_alternate_transactions(local = local)
		transactions = gets.get_transactions(start_date, today)
	
		# Add additional data about processed payments and format
		transactions = joins.join_alternate_transactions(transactions, transactions_alt)
		transactions = formatters.format_transactions_jcn(transactions)
		
		# Merge camp and transactions data 
		df_merged = merge_camp_data(transactions, camp)
		df_dynamic = get_dynamic_call_script(df_merged)
		df_dynamic.to_csv(local_output_path, index = False)
	
	# Output scripts
	if join == 1:
		df = pd.concat([df_dynamic, df_static])
		df.to_csv(local_output_path, index = False)
	
	helpers.send_mail('GMA Update: Finished executing the script creation. Please check the previous mails for any errors.')
		

if __name__ == '__main__':
	
	main()		
# Pending
# Add rejection reason
# Add output function call and S3 upload S3 upload
# Change file name convention to call date
# Do Alembic migrations