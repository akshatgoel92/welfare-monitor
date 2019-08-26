import pandas as pd
import numpy as np
import argparse
import random
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
	# Initialize script
	df['day1'] = ''
	
	# Separate into dynamic
	df = df.loc[df['_merge'] == 'both']
	# Allocate dynamic scripts
	df = sets.set_nrega_scripts(df)
	
	# Aggregate dates to household level
	df = sets.set_nrega_hh_dates(df)
	# Aggregate amounts to household level
	df = sets.set_nrega_hh_amounts(df)
	# Allocate rejection reason - still need to complete
	df = sets.set_nrega_rejection_reason(df)
	
	# Keep only columns that are relevant to BTT in dynamic data
	df = utils.formatters.format_final_df(df)
	# Add test calls to dynamic script
	df = utils.sets.set_test_calls(df)
	
	# Output
	df_dynamic.to_csv(local_output_path, index = False)
	# S3 upload
	helpers.s3_upload(local_output_path, s3_output_path)
	
	return(df)


def get_static_call_script(local_output_path, s3_output_path, pilot = 0):

	# Prepare camp data
	camp = gets.get_camp_data(pilot)
	# Format JCNs
	df = formatters.format_camp_jcn(camp)
	
	# Check welcome script
	df = checks.check_welcome_script(df)
	# Get welcome script indicator
	df = checks.get_welcome_script_indicator(df)
	
	# Check static NREGA script for A 
	df = checks.check_static_nrega_script(df, "P0 P1 P2 P3 Q A P0 Z1 Z2", "got_static_nrega")
	# Get static NREGA script indicator for A 
	df = checks.get_static_nrega_script_indicator(df, "got_static_nrega")
	
	# Check static NREGA script for B
	df = checks.check_static_nrega_script(df, "P0 P1 P2 P3 Q A P0 Z1 Z2", "got_second_static_nrega")
	# Get static NREGA script indicator for B
	df = checks.get_static_nrega_script_indicator(df, "got_second_static_nrega")
	
	# Initialize script
	df['day1'] = ''
	# Allocate static scripts
	df = sets.set_static_scripts(df)
	
	# Format final df
	df = formatters.format_static_df(df)
	# Keep only columns that are relevant to BTT in static data
	df = formatters.format_final_df(df)
	# Add test calls to static script
	df = sets.set_test_calls(df)  
	# Output 
	df.to_csv(local_output_path, index = False)
	# S3 upload
	helpers.s3_upload(local_output_path, s3_output_path)

	
def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('window_length', type = int, help ='Time window in days from today for NREGA lookback', default = 7)
	parser.add_argument('pilot', type = int, help = 'Whether to make script for pilot data or production data', default = 0)
	parser.add_argument('dynamic', type = int, help ='Whether to make dynamic script', default = 1)
	parser.add_argument('join', type = int, help ='Whether to join the two or not...', default = 0)
	parser.add_argument('static', type = int, help ='Whether to make static script', default = 0)
	parser.add_argument('local', type = int, help ='Get transactions from local', default = 1)
	
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	dynamic = args.dynamic
	static = args.static
	pilot = args.pilot
	local = args.local
	join = args.join
	
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	local_output_path = './output/callsequence_{}.csv'.format(today)
	merge_output_path = './output/nregamerge_{}.csv'.format(today)
	s3_output_path = 'scripts/callsequence_{}.csv'.format(today)
	
	if static == 1: get_static_call_script(local_output_path, s3_output_path, pilot)
	if dynamic == 1: get_dynamic_call_script(local_output_path, s3_output_path, pilot, local)
	if join == 1: df = joins.join_dynamic_static(local_output_path, s3_output_path, pilot, local)
	
	helpers.send_mail('GMA Update: The script was successfully created and uploaded ...take a break. Relaaaaax. See you tomorrow!')
		

if __name__ == '__main__':
	
	main()		

# Pending
# Rejection reason
# Change file name convention to call date
# Do Alembic migrations