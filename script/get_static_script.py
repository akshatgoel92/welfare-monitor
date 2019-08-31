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


def get_static_call_script(local_output_path, s3_output_path, pilot = 0):

	# Prepare camp data
	camp = gets.get_camp_data(pilot)
	df = formatters.format_camp_jcn(camp)
	
	# Check welcome script
	df = checks.check_welcome_script(df)
	df = checks.get_welcome_script_indicator(df)
	
	# Check static NREGA script for A 
	df = checks.check_static_nrega_script(df, "P0 P1 P2 P3 Q A P0 Z1 Z2", "got_static_nrega")
	df = checks.get_static_nrega_script_indicator(df, "got_static_nrega")
	
	# Check static NREGA script for B
	# Get static NREGA script indicator for B
	df = checks.check_static_nrega_script(df, "P0 P1 P2 P3 Q B P0 Z1 Z2", "got_second_static_nrega")
	df = checks.get_static_nrega_script_indicator(df, "got_second_static_nrega")
	
	# Allocate scripts
	df['day1'] = ''
	df = sets.set_static_scripts(df)
	
	# Final cleaning
	df = formatters.format_static_df(df)
	df = formatters.format_final_df(df)
	df = sets.set_static_test_calls(df)
	  
	df.to_csv(local_output_path, index = False)
	helpers.upload_s3(local_output_path, s3_output_path)
	
def main():

	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('--window_length', type = int, help ='Time window in days from today for NREGA lookback', default = 7)
	parser.add_argument('--pilot', type = int, help = 'Whether to make script for pilot data or production data', default = 0)
	args = parser.parse_args()
	
	# Parse arguments
	window_length = args.window_length
	pilot = args.pilot
	today = str(datetime.today().date())
	start_date = helpers.get_time_window(today, window_length)
	
	file_name_today = datetime.strptime(today, '%Y-%m-%d').strftime('%d%m%Y')
	local_output_path = './output/callsequence_{}.csv'.format(file_name_today)
	
	merge_output_path = './output/nregamerge_{}.csv'.format(file_name_today)
	s3_output_path = 'tests/callsequence_{}.csv'.format(file_name_today)
	
	if static == 1: get_static_call_script(local_output_path, s3_output_path, pilot)
	if dynamic == 1: get_dynamic_call_script(local_output_path, s3_output_path, pilot, local)
	if join == 1: df = joins.join_dynamic_static(local_output_path, s3_output_path, pilot, local)
	
	subject = 'GMA Update: The script was successfully created and uploaded ...take a break. Relaaaaax. See you tomorrow!'
	message = 'The file name is {}'.format(s3_output_path)
	helpers.send_email(subject, message)
		

if __name__ == '__main__':
	
	main()		

# Pending
# Fix date issue issue
# Change file name convention to call date
# Do Alembic migrations