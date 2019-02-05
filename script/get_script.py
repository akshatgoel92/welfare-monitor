#------------------------------------------------------------------#
# Author: Akshat Goel
# Date: 8th August 2018
# Python version: 3.6.3
# Dependencies:

# [Only modules outside Python standard listed]
# 1) scrapy 
# 2) pandas 
# 3) numpy 
# 4) Selenium
#------------------------------------------------------------------#
import pandas as pd
import numpy as np
import argparse
import re
import random

from sqlalchemy import *
from common import helpers 

random.seed(90123)

def load_and_clean_scraped_data(path = './nrega_output/btt_pilot_2/btt_pilot_2_scrape.csv', 
					 			end_date = '2019-02-05', start_date = '2019-01-15'):
	''''Load and clean the scraped data-set.'''
	
	# Load and then filter the scraped data to the given time window
	scraped = pd.read_csv(path, low_memory = False)
	scraped['date'] = pd.to_datetime(scraped['transact_date'])
	scraped = scraped.loc[(scraped['date'] > start_date) & (scraped['date'] <= end_date)]
	
	return(scraped)

def get_transaction_dates_and_hh_totals(scraped):
	'''Get the transaction dates and household totals.'''
	
	scraped['date'] = scraped['processed_date']
	scraped.loc[scraped['date'].isna(), 'date'] = scraped['transact_date']
	scraped['date'] = pd.to_datetime(scraped['date'])
	
	transact_date = scraped.groupby('jcn')['date'].max()
	transact_date = transact_date.astype(str)

	total_amount_due = scraped.groupby('jcn')['credit_amt_due'].sum()
	total_amount_actual = scraped.groupby('jcn')['credit_amt_due'].sum()

	return(transact_date, total_amount_due, total_amount_actual)

def get_stage(scraped, engine):
	'''	
	Get the most recent payment process stage that the FTO is at.
	Then get whether the payment process stage corresponds to the block office
	or the bank. We want to be conservative with HH information.
	1: Block, 2: Bank, 3: Rejected'''

	fto_stage = pd.read_sql('fto_current_stage', con = engine)
	scraped = pd.merge(scraped, fto_stage, how = 'left', on = ['fto_no'])
	scraped.loc[scraped['status'].isna(), 'stage'] = 1
	scraped.loc[scraped['status'] == 'Processed', 'stage'] = 2
	scraped.loc[scraped['status'] == 'Rejected', 'stage'] = 3
	
	stage = scraped.groupby('jcn')['stage'].min()
	
	return(stage)

def get_nrega(date, amount, stage):
	'''Get calling script.'''

	data = pd.concat([date, amount, stage], axis = 1)
	data.columns = ['transact_date', 'amount', 'stage']
	data.rename_axis('jcn')
	
	data.loc[data['stage'] == 1, 'stage'] = "D E"
	data.loc[data['stage'] == 2, 'stage'] = "D F"
	data.loc[data['stage'] == 3, 'stage'] = "D G"
	data = data.reset_index()

	return(data)

def get_health(script):
	'''Randomize the health scripts according the scheme that the 
	field team specified.'''

	scenes = ['P', 'Q', 'R', 'S', 'T', 'U', 'V']
	options = ['Y Z ' + scene for scene in scenes]
	sequence = pd.DataFrame([random.sample(options, k = len(options)) for call in range(len(script))], 
							index = script.index)
	sequence = sequence.rename(columns = lambda x : 'day' + str(x + 1))
	script = pd.concat([script, sequence], axis = 1)
	
	return(script)



def load_and_clean_field(path = './nrega_output/btt_pilot_2/btt_pilot_2_1.csv', village_code = 'CH-016-008-026-001'):
	'''Load and clean field data-set.'''

	# Load field and get rid of missing values
	field = pd.read_csv(path)
	
	# Clean other columns
	field['sky_phone'] = field['sky_phone'].astype(int).astype(str)
	field['id'] = field['id'].astype(int).astype(str)
	field['time_pref'] = field['time_pref'].astype(str)

	# Fill in missing values
	field['sky_phone'].fillna('', inplace = True)
	field['id'].fillna('', inplace = True)
	field['time_pref'].fillna('', inplace = True)
	field['jcn'].fillna('', inplace = True)
	field['jcn_extra'].fillna('', inplace = True)
	
	# Generate field JCN
	field.loc[field['jcn'].isna(), 'jcn'] = ''
	field['jcn_field'] = field['jcn'].apply(lambda x: village_code + '/' + re.split('\W+', x)[-1].lstrip('0'))
	field.loc[field['jcn_field'] == village_code + '/', 'jcn_field'] = ''
	
	# Drop old JCNs
	field.drop('jcn', axis = 1, inplace = True)
	field.rename({'jcn_field': 'jcn'}, inplace = True, axis = 1)
	
	return(field)

def put_nrega_script(script):
	'''Put NREGA script in place of health calls on the days that an individual worked.'''

	script = pd.merge(field, script, 
					  how = 'left', on = ['jcn'], indicator = 'script_merge')
	script.loc[(~script['stage'].isna()), 'day1'] = script['stage']

	return(script)

def clean_final_script(script):
	'''Clean the final script.'''

	cols = ['id', 'sky_phone', 'amount', 'transact_date', 'time_pref', 
			'time_pref_label', 'day1', 'day2', 'day3', 'day4', 'day5', 'day6']

	script = script[cols]

	return(script)

def add_dummy_calls(field, dummy = './nrega_output/dummy_pilot_arang_pilot.csv'):
	'''Add dummy calls.'''

	dummy = pd.read_csv(dummy)
	field = pd.concat([field, dummy])
	field.loc[(field['id'].astype(int) > 8000) & (script['day1'] == 'D E'), 'day1'] = 'D G'
	
	return(field)


def execute(path, file_to, field, engine):
	'''Execute the script.'''
	
	# Household data preparation
	hh_data = load_and_clean_scraped_data()

	# Prepare NREGA data
	date, amount_due, amount_actual = get_transaction_dates_and_hh_totals(hh_data)
	stage = get_stage(hh_data, engine)
	nrega = get_nrega(date, amount_due, stage)
	
	# Clean field data
	field = load_field()
	field = clean_field(field)
	field = add_dummy_calls(field = field)

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
    parser.add_argument('file_field', type=str, help='Whether to write to Dropbox')
    
    # Parse arguments
    args = parser.parse_args()
    file_from = args.file_from
    file_to = args.file_to
    file_field = args.file_field() 
    
    # Create SQL data-base connection 
   	user, password, host, db = helpers.sql_connect().values()
    engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
    # Call functions
    execute(path = file_from, file_to = file_to)