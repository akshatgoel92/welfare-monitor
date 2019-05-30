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

def get_stage(scraped, bank_pending):
	'''	
	Get the most recent payment process stage that the FTO is at.
	Then get whether the payment process stage corresponds to the block office
	or the bank. We want to be conservative with HH information.
	1: Block, 2: Bank, 3: Rejected'''
	
	scraped = pd.merge(scraped, bank_pending, how = 'left', on = ['fto_no'], indicator = 'bank_pending')
	scraped.loc[(scraped['status'].isna()) & (scraped['bank_pending'] == 'left_only'), 'stage'] = 1
	scraped.loc[(scraped['status'].isna()) & (scraped['bank_pending'] == 'both'), 'stage'] = 2
	scraped.loc[scraped['status'] == 'Processed', 'stage'] = 3
	scraped.loc[scraped['status'] == 'Rejected', 'stage'] = 4
	stage = scraped.groupby('jcn')['stage'].min()
	
	return(stage)


def get_nrega(date, amount, stage):
	'''Get calling script.'''

	data = pd.concat([date, amount, stage], axis = 1)
	data.columns = ['transact_date', 'amount', 'stage']
	data.rename_axis('jcn')
	
	data.loc[data['stage'] == 1, 'stage'] = "B E J"
	data.loc[data['stage'] == 2, 'stage'] = "B F J"
	data.loc[data['stage'] == 3, 'stage'] = "B G J"
	data.loc[data['stage'] == 4, 'stage'] = "B H J"
	data = data.reset_index()
	
	# Randomize the introductions for the pilot
			
	data['intro'] = ''
	data_sample = data.sample(frac = 0.5)
	data.loc[data_sample.index.isin(data.index), 'intro'] = 'A2 '
	data.loc[data['intro'] == '', 'intro'] = 'A3x A3y A1 '
	data['stage'] = data['intro'] + data['stage']
	data.drop(['intro'], inplace = True, axis = 1)

	return(data)


def load_and_clean_field(path = './nrega_output/btt_pilot_2/btt_pilot_2_2.csv'):
	'''Load and clean field data-set.'''

	village_codes = {'./nrega_output/btt_pilot_2/btt_pilot_2_1.csv': 'CH-016-008-026-001',
	'./nrega_output/btt_pilot_2/btt_pilot_2_2.csv': 'CH-016-014-103-001'}

	village_code = village_codes[path]

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
	
	# JCN household number
	field['hh_no'] = field['jcn'].apply(lambda x: re.split(r'(\d+)', x)[-2:])	
	field['hh_no'] = field['hh_no'].apply(lambda x: '-'.join(x) if x[-1] != '' else x[0])
	
	# JCN field-work
	field['jcn_field'] = field['hh_no'].apply(lambda x: village_code + '/' + x)
	field.loc[field['jcn_field'] == village_code + '/', 'jcn_field'] = ''
	
	# Drop old JCNs
	field.drop('jcn', axis = 1, inplace = True)
	field.rename({'jcn_field': 'jcn'}, inplace = True, axis = 1)

	# Now we creat a flag for the extra JCN script
	field['jcn_script'] = ''
	field.loc[field['jcn_extra'] == 'No extra script', 'jcn_script'] = ''
	field.loc[field['jcn_extra'] == 'JCN renewal script', 'jcn_script'] = ' C '
	field.loc[field['jcn_extra'] == 'JCN generation script', 'jcn_script'] = ' D '  

	return(field)

def merge_field_scrape(field, nrega):
	'''Merge field data with scraped data.'''

	script = pd.merge(field, nrega, 
					  how = 'left', on = ['jcn'], indicator = 'script_merge')

	script['stage'] = script['stage'].astype(str)

	return(script)

def get_health(script):
	'''Randomize the health scripts according the scheme that the 
	field team specified.'''

	scenes = random.sample(['O', 'P', 'Q', 'R', 'S', 'T'], 6)
	option_1 = ['A4 N ' + scene for scene in scenes]
	option_2 = ['A3a A3b A3c N ' + scene for scene in scenes]
	
	sequence_1 = pd.DataFrame([option_1 for call in range(len(script)//2)])
	sequence_2 = pd.DataFrame([option_2 for call in range(len(script)//2, len(script))])
	
	sequence = pd.concat([sequence_1, sequence_2])
	sequence.reset_index(inplace = True, drop = True)
	sequence = sequence.rename(columns = lambda x : 'day' + str(x + 1))
	
	script = pd.concat([script, sequence], axis = 1)
	
	return(script, scenes)

def jcn_renewal(script):
	'''This is the JCN renewal script.'''

	
	script['stage_1'] = ''
	script['stage_2'] = ''
	script['stage_1'] = script['stage'].apply(lambda x: x[0:4] if len(x) == 8 else x[0:10])
	script['stage_2'] = script['stage'].apply(lambda x: x[5:] if len(x) == 8 else x[11:])
	script['stage'] == script['stage_1'] + script['jcn_script'] + script['stage_2']

	return(script)


def put_nrega_script(script, scenes):
	'''Put NREGA script in place of health calls for individuals who worked.'''

	nrega_day = str([index for (index, script) in enumerate(scenes) if script == 'O'][0] + 1)
	script.loc[~(script['stage'] == 'nan'), 'day' + nrega_day] = script['stage']

	return(script)


def clean_final_script(script):
	'''Clean the final script and retain only the correct columns.'''

	cols = ['id', 'sky_phone', 'amount', 'transact_date', 'time_pref', 'day1', 'day2', 'day3', 'day4', 'day5', 'day6']

	script = script[cols]
	script['amount'] = script['amount'].astype(str).fillna('')
	script['transact_date'] = script['transact_date'].astype(str).fillna('')
	script['time_pref'] = script['transact_date'].astype(str).fillna('')

	return(script)


def add_dummy_calls(script, dummy_path = './nrega_output/btt_pilot_2/dummy_calls_btt_pilot_2.csv'):
	'''Add dummy calls.'''

	dummy = pd.read_csv(dummy_path)
	dummy = dummy.loc[~dummy['id'].isna()]
	dummy['id'] = dummy['id'].astype(int).astype(str)
	dummy['sky_phone'] = dummy['sky_phone'].astype(int).astype(str)
	dummy['time_pref'] = dummy['time_pref'].astype(str)
	script = pd.concat([script, dummy])
	script = script.reset_index(drop = True)
		
	return(script)

def append_files(path_1, path_2, path):

	data_1 = pd.read_csv(path_1)
	data_2 = pd.read_csv(path_2)
	data = pd.concat(data_1, data_2)
	data.to_csv(path, index = False)
	return(data)

def execute(path, field, file_to = './nrega_output/btt_pilot_2/script_2'):
	'''Execute the script.'''
	
	# Household data preparation
	hh_data = load_and_clean_scraped_data()
	bank_pending = pd.read_excel('./nrega_output/btt_pilot_2/fto_pending_bank.xlsx')

	# Prepare NREGA data
	date, amount_due, amount_actual = get_transaction_dates_and_hh_totals(hh_data)
	stage = get_stage(hh_data, bank_pending)
	nrega = get_nrega(date, amount_due, stage)
	
	# Clean field data
	field = load_and_clean_field()
	
	# Script preparation
	script = merge_field_scrape(field, nrega)
	script, scenes = get_health(script)
	script = put_nrega_script(script, scenes)
	script = jcn_renewal(script)
	
	# Renew the JCNs here
	script = clean_final_script(script)
	script = add_dummy_calls(script = script)
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
      
    # Call functions
    execute(path = file_from, file_to = file_to)