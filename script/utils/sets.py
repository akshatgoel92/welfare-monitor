import pandas as pd
import numpy as np
import random
import json
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *


	
def set_static_scripts(df):
			
	df['day1'] = df.apply(lambda row: row['health'] if row['got_static_nrega'] == 1 or row['got_second_static_nrega'] == 1 or row['got_welcome'] == 1 else "P0 P1 P2 00 P0", axis = 1)
	
	return(df)


def set_nrega_scripts(df):
	
	# Dynamic NREGA scripts for FTOs at the block office
	df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	df.loc[(df['stage']=='sec_sig_not') | (df['stage']=='sec_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	
	# Dynamic NREGA scripts for unprocessed FTOs at the bank
	df.loc[(df['stage']=='sb') | (df['stage']=='pp') | (df['stage']=='P'), 'day1'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
		
	# Dynamic NREGA scripts for transactions which have been processed
	df.loc[(df['stage']=='pb'), 'day1'] = 'P0 P1 P2 P3 R EA EB EC P0 Z1 Z2'
	
	return(df)


def set_nrega_hh_amounts(df):
	
	# Replace amount with actual amount if it exists else with the amount due
	df['amount'] = np.where(df['credit_amt_actual'] != 0, df['credit_amt_actual'], df['credit_amt_due'])
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	return(df)
	
	
def set_nrega_hh_dates(df):
	
	# Replace transact_date with processed_date if transaction has been processed and then format
	df['transact_date'] = df.apply(lambda row: row['transact_date'] if row['status'] == '' else row['processed_date'], axis = 1)
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	return(df)


def set_nrega_rejection_reason(df, rejection_reasons):
	
	df = pd.merge(df, rejection_reasons, how = 'left', on = 'rejection_reason', indicator = 'rejection_reason_merge')
	
	df['day1'] = ''
	df['day1'] = df.apply(lambda row: row['day1_y'] if row['rejection_reason'] !='' else row['day1'], axis = 1)
	df['day1'] = df.apply(lambda row: 'P0 P1 P2 P3 R FF1 FF2 FA5 FB FB5 FC P0 Z1 Z2' if row['rejection_reason'] != '' and row['day1'] == '' else row['day1'], axis = 1)
	
	df['day1'] = df.apply(lambda row: row['day1_x'] if row['day1'] == '' else row['day1'], axis = 1)
	df.drop(['rejection_reason_merge', 'day1_x', 'day1_y'], inplace = True, axis = 1)
	
	return(df)


def set_static_test_calls(df, filepath = './script/data/test_static_calls.json'):
	
	with open(filepath, 'r') as f:
		test_calls = pd.DataFrame.from_dict(json.load(f), orient = 'index')
		
	test_calls['amount'].fillna('', inplace = True)
	test_calls['transact_date'].fillna('', inplace = True)
	test_calls['rejection_reason'].fillna('', inplace = True)
	
	df = pd.concat([df, test_calls])
	
	return(df)