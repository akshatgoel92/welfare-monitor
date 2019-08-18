import pandas as pd
import numpy as np
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *


def format_jcn(old_jcn):
	
	if not old_jcn: return ''
	old_jcn.replace(" ", "")
	new_jcn = ''
	
	for index, char in enumerate(old_jcn):
		
		if char.isalpha() or char.isdigit(): 
			new_jcn += char
		
		elif char == '/': 
			new_jcn += old_jcn[index:]
			break
		
		elif char == '-':
			
			new_jcn += char
			next_dash_index = old_jcn.find('-', index + 1)
			index_difference = next_dash_index - index
			
			if next_dash_index == -1: 
				index_dash_difference = old_jcn.find('/') - index
				
				if index_dash_difference != 4: 
					for iter in range(4 - index_dash_difference): 
						new_jcn += '0'
			
			elif index_difference != 4:
				for iter in range(4 - index_difference): 
					new_jcn += '0'
	
	return(new_jcn)
	


def check_welcome_script(df, welcome_code = "P0 P1 P2 00", welcome_code_alt = "P0 P1 P2 00 P0"):
	
	query = "SELECT id FROM scripts where day1 = '{}' OR day1 = '{}';"
	engine = helpers.db_engine()
	
	try: welcome_script = pd.read_sql(query.format(welcome_code, welcome_code_alt), con = engine)
	
	except Exception as e: 
		print(e)
		sys.exit()
	
	df = pd.merge(df, welcome_script, how = 'left', on = 'id', indicator = 'got_welcome')
		
	return(df)


def check_static_nrega_script(df, static_script_code = "P0 P1 P2 P3 Q A P0 Z1 Z2"):
	
	engine = helpers.db_engine()
	
	try: static_nrega = pd.read_sql("SELECT id FROM scripts WHERE day1 = '{}'".format(static_script_code), con = engine)
	
	except Exception as e:
		print(e)
		sys.exit()
	
	df = pd.merge(df, static_nrega, how = 'left', on = 'id', indicator = 'got_static_nrega')
		
	return(df)


# Still need to complete
def set_nrega_rejection_reason(df):
	
	return(df)


def format_indicators(df):
	
	df['got_welcome_int'] = 0
	df['got_static_nrega_int'] = 0
	
	df.loc[(df['got_static_nrega'] == 'both'), 'got_static_nrega_int'] = 1
	df.loc[(df['got_welcome'] == 'both'), 'got_welcome_int'] = 1
	
	df.drop(['got_static_nrega', 'got_welcome'], inplace = True, axis = 1)
	df = df.rename(columns={'got_welcome_int': 'got_welcome', 'got_static_nrega_int': 'got_static_nrega'})
	
	return(df)
	

def set_static_scripts(df):
		
	# Initialize script
	df['day1'] = ''
	
	# Not got welcome script and not got static NREGA introduction so should get the welcome script
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 0), 'day1'] = "P0 P1 P2 00 P0"
	
	# Got welcome script but not got static NREGA introduction so should get static NREGA introduction
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 1), 'day1'] = "P0 P1 P2 P3 Q A P0 Z1 Z2"
	
	# Proportional wages
	df.loc[(df['_merge'] == 'right_only') & (df['got_static_nrega'] == 1) & (df['transact_date'].isna()) & (df['status'].isna()), 'day1'] = 'P0 P1 P2 P3 Q B P0 Z1 Z2'
	
	return(df)


def set_nrega_scripts(df):
	
	# Dynamic NREGA scripts for FTOs at the block office
	df.loc[(df['stage']=='fst_sig_not') | (df['stage']=='fst_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	df.loc[(df['stage']=='sec_sig_not') | (df['stage']=='sec_sig'), 'day1'] = 'P0 P1 P2 P3 R CA CB CC P0 Z1 Z2'
	
	# Dynamic NREGA scripts for unprocessed FTOs at the bank
	df.loc[(df['stage']=='sb') | (df['stage']=='pp') | (df['stage']=='P'), 'day1'] = 'P0 P1 P2 P3 R DA DB DC P0 Z1 Z2'
		
	# Dynamic NREGA scripts for transactions which have been processed
	df.loc[(df['status']=='Processed') & (df['stage']=='pb'), 'day1'] = 'P0 P1 P2 P3 R EA EB EC P0 Z1 Z2'
	df.loc[(df['status']=='Rejected') & (df['stage']=='pb'), 'day1'] = 'P0 P1 P2 P3 R FA FB FC P0 Z1 Z2'
	
	return(df)


def set_nrega_hh_amounts(df):
	
	# Replace amount with actual amount if it exists else with the amount due
	df['amount'] = np.where(df['credit_amt_actual'] != 0, df['credit_amt_actual'], df['credit_amt_due'])
	df['amount'] = df.groupby('id')['amount'].transform('sum')
	df['amount'] = df['amount'].replace(0, np.nan)
	
	return(df)
	
	
def set_nrega_hh_dates(df):
	
	# Replace transact_date with processed_date if transaction has been processed and then format
	df['transact_date'] = np.where(~df['processed_date'].isna(), df['processed_date'], df['transact_date'])
	df['transact_date'] = pd.to_datetime(df['transact_date'], format = '%Y/%m/%d', dayfirst = True)
	df['transact_date'] = df.groupby('id')['transact_date'].transform('max')
	
	return(df)


def format_df(df):
	
	df = df[['id', 'phone', 'transact_date', 'time_pref', 'time_pref_label', 'amount', 'transact_date', 'rejection_reason', 'day1']] 
	df.drop_duplicates(['id'], inplace = True)
	df.reset_index(inplace = True)
	df = df.sample(frac=1)
	
	return(df)


def add_test_calls(df):
	
	return(df)