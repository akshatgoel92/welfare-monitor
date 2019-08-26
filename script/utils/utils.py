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
	

def format_df(df, static):
	
	if static == 1: 
		df.rename(columns={'credit_amt_actual':'amount'}, inplace = True)
		df['transact_date'] = ''
		df['amount'] = ''
	
	df = df[['id', 'phone', 'time_pref', 'time_pref_label', 'amount', 'transact_date', 'rejection_reason', 'day1']] 
	df.drop_duplicates(['id'], inplace = True)
	df.reset_index(inplace = True)
	df = df.sample(frac=1)
	df.drop(['index'], axis = 1, inplace = True)
	
	return(df)


def add_test_calls(df):
	
	test_calls = pd.read_excel('./script/test_calls.xlsx')
	df = pd.concat([df, test_calls])
	
	return(df)


def output_df(df, static, dynamic, join):
	
	# Output scripts
	if join == 1:
		df = pd.concat([df_dynamic, df_static])
		df.to_csv(local_output_path, index = False)
	
	elif static == 1: df_static.to_csv(local_output_path, index = False)
	elif dynamic == 1: df_dynamic.to_csv(local_output_path, index = False)
	
	return(join, static, dynamic)