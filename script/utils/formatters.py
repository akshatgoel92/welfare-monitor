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


def format_transactions_jcn(transactions):
	
	transactions['jcn'] = transactions[['CH-033' not in x for x in transactions['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:])
	transactions['jcn'] = transactions['jcn'].apply(format_jcn)
	
	return(transactions)
	

def format_camp_jcn(df_field):
		
	df_field['jcn'] = df_field['jcn'].fillna('').apply(lambda x: x.replace('--', '-'))
	df_field['jcn'] = df_field[['CH-033' not in x for x in df_field['jcn']]]['jcn'].apply(lambda x: 'CH-033' + x[2:] if x != '' else '')
	df_field['jcn'] = df_field['jcn'].apply(format_jcn)
	
	return(df_field)
	

def format_static_df(df):

	df.rename(columns={'credit_amt_actual':'amount'}, inplace = True)
	df['rejection_reason'] = ''
	df['transact_date'] = ''
	df['amount'] = ''
	
	return(df)


def format_final_df(df):
		
	df = df[['id', 'phone', 'time_pref', 'time_pref_label', 'amount', 'transact_date', 'rejection_reason', 'day1']] 
	df.drop_duplicates(['id'], inplace = True)
	df.reset_index(inplace = True)
	df = df.sample(frac = 1)
	df.drop(['index'], axis = 1, inplace = True)
	
	return(df)