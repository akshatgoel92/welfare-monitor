import pandas as pd
import numpy as np
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *




def join_alternate_transactions(transactions, transactions_alt):
	
	cols = transactions.columns.tolist()
	cols.remove('transact_ref_no')
	
	transactions = pd.merge(transactions, transactions_alt, how = 'left', on = ['transact_ref_no'], indicator = True)
	transactions['rejection_reason_x'] = transactions['rejection_reason_y']
	transactions['processed_date_x'] = transactions['processed_date_y']
	transactions['status_x'] = transactions['status_y']
	
	drop_cols = ['processed_date_y', 'transact_ref_no', 'status_y', 'rejection_reason_y', '_merge']
	transactions.drop(drop_cols, inplace = True, axis = 1)
	transactions.columns = cols
	
	return(transactions)


def join_camp_data(transactions, df_field_data):

	
	df = pd.merge(transactions, df_field_data, on='jcn', how='outer', indicator=True)
	df = df.loc[df['_merge'] != 'left_only']
	
	df = df[['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
			 'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
			 'time_pref_label', '_merge']]
	
	df.columns = ['jcn', 'transact_date', 'processed_date', 'credit_amt_due', 'credit_amt_actual', 
				  'status', 'rejection_reason', 'fto_no', 'stage', 'id', 'phone', 'time_pref', 
				  'time_pref_label', '_merge']
	
	if df.empty: er.handle_error(error_code ='29', data = {})
	
	return(df)


def join_static_dynamic(df_dynamic, df_static):

	df = pd.concat([df_dynamic, df_static])
	df.to_csv(local_output_path, index = False)
	
	return(df)

	
def set_static_scripts(df):
			
	# Not got welcome script and not got static NREGA introduction so should get the welcome script
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 0), 'day1'] = "P0 P1 P2 00 P0"
	
	# Got welcome script but not got static NREGA introduction so should get static NREGA introduction
	df.loc[(df['got_static_nrega'] == 0) & (df['got_welcome'] == 1), 'day1'] = "P0 P1 P2 P3 Q A P0 Z1 Z2"
	
	# Proportional wages
	df.loc[(df['got_static_nrega'] == 1), 'day1'] = 'P0 P1 P2 P3 Q B P0 Z1 Z2'
	
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


# Still need to complete
def set_nrega_rejection_reason(df):
	
	return(df)


# Change to JSON	
def set_test_calls(df):
	
	test_calls = pd.read_excel('./script/test_calls.xlsx')
	df = pd.concat([df, test_calls])
	
	return(df)