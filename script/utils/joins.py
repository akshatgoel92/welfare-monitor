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