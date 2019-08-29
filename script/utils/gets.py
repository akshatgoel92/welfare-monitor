import pandas as pd
import numpy as np
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *


def get_camp_data(pilot):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = '''SELECT id, phone, jcn, jc_status, time_pref, time_pref_label FROM enrolment_record WHERE pilot = {};'''.format(pilot)
	
	try: 
		
		gens_field = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field = pd.concat([gen for gen in gens_field])
		conn.close()
		
	except Exception as e:
		
		er.handle_error(error_code ='26', data = {})
		sys.exit()
		conn.close()
		
	return(df_field)


def get_transactions(start_date, end_date):
	
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_joined_tables = '''SELECT a.jcn, a.transact_ref_no, a.transact_date, a.processed_date, a.credit_amt_due, 
						   a.credit_amt_actual, a.status, a.rejection_reason, a.fto_no, b.stage 
						   FROM transactions a INNER JOIN fto_queue b ON a.fto_no = b.fto_no 
						   WHERE a.transact_date BETWEEN '{}' and '{}';'''.format(start_date, end_date)

	try: 
		
		gens_transactions = pd.read_sql(get_joined_tables, con = conn, chunksize = 1000)
		transactions = pd.concat([gen for gen in gens_transactions])	
		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='27', data = {})
		sys.exit()
		conn.close()

	return(transactions)


def get_alternate_transactions(local = 1, filepath = './output/transactions_alt.csv'):
	
	transactions_alt = pd.read_csv(filepath)
	transactions_alt = transactions_alt[['transact_ref_no', 'status', 'processed_date', 'rejection_reason']]
			
	return(transactions_alt)


def get_static_script_look_backs(df):
	
	# Look-back for welcome script
	df = utils.check_welcome_script(df)
	# Look back for static NREGA introduction
	df = utils.check_static_nrega_script(df)
	# Look back for formatting indicators
	df = utils.format_indicators(df)
	# Return statement
	return(df)


def get_static_call_script(df):
	
	# Initialize script
	df['amount'] = ''
	df['day1'] = ''
	df['transact_date'] = ''
	df['rejection_reason'] = ''
	
	# Allocate static scripts
	# Keep only columns that are relevant to BTT in static data
	# Add test calls to static script
	df = utils.set_static_scripts(df)
	df = utils.format_df(df, 1)
	df = utils.add_test_calls(df)
		
	return(df)


def get_rejection_reasons(filepath = './script/rejection_reasons.json'):
	
	with open(filepath) as f:
		
		rejection_reasons = json.load(f)
		rejection_reasons = pd.DataFrame(from_dict(rejection_reasons, orient = 'index')
	
	return(df)