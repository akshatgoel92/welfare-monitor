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


def get_camp_data(pilot):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_field_data = '''SELECT id, phone, jcn, jc_status, health_category, time_pref, time_pref_label FROM enrolment_record WHERE pilot = {};'''.format(pilot)
	
	try: 
		
		gens_field = pd.read_sql(get_field_data, con = conn, chunksize = 1000)
		df_field = pd.concat([gen for gen in gens_field])
		conn.close()
		
	except Exception as e:
		
		er.handle_error(error_code ='26', data = {})
		sys.exit()
		conn.close()
		
	return(df_field)


def get_transactions(start_date, end_date, table_name = 'transactions_alt'):
	
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_joined_tables = '''SELECT a.jcn, a.transact_ref_no, a.transact_date, a.processed_date, a.credit_amt_due, 
						   a.credit_amt_actual, a.status, a.rejection_reason, a.fto_no, b.stage 
						   FROM {} a INNER JOIN fto_queue b ON a.fto_no = b.fto_no 
						   WHERE a.transact_date BETWEEN '{}' and '{}';'''.format(table_name, start_date, end_date)

	try: 
		
		gens_transactions = pd.read_sql(get_joined_tables, con = conn, chunksize = 1000)
		transactions = pd.concat([gen for gen in gens_transactions])	
		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='27', data = {})
		sys.exit()
		conn.close()

	return(transactions)


def get_static_script_look_backs(df):
	
	df = utils.check_welcome_script(df)
	df = utils.check_static_nrega_script(df)
	
	df = utils.format_indicators(df)
	
	return(df)


def get_rejection_reasons(filepath = './script/data/rejection_reason.json'):
	
	with open(filepath) as f: rejection_reasons = json.load(f)
	
	rejection_reasons = pd.DataFrame.from_dict(rejection_reasons, orient = 'index')
	rejection_reasons.reset_index(inplace = True)
	rejection_reasons.columns = ['rejection_reason', 'day1'] 
	
	return(rejection_reasons)


def get_health_data_file(camp_data_file = 'health/health_schedule_090919.csv'):
	
	df = pd.read_csv(helpers.get_object_s3(camp_data_file))
	df.dropna(inplace = True)
	
	return(df)