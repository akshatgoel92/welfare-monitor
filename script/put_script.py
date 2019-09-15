from sqlalchemy.types import Integer, String
from sqlalchemy.dialects.mysql import TINYINT
from common import errors as er
from common import helpers
from db import update
import pandas as pd
import argparse
import datetime
import sys
import re


def get_script_data_list(prefix = "scripts/call", suffix = ".csv"):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	
	return(objects)
	

def get_script_data_file(script_data_file):
	
	df = pd.read_csv(helpers.get_object_s3(script_data_file))
	
	return(df)


def add_s3_file_name(df, script_data_file):
	
	df['file_name_s3'] = script_data_file
	
	return(df)


def add_s3_date(df, script_data_file): 
	
	date = re.findall('callsequence_(.*).csv', script_data_file)[0]
	date = datetime.datetime.strptime(date, '%d%m%Y')
	date = datetime.datetime.strftime(date, '%Y-%m-%d') 
	
	df['file_upload_to_s3_date'] = date
	
	return(df)
	

def get_script_data(script_data_list):
	 
	 df = [get_script_data_file(script_data_file) for script_data_file in script_data_list]
	 
	 return(df)


def add_script_data_columns(df, script_data_list):
	
	df = [add_s3_file_name(data, file_name) for data, file_name in zip(df, script_data_list)]
	df = [add_s3_date(data, file_name) for data, file_name in zip(df, script_data_list)]
	df = pd.concat(df, ignore_index = True)
	
	# Change these columns
	df = df[['id', 'phone', 'time_pref', 'time_pref_label', 'amount', 
			'transact_date', 'rejection_reason', 'day1', 
			'file_name_s3', 'file_upload_to_s3_date']]
	
	df['insert_date'] = str(datetime.datetime.today())
	
	# Drop staff members
	df = df.loc[df['id'] > 1000]
	 
	return(df)


def put_scripts(scripts):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	if not scripts.empty: 
	
		try: 
			
			scripts.to_sql('scripts', if_exists = 'replace', con = engine, index = False, chunksize = 100, 
						   dtype = {'id': Integer(), 'phone': String(50), 'time_pref': String(50), 'time_pref_label': String(50), 
						   			'amount': Integer(), 'transact_date': String(50), 'rejection_reason': String(50), 
									'day1': String(50), 'file_name_s3': String(50), 
									'file_upload_to_s3_date': String(50), 
									'insert_date': String(50)})
		
		except Exception as e: 
		
			er.handle_error(error_code ='23', data = {})
			sys.exit()
		
	return


def make_script_primary_key():
	
	engine = helpers.db_engine()
	conn = engine.connect()
		
	try: 
		add_primary_key = "ALTER TABLE scripts ADD PRIMARY KEY(id, file_name_s3(50), file_upload_to_s3_date(50));"
		engine.execute(add_primary_key)
	
	except Exception as e: 
		er.handle_error(error_code ='25', data = {})
		sys.exit()
	
	return
	

def main():
	
	script_data_list = get_script_data_list(suffix = '.csv')
	df = get_script_data(script_data_list)
	df = add_script_data_columns(df, script_data_list)
		
	put_scripts(df)
	make_script_primary_key()

	subject = 'GMA Update: The scripts table data has been refreshed...'
	message = ''
	helpers.send_email(subject, message)

	
if __name__ == '__main__':
	
	main()