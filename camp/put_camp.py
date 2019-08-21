from sqlalchemy.types import Integer, String
from sqlalchemy.dialects.mysql import TINYINT
from common import errors as er
from common import helpers
from db import update

import pandas as pd
import datetime
import re


def get_camp_data_list(prefix = "camps/camp", suffix = ".csv"):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	
	return(objects)
	

def get_camp_data_file(camp_data_file):
	
	df = pd.read_csv(helpers.get_object_s3(camp_data_file))
	
	return(df)


def add_s3_file_name(df, camp_data_file):
	
	df['file_name_s3'] = camp_data_file
	
	return(df)


def add_s3_date(df, camp_data_file): 
	
	date = re.findall('camp_(.*).csv', camp_data_file)[0]
	date = datetime.datetime.strptime(date, '%d%m%Y')
	date = datetime.datetime.strftime(date, '%Y-%m-%d') 
	
	df['file_upload_to_s3_date'] = date
	
	return(df)
 
 
def get_camp_data(camp_data_list):
	 
	 df = [get_camp_data_file(camp_data_file) for camp_data_file in camp_data_list]
	 
	 return(df)


def add_camp_data_columns(df, camp_data_list):
	
	df = [add_s3_file_name(data, file_name) for data, file_name in zip(df, camp_data_list)]
	df = [add_s3_date(data, file_name) for data, file_name in zip(df, camp_data_list)]
	df = pd.concat(df, ignore_index = True)
	
	df = df[['id', 'phone', 'jcn', 'time_pref', 'time_pref_label', 'file_name_s3', 'file_upload_to_s3_date']]
	df['insert_date'] = str(datetime.datetime.today())
	df['enrolment_date'] = ''
	df['pilot'] = 0
	
	return(df)


def check_if_camp_table_exists():
	
	db_name = helpers.sql_connect()['db']
	engine = helpers.db_engine()
	table_exists = update.check_table_exists(engine, db_name, 'enrolment_record')
	
	return(table_exists)
	

def get_camp_table_from_db():
	
	engine = helpers.db_engine()
	
	df_db = pd.read_sql("SELECT id FROM enrolment_record;", con = engine)
	
	return(df_db)


def get_new_trainees(df, df_db):
	
	new_df = update.anti_join(df, df_db, on = ['id'])
	
	return(new_df)


def put_new_trainees(new_df):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	if not new_df.empty: 
	
		try: 
		
			new_df.to_sql('enrolment_record', if_exists = 'append', con = engine, index = False, chunksize = 100, 
						  dtype = {'id': Integer(), 'phone': String(50), 'jcn': String(50), 'time_pref': String(50), 
						  		   'time_pref_label': String(50), 'file_name_s3': String(50), 
								   'file_upload_to_s3_date': String(50), 
								   'insert_date': String(50), 
								   'enrolment_date': String(50), 
								   'pilot': TINYINT(2)})
		
		except Exception as e: 
		
			er.handle_error(error_code ='23', data = {})
			sys.exit()
		
	return


def make_camp_primary_key():
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: has_primary_key = update.check_primary_key(engine, 'enrolment_record')
	
	except Exception as e: 
		
		er.handle_error(error_code ='24', data = {})
		sys.exit()
	
	try: 
		if has_primary_key == 0: update.create_primary_key(engine, "enrolment_record", "id")
	
	except Exception as e: 
		
		er.handle_error(error_code ='25', data = {})
		sys.exit()
	
	return

	
def main():
	
	camp_data_list = get_camp_data_list()
	df = get_camp_data(camp_data_list)
	df = add_camp_data_columns(df, camp_data_list)
	
	if check_if_camp_table_exists() == 1:
		df_db = get_camp_table_from_db()
		df = get_new_trainees(df, df_db)
	
	put_new_trainees(df)
	make_camp_primary_key()
	

if __name__ == '__main__':
	main()