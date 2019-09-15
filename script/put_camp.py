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
	
	df = df[['id', 'phone', 'jcn', 'jc_status', 'time_pref', 'time_pref_label', 'file_name_s3', 'file_upload_to_s3_date', 
		  'breastfeeding', 'pregnant', 'children_under6', 'teenage_girls', 'nocategory']]
	df['insert_date'] = str(datetime.datetime.today())
	df['enrolment_date'] = ''
	df['pilot'] = 0
	
	return(df)


def remove_epod_staff(df):
	
	df = df.loc[df['id'] >= 1000]
	
	return(df)


def check_if_camp_table_exists():
	
	db_name = helpers.sql_connect()['db']
	engine = helpers.db_engine()
	table_exists = update.check_table_exists(engine, db_name, 'enrolment_record')
	
	return(table_exists)


def check_data_from_s3(file_name_s3_source, file_name_s3_target):
	
	df_source = pd.read_csv(helpers.get_object_s3(file_name_s3_source))
	df_target = pd.read_csv(helpers.get_object_s3(file_name_s3_target))
	
	variables_list = ['phone', 'jc_status', 'time_pref', 
				 'time_pref_label', 'breastfeeding', 
				 'pregnant', 'children_under6', 'teenage_girls', 'nocategory']
	
	df_check = pd.merge(df_source, df_target, how = 'inner', on = ['id'], indicator = True)
	
	for var in variables_list: df_check[var + '_check_failed'] = (df_check[var + '_x'] != df_check[var + '_y']).astype(int)
	
	check_columns = [var + '_check_failed' for var in variables_list]
	df_check['check'] = df_check[check_columns].sum(axis = 1)
	df_check = df_check.loc[df_check['check'] != 0]
	df_check['file_name_source'] = file_name_s3_source
	df_check['file_name_target'] = file_name_s3_target
	
	return(df_check)

def run_data_check_from_s3(camp_data_list):
	
	df_result = []
	
	for source in camp_data_list: 
		for target in camp_data_list:
			try: 
				df_result.append(check_data_from_s3(source, target))
			
			except Exception as e:
				subject = 'GMA Error: There are inconsistent columns in the camp data...!'
				message = 'We are comparing {} with {}'.format(source, target)
				helpers.send_email(subject, message)
				sys.exit() 
	
	df_result = pd.concat(df_result)
	
	if len(df_result) >= 0:
		
		today = str(datetime.today().date())
		file_name_today = datetime.strptime(today, '%Y-%m-%d').strftime('%d%m%Y')
		
		df_result.to_csv('./output/camp_checks.csv', index = False)
		helpers.upload_s3('./output/camp_checks.csv', '/tests/camp_checks_{}.csv'.format(today))
		
		subject = 'GMA Error: There are inconsistent rows in the camp data...!'
		message = 'We are comparing {} with {}. Check /tests/camp_checks_{}.csv'.format(source, target, today)
		helpers.send_email(subject, message)
		
	return(df_result)


def check_data_from_db(file_name_s3_source, file_name_s3_target, var):
	
	engine = helpers.db_engine()
	
	df = pd.read_csv(helpers.get_object_s3(file_name_s3_source))
	df_db = pd.read_sql("SELECT id, {} FROM enrolment_record where file_name_s3 = '{}'".format(var, file_name_s3_target), con = engine)
	
	df_check = pd.merge(df, df_db, how = 'inner', on = ['id'], indicator = True)
	
	return(df_check, df_db, df)


def get_camp_table_from_db():
	
	engine = helpers.db_engine()
	
	df_db = pd.read_sql("SELECT id FROM enrolment_record;", con = engine)
	
	return(df_db)


def get_new_trainees(df, df_db):
	
	new_df = update.anti_join(df, df_db, on = ['id'])
	
	return(new_df)
	

def get_health_category(df): 
	         
	df['health_category'] = 0
	
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 0) & (df['children_under6'] == 1) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 1
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 0) & (df['children_under6'] == 0) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 2
	
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 0) & (df['children_under6'] == 0) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 3
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 1) & (df['children_under6'] == 0) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 4
	
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 0) & (df['children_under6'] == 0) & (df['teenage_girls'] == 0) & (df['nocategory'] == 1), 'health_category'] = 5
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 0) & (df['children_under6'] == 1) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 6
	
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 0) & (df['children_under6'] == 1) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 7
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 1) & (df['children_under6'] == 1) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 8
	
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 0) & (df['children_under6'] == 0) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 9
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 1) & (df['children_under6'] == 0) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 10
	 
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 1) & (df['children_under6'] == 0) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 11
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 1) & (df['children_under6'] == 1) & (df['teenage_girls'] == 0) & (df['nocategory'] == 0), 'health_category'] = 12

	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 0) & (df['children_under6'] == 1) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 13
	df.loc[(df['breastfeeding'] == 0) & (df['pregnant'] == 1) & (df['children_under6'] == 1) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 14
	
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 1) & (df['children_under6'] == 0) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 15
	df.loc[(df['breastfeeding'] == 1) & (df['pregnant'] == 1) & (df['children_under6'] == 1) & (df['teenage_girls'] == 1) & (df['nocategory'] == 0), 'health_category'] = 16
	
	return(df)


def put_new_trainees(new_df):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	if not new_df.empty: 
	
		try: 
		
			new_df.to_sql('enrolment_record', if_exists = 'append', con = engine, index = False, chunksize = 100, 
						  dtype = {'id': Integer(), 
						  		'phone': String(50), 
								'jcn': String(50),
								'jc_status': Integer(), 
								'time_pref': String(50),
								'time_pref_label': String(50),
								'file_name_s3': String(50),
								'file_upload_to_s3_date': String(50),
								'breastfeeding': String(50),
								'pregnant': String(50),
								'children_under6': String(50), 
								'teenage_girls': String(50), 
								'nocategory': String(50),
								'health_category': String(50), 
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
	
	# Create parser for command line arguments
	parser = argparse.ArgumentParser(description = 'Parse the data for script generation')
	parser.add_argument('--prefix', type = str, help ='Prefix for file names to be searched on S3', default = 'camps/camp')
	parser.add_argument('--suffix', type = str, help ='Suffix for file names to be searched on S3', default = '.csv')
	args = parser.parse_args()
	
	# Parse arguments
	prefix = args.prefix
	suffix = args.suffix	
	
	camp_data_list = get_camp_data_list(prefix = prefix, suffix = suffix)
	df = get_camp_data(camp_data_list)
	df = add_camp_data_columns(df, camp_data_list)
	df = remove_epod_staff(df)
	
	if check_if_camp_table_exists() == 1:
		df_db = get_camp_table_from_db()
		df = get_new_trainees(df, df_db)
		df = get_health_category(df)
	
	put_new_trainees(df)
	make_camp_primary_key()
	
	subject = 'GMA Update: The camp data has been refreshed...'
	message = ''
	helpers.send_email(subject, message)

if __name__ == '__main__':
	main()