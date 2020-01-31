from common import helpers
from sqlalchemy.types import Integer, String
from db import update
import pandas as pd
import re

def get_report_data(prefix = "reports/I", suffix = ".csv"):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	try: df_report = pd.concat([pd.read_csv(helpers.get_object_s3(obj), encoding='latin-1') for obj in objects], ignore_index = True)
	except Exception as e: print(e)
	
	return(df_report)


def check_column_names():
	
	pass


def check_column_types():
	
	pass


def fill_empty_string_rows(df_report):
	
	pass


def get_report_columns(df_report):
	
	current_cols = [columns[0:2] + [col for col in df_report.columns.tolist() if str(suffix) in col] for suffix in range(1,5)]
	new_cols = [re.sub(r'_[0-9]+', '' , current_col).lower() for current_col in current_cols[0]]
	new_cols[0:2] = ['recipient_id', 'recipient_phone']
		
	return(current_cols, new_cols)


def arrange_report_data(df_report, current_cols, new_cols):
	
	df_report = [df_report[current_cols[0]], df_report[current_cols[1]], df_report[current_cols[2]], df_report[current_cols[3]]]
	for df in df_report: df.columns = new_cols
	df_report = pd.concat(df_report)
	
	return(df_report)


# Each recipient ID should be 4 digits long
# Each recipient ID should be greater than 9000: check with Raipur team
# Recipient ID should not be missing 
# Recipient ID should be coercable to int
def check_recipient_id(df_report):
	
	pass


# Recipient phone number should not be missing
# Recipient phone number should be coercible to int
# Recipient phone number should be 10 digits long
def check_recipient_phone(df_report):
	
	pass


# Identifier should follow consistent format: Check this format
def check_identifier(df_report):
	
	pass	


# Script pushed should be according to camp data
# Script pushed should be one of the allowed values
def check_script_pushed(df_report):
	
	pass


# Call time should be coercible to date-time
def check_call_time(df_report):
	
	pass


# Call status should not be missing
# Call status
def check_call_status(df_report):
	
	pass


# Call duration should be coercable to int
# Call duration should be 
def check_call_duration(df_report):
	
	pass
	

# Call complete should be 1 iff call duration should be greater than 0
def check_call_complete(df_report):
	
	pass


# Gender classification should be string
# Gender classification should be either male, female, or NA
def check_gender_classification(df_report):
	
	pass


# Confidence score should exist where gender classification is not missing
# Confidence score should be weakly between 0 and 1
def check_confidence_score(df_report):
	
	pass


# Audio file names should follow format
# Audio file extensions should all be .wav
def check_audio_file(df_report):
	
	pass	 


def check_report_table_exists(df_report):
	
	pass

	
def get_report_table_from_db():
	
	engine = helpers.db_engine()
	df_db = pd.read_sql("SELECT identifier FROM reports;", con = engine)
	
	return(df_db)


def get_new_calls(df_s3, df_db):
	
	new_calls = db.update.anti_join(df_s3, df_db, on = ['id'])
	
	return(new_calls)


def write_new_calls(new_df_field):
	
	data_types = {'recipient_id': Integer(), 'recipient_phone': String(50), 'identifier': String(50), 
 				  'script_pushed': String(50), 'calltime': String(50), 'call_status': String(50), 
				  'call_duration': String(50),'call_complete': Integer(),'gender_classification': String(50), 
				  'confidence_score': Float(), 'audio_files': String(50)}
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: 
		
		df_report.to_sql('call_report', if_exists = 'append', con = engine, index = False,
							 chunksize = 100, dtype = data_types)
		
	except Exception as e: 
		
		print(e)
		
	return(df_report)


def main():
	
	df_report = get_report_data()

		
if __name__ == '__main__':
	
	main()

# Add in e-mail error handling
# Figure out how to check for primary key and add that in [check and if not exists ADD primary key]
# Alembic migrations