from common import helpers
from sqlalchemy.types import Integer, String
from db import update
import pandas as pd


def get_report_data(prefix = "reports/I", suffix = ".xls"):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	df_report = pd.concat([pd.read_csv(helpers.get_object_s3(obj)) for obj in objects], ignore_index = True)
	
	return(df_report)


def clean_report_data(df_report):
	
	df_report.columns = ['id', 'sky_phone', 'time_pref', 'jcn_extra', 'jcn']
	df_report = df_report[['id', 'sky_phone', 'jcn', 'jcn_extra', 'time_pref']]
	
	return(df_report)


def check_report_data(df_report):
	
	pass
	
	
def get_report_table_from_db():
	
	engine = helpers.db_engine()
	df_db = pd.read_sql("SELECT id FROM reports;", con = engine)
	
	return(df_db)


def get_new_calls(df_s3_report, df_db_report):
	
	new_calls = db.update.anti_join(df_s3_report, df_db_report, on = ['id'])
	
	return(new_calls)


def get_new_calls(new_df_field):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: 
		new_df_field.to_sql('field_data', if_exists = 'append', con = engine, index = False,
							 chunksize = 100, dtype = {'id': Integer(), 'sky_phone': String(50), 
							 'jcn': String(50), 'jcn_extra': String(50), 'time_pref': String(50)})
		
	except Exception as e: 
		
		print(e)
		
	return(new_df_field)


def main():
	
	df_field = get_camp_data()
	df_field = check_camp_data_columns(df_field)
	df_db = get_camp_table_from_db()
	
	new_df_field = get_new_camp_trainees(df_field, df_db)
	put_new_trainees(new_df_field)

		
if __name__ == '__main__':
	
	main()

# Add in e-mail error handling
# Figure out how to check for primary key and add that in [check and if not exists ADD primary key]
# Alembic migrations