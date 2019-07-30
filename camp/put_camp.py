from common import errors as er
from common import helpers
from sqlalchemy.types import Integer, String
from db import update
import pandas as pd


def get_camp_data(prefix = "pilots/btt_merged", suffix = ".csv"):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	df_field = pd.concat([pd.read_csv(helpers.get_object_s3(obj)) for obj in objects], ignore_index = True)
	
	
	return(df_field)


def check_camp_data_columns(df_field):
	
	df_field.columns = ['id', 'sky_phone', 'time_pref', 'jcn_extra', 'jcn']
	df_field = df_field[['id', 'sky_phone', 'jcn', 'jcn_extra', 'time_pref']]
	
	return(df_field)
	

def get_camp_table_from_db():
	
	engine = helpers.db_engine()
	df_db = pd.read_sql("SELECT id FROM field_data;", con = engine)
	
	return(df_db)


def get_new_trainees(df_field, df_db):
	
	new_df_field = update.anti_join(df_field, df_db, on = ['id'])
	
	return(new_df_field)


def put_new_trainees(new_df_field):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: 
		
		new_df_field.to_sql('field_data', if_exists = 'append', con = engine, index = False,
							 chunksize = 100, dtype = {'id': Integer(), 'sky_phone': String(50), 
							 'jcn': String(50), 'jcn_extra': String(50), 'time_pref': String(50)})
		
	except Exception as e: 
		
		er.handle_error(error_code ='23', data = {})
		sys.exit()
		
	return(new_df_field)


def add_primary_key():
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: has_primary_key = update.check_primary_key(engine, 'field_data', )
	
	except Exception as e: 
		
		er.handle_error(error_code ='24', data = {})
		sys.exit()
	
	try: 
		
		if has_primary_key = 0: update.create_primary_key(engine, "field_data", "id")
	
	except Exception as e: 
		
		er.handle_error(error_code ='25', data = {})
		sys.exit()
	
	return
	
	
def main():
	
	df_field = get_camp_data()
	df_field = check_camp_data_columns(df_field)
	df_db = get_camp_table_from_db()
	
	new_df_field = get_new_trainees(df_field, df_db)
	put_new_trainees(new_df_field)

		
if __name__ == '__main__':
	
	main()


# Alembic migrations