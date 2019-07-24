import helpers
import db


def get_camp_data(prefix = "scripts"):
	
	objects = helpers.get_matching_s3_keys(prefix = prefix)
	df_field = pd.concat([helpers.download_s3(obj) for obj in objects])
	
	return(df_field)


def get_camp_table_from_db():
	
	engine = helpers.db_engine()
	df_db = pd.read_sql("SELECT * FROM field_data;", conn = engine)
	
	return(df_db)


def put_camp_data(df_field, df_db):

	engine = helpers.db_engine()
	conn = engine.connect()
	
	
	new_df_field = db.modify.anti_join(df_field, df_db, on = ['id'])
	new_df_field['welcome'] = 0
	
	try: new_df_field.to_sql(field_data, if_exists = 'append', conn = engine, chunksize = 100)
	except Exception as e: print(e)
		
	return(new_df_field)


def main():
	
	df_field = get_camp_data()
	df_db = get_camp_table_from_db()
	put_camp_data(df_field, df_db)
		
if __name__ == '__main__':
	
	main()