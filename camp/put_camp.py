import helpers
import db


def get_camp_data():
	'''
	Parameters:
	* 
	*
	Returns:
	*
	'''
	objects = helpers.get_matching_s3_keys(prefix = "scripts")
	df = pd.concat([helpers.download_s3(obj) for obj in objects])
	
	return(df)

def get_camp_table_from_db():
	'''
	Parameters:
	* 
	* 
	Returns:
	*
	'''
	

def put_camp_data():
	'''
	Parameters: 
	* df_field: Concatenated data from the camps
	* df_db: Concatenated data from the DB table
	
	Returns:
	* 
	'''
	
	engine = helpers.db_engine()
	
	new_df_field = db.modify.anti_join(df_field, df_db, on = ['id'])
	new_df_field.to_sql(field_data, if_exists = 'append', conn = engines, new)
	
	return(new_df_field)