from common import helpers
import argparse


def script_to_s3(file_from, file_to):
	
	script = pd.read_csv(file_from)
	
	try: helpers.s3_upload(file_from, file_to)
	except Exception as e: print(e)
	
	return


def script_from_s3(prefix, suffix):
	
	objects = [obj for obj in helpers.get_matching_s3_keys(prefix = prefix, suffix = suffix)]
	scripts = pd.concat([pd.read_csv(helpers.get_object_s3(obj)) for obj in objects], ignore_index = True)
	
	return(scripts)


def get_script_date(script_name):
	
	pass


def script_to_db(scripts):
	
	engine = helpers.db_engine()
	conn = engine.connect()
	trans = conn.begin()
		
	try: 
		scripts.to_sql('script', if_exists='replace', con=conn)
		trans.commit()
		conn.close()

	except Exception as e:
		trans.rollback() 
		conn.close()
		print(e)
	
	return


def make_script_primary_key():
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	try: has_primary_key = update.check_primary_key(engine, 'field_data')
	
	except Exception as e: 
		
		er.handle_error(error_code ='24', data = {})
		sys.exit()
	
	try: 
		
		if has_primary_key == 0: update.create_primary_key(engine, "scripts", "id")
	
	except Exception as e: 
		
		er.handle_error(error_code ='25', data = {})
		sys.exit()
	
	return
	

def main():
	
	file_from = argparse.args(file_from)
	file_to = argparse.args(file_to)
	
	script_to_s3(file_from, file_to)
	script_to_db(file_from)
	
	return
	
	
if __name__ == '__main__':
	
	main()