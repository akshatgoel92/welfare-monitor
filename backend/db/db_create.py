#---------------------------------------------------------------------# 
# Import packages
#---------------------------------------------------------------------# 
import db_schema
import sys

from common import helpers

#---------------------------------------------------------------------# 
# Send stage table names to file
#---------------------------------------------------------------------# 
def stage_names_execute():

	engine = helpers.db_engine()

	db_schema.create_stage_table_names()

	return

#---------------------------------------------------------------------# 
# Call the create data-base function
#---------------------------------------------------------------------# 
def db_execute(branch = 1):

	engine = helpers.db_engine()

	db_schema.db_create(engine, branch)

	return

#---------------------------------------------------------------------# 
# Create the stage tables
#---------------------------------------------------------------------#
def stage_tables_execute():

	stages = db_schema.load_stage_table_names()

	engine = helpers.db_engine() 

	conn = engine.connect()
	
	trans = conn.begin()
	
	for stage in stages:
			
		try: 
				
			db_schema.create_stage(conn, stage)
			
		except Exception as e:
				
			print(e)
				
			print('Error making the stage table for...:' + stage)
				
			trans.rollback()
				
			sys.exit()

	trans.commit()

	return


#---------------------------------------------------------------------# 
# Create additional primary keys wherever needed
#---------------------------------------------------------------------#
def primary_key_execute():

	engine = helpers.db_engine()

	db_schema.make_primary_key(engine, 'banks', 'ifsc_code')

	return


def main():

	stage_names_execute()

	db_execute()
	
	stage_tables_execute()




#---------------------------------------------------------------------# 
# Execute the database creation
#---------------------------------------------------------------------#
if __name__ == '__main__':

	main()


	
