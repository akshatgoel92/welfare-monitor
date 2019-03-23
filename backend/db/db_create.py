import db_schema
from common import helpers

#---------------------------------------------------------------------# 
# Call the create data-base function
#---------------------------------------------------------------------# 
def db_execute(branch = 1):

	engine = helpers.db_engine()

	db_schema.db_create(engine, branch)

#---------------------------------------------------------------------# 
# Create the stage tables
#---------------------------------------------------------------------#
def stage_tables_execute(stages):

	stages = ['fst_sig', 'fst_sig_not', 'sec_sig', 'sec_sig_not', 'sb', 'pp', 'pb', 'P'] 

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


#---------------------------------------------------------------------# 
# Create additional primary keys wherever needed
#---------------------------------------------------------------------#
def primary_key_execute():

	engine = helpers.db_engine()

	db_schema.make_primary_key(engine, 'banks', 'ifsc_code')


def main():

	db_execute()
	
	stage_tables_execute()


#---------------------------------------------------------------------# 
# Execute the database creation
#---------------------------------------------------------------------#
if __name__ == '__main__':

	db_execute()

	stage_tables_execute()

	
