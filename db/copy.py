# Import packages
from sqlalchemy.engine import reflection
from common import helpers
import sys


def copy_db(db_test = 'gma_bank_db_test',  db_prod = 'gma_bank_db'): 
	
	engine_prod = helpers.db_engine()
	engine_test = helpers.db_engine(test = 1)
	
	tables = reflection.Inspector.from_engine(engine_prod).get_table_names()
	conn = engine_test.connect()
	trans = conn.begin()
	print(tables)
	
	for table in tables:
		try: 	
		 
			db_prod_sql = db_prod + "." + table 
			sql = "CREATE TABLE IF NOT EXISTS {} LIKE {};".format(table, db_prod_sql)
			conn.execute(sql)
			print('Table {} done'.format(table))					
	
		except Exception as e:
			
			print(e)
			trans.rollback()
			sys.exit()

	trans.commit()

if __name__ == '__main__': 

	copy_db()