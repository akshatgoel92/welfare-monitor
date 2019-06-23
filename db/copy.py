# Import packages
import common
import sys
from sqlalchemy.engine import reflection


def create_db(engine_test, engine_prod, db_test,  db_prod = 'nrega_payments'): 
	
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

	db_test = 'bh_test'
	engine_prod = common.database_engine()	
	engine_test = common.database_engine('bh_test')
	create_db(engine_test, engine_prod, db_test)