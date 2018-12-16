# Imports
import sys
import os
import pandas as pd
import numpy as np
import pymysql
import argparse
from sqlalchemy import *
from sqlalchemy.engine import reflection

# Import item files
from helpers import sql_connect

# Install this as MySQLdb
pymysql.install_as_MySQLdb()

# Put FTO queue in a table
def put_fto_nos(table, engine, path, if_exists):

    fto_nos = pd.read_excel(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql(table, 
					con = engine, 
					index = False, 
					if_exists = if_exists)

if __name__ == '__main__':

	 # Create parser
	parser = argparse.ArgumentParser(description='Parse the block')
	parser.add_argument('block', 
						type=str,
						help='Block name')
	parser.add_argument('if_exists',
						 type = str, 
						 help = 'Append or replace?')

	# Parse arguments
	args = parser.parse_args()
	block = args.block
	if_exists = args.if_exists
	path = os.path.abspath('./fto_nos/' + block + '.xlsx')
	
    # Create the DB engine here
	# Then create the data-base using the schema defined above
	user, password, host, db = sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	put_fto_nos(block, engine, path, if_exists)

