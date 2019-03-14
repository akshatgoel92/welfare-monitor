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
from common.helpers import sql_connect
from make_fto_tables import put_fto_nos

# Install this as MySQLdb
pymysql.install_as_MySQLdb()


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

