# Import packages
from sqlalchemy import *
from sqlalchemy.engine import reflection
from common.helpers import sql_connect
from make import put_fto_nos
import sys
import os
import pandas as pd
import numpy as np
import pymysql
import argparse


# Install this as MySQLdb
pymysql.install_as_MySQLdb()


if __name__ == '__main__':

	 # Create parser
	parser = argparse.ArgumentParser(description='Append to or replace the queue?')
	parser.add_argument('if_exists',
						 type = str, 
						 help = 'Append or replace?')

	# Parse arguments
	args = parser.parse_args()
	if_exists = args.if_exists
	print(if_exists)
	print(type(if_exists))
	path = os.path.abspath('./output/fto_queue.csv')
	
    # Create the DB engine here
	# Then create the data-base using the schema defined above
	user, password, host, db = sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	put_fto_nos(engine, path, if_exists)

