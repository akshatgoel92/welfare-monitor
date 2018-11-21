# Imports
import os
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import *
from sqlalchemy.engine import reflection
pymysql.install_as_MySQLdb()

# Import item files
from nrega_scrape.items import FTOItem
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from common.helpers import sql_connect

# Put FTO queue in a table
def put_fto_nos(table, engine, path):

    fto_nos = pd.read_excel(path).drop_duplicates()
    fto_nos['done'] = 0
    fto_nos['fto_type'] = ''
    fto_nos.to_sql(table, con = engine, index = False, if_exists = 'replace')

if __name__ == '__main__':

    # Create block list here
	# Create file paths after that
	block_list = ['morena']
	paths = [os.path.abspath('./fto_nos/' + block + '.xlsx') for block in block_list]
	
    # Create the DB engine here
	# Then create the data-base using the schema defined above
	user, password, host, db = sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)

	# Put the FTO no. in the queue here
	for block, path in zip(block_list, paths):
		put_fto_nos(block, engine, path)

