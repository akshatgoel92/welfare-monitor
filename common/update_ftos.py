#-------------------------#
# Import packages
#-------------------------#
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import argparse
import helpers
import pandas as pd
import numpy as np
from sqlalchemy import *

pymysql.install_as_MySQLdb()

# Update the FTO nos. table
def update_fto_nos(block):

	# Create the SQL queries
	get_scraped_ftos = "SELECT DISTINCT fto_no FROM transactions;"
	get_target_ftos = "SELECT * FROM " + block + ";"

	# Create the SQLalchemy engine
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	# Begin the transaction
	conn = engine.connect()
	trans = conn.begin()
	
	try: 
		
		# Get the 1) scraped data from transactions and 2) get target FTOs
		scraped_ftos = pd.read_sql(get_scraped_ftos, con = conn)
		target_ftos = pd.read_sql(get_target_ftos, con = conn)
		
		# Use a right join to exclude all FTOs in transactions table from previous districts/blocks
		all_ftos = pd.merge(scraped_ftos, 
							target_ftos, 
							how = 'right', 
							on = ['fto_no'], 
							indicator = True)
		
		# Update the tracker for the FTOs which are transactions
		all_ftos.loc[(all_ftos['_merge'] == 'both'), 'done'] = 1

		# Update the materials FTOs because there are no transactions scraped from these
		all_ftos.loc[(all_ftos['fto_type'] == 'Material'), 'done'] = 1

		# Drop the merge variable
		all_ftos.drop(['_merge'], 
						axis = 1, 
						inplace = True)

		# Write to the SQL table
		all_ftos.to_sql(block, 
						con = conn, 
						index = False, 
						if_exists = 'replace')

		# Commit the changes and close the connection
		trans.commit()
		conn.close()

	except Exception as e: 
		print(e)
		trans.rollback()
		conn.close()
	
	try: 
		done = len(all_ftos.loc[all_ftos['done'] == 1])
		progress = str(done/len(all_ftos))
		print(block.title() + ' is ' + progress + " done.")
		print('The code has done ' + str(done) + ' / ' + str(len(all_ftos)) + ' FTOs!')
	
	except Exception as e:
		print(e)
		print('There has been an error in the progress calculation...please check the calculation.')
		
if __name__ == '__main__':

	# Create parser
	parser = argparse.ArgumentParser(description='Parse the block')
	parser.add_argument('block', type=str, help='Block name')
	
	# Parse arguments
	args = parser.parse_args()
	block = args.block

	# Call function
	update_fto_nos(block)
