# Import packages
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
from common import helpers
pymysql.install_as_MySQLdb()


def get_scraped_ftos(engine):
	'''Get the FTO nos. which have at least one distinct transaction
	associated with them from the transactions table. Treat this as
	a list of FTOs already scraped.'''

	get_scraped_ftos="SELECT DISTINCT fto_no FROM transactions;"
	scraped_ftos=pd.read_sql(get_scraped_ftos, con = conn)

	return(scraped_ftos)


def get_target_ftos(engine):
	'''Get the target FTOs that we wanted to scrape 
	from the FTO queue.'''

	get_target_ftos="SELECT * FROM fto_queue;"
	target_ftos=pd.read_sql(get_target_ftos, con = conn)

	return(target_ftos)


def update_fto_nos(engine):
	'''Update the FTO queue with the progress of the scrape.'''

	
	# Create the SQLalchemy engine
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	# Wrap this in a transaction so we can roll-back if needed
	conn = engine.connect()
	trans = conn.begin()
	
	try: 
		
		# Use a right join to exclude all FTOs in transactions table from previous districts/blocks
		all_ftos = pd.merge(scraped_ftos, target_ftos, how = 'right', on = ['fto_no'], indicator = True)
		
		# Update the tracker for the FTOs which are transactions
		all_ftos.loc[(all_ftos['_merge'] == 'both'), 'done'] = 1

		# Drop the merge variable
		all_ftos.drop(['_merge'], axis = 1, inplace = True)

		# Update the materials FTOs because there are no transactions scraped from these
		all_ftos.loc[(all_ftos['fto_type'] == 'Material'), 'done'] = 1

		# Write to the SQL table
		all_ftos.to_sql(block, con = conn, index = False, if_exists = 'replace')

		# Commit the changes and close the connection
		trans.commit()
		conn.close()

	except Exception as e: 
		
		# Roll back the transaction and make no change if there's a problem
		print(e)
		trans.rollback()
		conn.close()

	return


def get_progress(engine):
	'''This function calculates the progress of the code.'''
	
	try: 
		
		total=len(all_ftos)
		done=len(all_ftos.loc[all_ftos['done'] == 1])
		progress = done/total
		
		print('''There are a total of {} FTOs. The code has done {} FTOs. The code is {} done'''.format(total, done, progress))
	
	except Exception as e:
		
		print(e)
		print('There has been an error in the progress calculation...please check the calculation.')


def main():
	'''Put the function calls here.'''



		
if __name__ == '__main__':

	# Call function
	main()
