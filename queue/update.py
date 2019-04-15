#-------------------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Merge and download scraped FTO data to Dropbox every day
# Contact: akshat.goel@ifmr.ac.in
#-------------------------------------------------------------------#
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import *
from common import helpers
pymysql.install_as_MySQLdb()


# Get the FTO nos. which have at least one distinct transaction 
# associated with them from the transactions table 
# Treat this as a list of FTOs already scraped
def get_scraped_ftos(engine):

	get_scraped_ftos="SELECT DISTINCT fto_no FROM transactions;"
	scraped_ftos=pd.read_sql(get_scraped_ftos, con = engine)

	return(scraped_ftos)


# Get the target FTOs that we wanted to scrape from the FTO queue
def get_target_ftos(engine):

	get_target_ftos="SELECT * FROM fto_queue;"
	target_ftos=pd.read_sql(get_target_ftos, con = engine)

	return(target_ftos)


# Update the FTO queue with the progress of the scrape
def update_ftos(engine, scraped_ftos, target_ftos):
	
	conn = engine.connect()
	trans = conn.begin()
	
	try: 
		
		# The code block below proceeds as follows
		# Use a right join to exclude all FTOs in transactions table from previous districts/blocks
		# Update the tracker for the FTOs which are transactions
		# Drop the merge variable
		# Update the materials FTOs because there are no transactions scraped from these in the wage-list scrape
		# Write to the SQL table
		all_ftos = pd.merge(scraped_ftos, target_ftos, how = 'right', on = ['fto_no'], indicator = True)
		all_ftos.loc[(all_ftos['_merge'] == 'both'), 'done'] = 1
		all_ftos.drop(['_merge'], axis = 1, inplace = True)

		all_ftos.loc[(all_ftos['fto_type'] == 'Material'), 'done'] = 1
		all_ftos.to_sql('fto_queue', con = conn, index = False, if_exists = 'replace', chunksize=100)

		trans.commit()
		conn.close()

	except Exception as e: 
		
		print(e)
		trans.rollback()
		conn.close()

	total = len(all_ftos)
	done = len(all_ftos.loc[all_ftos['done'] == 1])

	return(total, done)


# This function calculates the progress of the code
def get_progress(total, done):
	
	try: 
		
		progress = done/total
		msg = 'There are a total of {} FTOs. The code has done {} FTOs. The code is {} done'
		print(msg.format(total, done, progress))
	
	except Exception as e:
		
		print(e)
		msg = 'There has been an error in the progress calculation...please check the calculation.'
		print(msg)

	return


# Function calls
def main():
	
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
	scraped_ftos = get_scraped_ftos(engine)
	target_ftos = get_target_ftos(engine)

	total, done = update_ftos(engine,scraped_ftos, target_ftos)
	get_progress(total, done)


if __name__ == '__main__':

	# Call function
	main()
