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
from common import errors as er

pymysql.install_as_MySQLdb()


# Get the FTO nos. which have at least one distinct transaction 
# associated with them from the transactions table 
# Treat this as a list of FTOs already scraped
def get_scraped_ftos(engine):

	get_scraped_ftos = "SELECT DISTINCT fto_no FROM transactions;"
	scraped_ftos = pd.read_sql(get_scraped_ftos, con = engine)

	return(scraped_ftos)


# Get the target FTOs that we wanted to scrape from the FTO queue
def get_target_ftos(engine):

	get_target_ftos = "SELECT * FROM fto_queue;"
	target_ftos = pd.read_sql(get_target_ftos, con = engine)

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
		all_ftos.to_sql('fto_queue', con = conn, index = False, if_exists = 'replace', chunksize=100,
						dtype = {'fto_no': String(100), 'fto_type': String(15), 'done': SmallInteger(), 
								 'stage': String(15)})
		conn.execute("ALTER TABLE fto_queue ADD PRIMARY KEY (fto_no(100));")
		trans.commit()
		conn.close()

	except Exception as e: 
		
		er.handle_error(error_code ='10', data = {})
		trans.rollback()
		conn.close()

	total = len(all_ftos)
	done = len(all_ftos.loc[all_ftos['done'] == 1])
	progress = done/total

	return(total, done, progress)


# This function calculates the progress of the code
def send_progress(total, done, progress):
	
	try: 
		
		msg = 'There are a total of {} FTOs. The code has done {} FTOs. The code is {} done.'
		subject = 'GMA FTO Scrape: Progress Report'
		helpers.send_email(subject, msg.format(total, done, progress))
	
	except Exception as e: er.handle_error(error_code ='11', data = {})

	return


# Function calls
def main():
	
	engine = helpers.db_engine()
	scraped_ftos = get_scraped_ftos(engine)
	target_ftos = get_target_ftos(engine)
	total, done, progress = update_ftos(engine,scraped_ftos, target_ftos)
	send_progress(total, done, progress)


if __name__ == '__main__':

	# Call function
	main()
