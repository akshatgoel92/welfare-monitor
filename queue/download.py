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

from datetime import datetime
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *
from common import helpers
from common import errors as er

pymysql.install_as_MySQLdb()


# Get the scraped transactions data
def get_transactions():
	
	engine = helpers.db_engine()
	conn = engine.connect()
	
	get_transactions = "SELECT * FROM transactions;"
	get_banks = "SELECT * FROM banks;"
	get_accounts = "SELECT * from accounts;"

	try: 
		
		transactions = pd.read_sql(get_transactions, con = conn)
		banks = pd.read_sql(get_banks, con = conn)
		accounts = pd.read_sql(get_accounts, con = conn)
		conn.close()

	except Exception as e:
		
		er.handle_error(error_code ='5', data = {})
		conn.close()

	return(transactions, banks, accounts)


# Merge transactions, bank codes, and bank account data-sets
def merge_transactions(transactions, banks, accounts, file_from):
		
	try: 
		
		transactions = pd.merge(transactions, banks, how = 'left', on = ['ifsc_code'], indicator = 'banks_merge')
		transactions = pd.merge(transactions, accounts, how = 'left', on = ['jcn', 'acc_no', 'ifsc_code'], 
								indicator = 'accounts_merge')

	except Exception as e: er.handle_error(error_code ='6', data = {})
		
	try: transactions.to_csv(file_from, index = False)
	
	except Exception as e: er.handle_error(error_code ='7', data = {})


# Download data to .csv
def download_transactions(transactions, to_dropbox, to_s3, file_to, file_from):
	
	engine = helpers.db_engine()
	conn = engine.connect()

	if to_dropbox == 1: 

		try: helpers.upload_dropbox(file_from, file_to)
		except Exception as e: er.handle_error(error_code ='8', data = {})

	if to_s3 == 1: 

		try: helpers.upload_s3(file_from, file_to)
		except Exception as e: er.handle_error(error_code ='9', data = {})


# Function calls go here
def main():
	
	# Create parser
	parser = argparse.ArgumentParser(description = 'Parse the data for download')
	parser.add_argument('to_dropbox', type = int, help = 'Whether to write to Dropbox')
	parser.add_argument('to_s3', type = int, help ='Whether to write to S3')
	parser.add_argument('file_from', type = str, help = 'Append or replace?')
	parser.add_argument('file_to', type = str, help = 'Append or replace?')
	
    # Parse arguments
	args = parser.parse_args()
	to_dropbox = args.to_dropbox
	to_s3 = args.to_s3
	file_from = args.file_from
	file_to = args.file_to + '_' + str(datetime.today()) + '.csv'
	
    # Execute function calls
	transactions, banks, accounts = get_transactions()
	merge_transactions(transactions, banks, accounts, file_from)
	download_transactions(transactions, to_dropbox, to_s3, file_to, file_from)


if __name__ == '__main__':

	main()