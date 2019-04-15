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

from common import helpers
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

pymysql.install_as_MySQLdb()


# Get the scraped transactions data
def get_transactions():
	

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()
	
	get_transactions = "SELECT * FROM transactions where fto_no in (SELECT fto_no from fto_queue);"
	get_banks = "SELECT * FROM banks;"
	get_accounts = "SELECT * from accounts;"

	try: 
		transactions = pd.read_sql(get_transactions, con = conn)
		banks = pd.read_sql(get_banks, con = conn)
		accounts = pd.read_sql(get_accounts, con = conn)

		print(type(transactions))
		print(type(banks))
		print(type(accounts))
		print('Got transactions')

		conn.close()

	except Exception as e:
		print(e)
		conn.close()

	return(transactions, banks, accounts)


# Merge transactions, bank codes, and bank account data-sets
def merge_transactions(transactions, banks, accounts, file_from='./output/transactions.csv'):
	

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()
	
	try: 
		transactions = pd.merge(transactions, banks, 
								how = 'left', 
								on = ['ifsc_code'], 
								indicator = 'banks_merge')
		print('Banks merge done..')
		
		transactions = pd.merge(transactions, accounts, 
								how = 'left', 
								on = ['jcn', 'acc_no', 'ifsc_code'], 
								indicator = 'accounts_merge')
		print('Accounts merge done..')

	except Exception as e: 
		print(e)
		print('Merge failed...please check the merge.')

	try: 
		transactions.to_csv(file_from, index = False)
		print('Transactions sent to .csv...')
	
	except Exception as e:
		print(e) 
		print('Sending data to .csv failed...please check the .csv upload.')

# Download data to .csv
def download_transactions(transactions, to_dropbox, to_s3, file_to, file_from='./output/transactions.csv'):
	

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()

	if to_dropbox==1:
		
		file_to=os.path.join('./Female Mobile Phones Phase I/Phase II/CHiPS/Data/mis_scrapes/', file_to)
		helpers.upload_dropbox(file_from, file_to)

	if to_s3==1:
		helpers.upload_s3(file_from, file_to)


# Function calls go here
def main():
	

	# Create parser
	parser = argparse.ArgumentParser(description='Parse the data for download')
	parser.add_argument('to_dropbox', type=int, help='Whether to write to Dropbox')
	parser.add_argument('to_s3', type=int, help='Whether to write to S3')
	parser.add_argument('file_to', type = str, help = 'Append or replace?')

    # Parse arguments
	args=parser.parse_args()
	to_dropbox=args.to_dropbox
	to_s3=args.to_s3
	file_to=args.file_to

    # Execute function calls
	transactions, banks, accounts=get_transactions()
	merge_transactions(transactions, banks, accounts)
	download_transactions(transactions, to_dropbox, to_s3, file_to)


if __name__ == '__main__':

	main()