
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import helpers
import argparse
import pandas as pd
import numpy as np

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

pymysql.install_as_MySQLdb()

# Upload data to Dropbox
def upload_data(block, path_from = '', path_to = '', to_dropbox = 0):

	get_transactions = "SELECT * FROM transactions where block_name = '" + block.capitalize() + "';"
	get_banks = "SELECT * FROM banks;"
	get_accounts = "SELECT * from accounts"

	file_from = path_from + block + '.csv'

	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()

	try: 
		transactions = pd.read_sql(get_transactions, con = conn)
		banks = pd.read_sql(get_banks, con = conn)
		accounts = pd.read_sql(get_accounts, con = conn)
		conn.close()

	except Exception as e:
		print(e)
		conn.close()
		
	try: 
		transactions = pd.merge(transactions, banks, 
								how = 'outer', 
								on = ['ifsc_code'], 
								indicator = 'banks_merge')
		
		transactions = pd.merge(transactions, accounts, 
								how = 'outer', 
								on = ['jcn', 'acc_no', 'ifsc_code'], 
								indicator = 'accounts_merge')

	except Exception as e: 
		print(e)
		print('Merge failed...please check the merge.')

	try: 
		transactions.to_csv(file_from, index = False)
	
	except Exception as e:
		print(e) 
		print('Sending data to .csv failed...please check the .csv upload.')
	
	if to_dropbox == 1:

		try:
			helpers.dropbox_upload(file_from, file_to)
	
		except Exception as e:
			print('Dropbox upload failed...please check the Dropbox upload.')

if __name__ == '__main__':

    # Create parser
    parser = argparse.ArgumentParser(description='Parse the block')
    parser.add_argument('block', type=str, help='Block name')
    parser.add_argument('to_dropbox', type=int, help='Whether to write to Dropbox')
    
    # Parse arguments
    args = parser.parse_args()
    block = args.block
    to_dropbox = args.to_dropbox
    
    # Set file paths
    file_from = './nrega_output/' + block + '_data.csv'
    file_to = ''
    
    # Call function
    upload_data(block, file_from, file_to, to_dropbox)