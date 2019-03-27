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

from common import helpers
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

pymysql.install_as_MySQLdb()


# Upload data to Dropbox
def download_data(block, path_from = '', path_to = ''):

	get_transactions = "SELECT * FROM transactions where fto_no in (SELECT fto_no from " + block + ");"
	get_banks = "SELECT * FROM banks;"
	get_accounts = "SELECT * from accounts;"
	file_from = path_from + block + '.csv'
	
	user, password, host, db = helpers.sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	conn = engine.connect()

	try: 
		transactions = pd.read_sql(get_transactions, con = conn)
		banks = pd.read_sql(get_banks, con = conn)
		accounts = pd.read_sql(get_accounts, con = conn)
		print('Got transactions')
		conn.close()

	except Exception as e:
		print(e)
		conn.close()
		
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
    file_from = './nrega_output/' + block
    file_to = ''
    
    # Call function
    download_data(block, file_from, file_to, to_dropbox)

	try:
		helpers.dropbox_upload(file_from, file_to)
	
	except Exception as e:
		print(e)
		print('Dropbox upload failed...please check the Dropbox upload.')