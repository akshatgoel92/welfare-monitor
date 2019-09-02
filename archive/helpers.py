import os
import json
import sys
import dropbox
import pymysql
import smtplib
import boto3
import pandas as pd
import numpy as np
import datetime

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

pymysql.install_as_MySQLdb()


def sql_connect():

	with open('../gma_secrets.json') as secrets: 
		sql_access = json.load(secrets)['mysql']
	
	return(sql_access)


def db_conn():
	
	with open('./gma_secrets.json') as secrets:
		sql_access = json.load(secrets)['mysql']
	
	user = sql_access['username']
	password = sql_access['password']
	host = sql_access['host']
	db = sql_access['db']
		
	conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
	cursor = conn.cursor()
	
	return(conn, cursor)


def db_engine():
	
	user, password, host, db = sql_connect().values()
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)

	return(engine)

	
def upload_dropbox(file_from = './output/gma_test.xlsx', file_to = 'tests/gma_test.xlsx'):
	
	with open('./gma_secrets.json') as data_file:
		credentials = json.load(data_file)

	access_token = credentials['dropbox']['access_token']
	file_to_prefix = credentials['dropbox']['prefix']
	dbx = dropbox.Dropbox(access_token)

	# Prepend the project Dropbox path to the folder
	file_to=os.path.join(file_to_prefix, file_to)

	with open(file_from, 'rb') as f:
		dbx.files_upload(f.read(), file_to, mode = dropbox.files.WriteMode.overwrite)


def upload_s3(file_from, file_to):
	
	with open('./gma_secrets.json') as secrets:
		s3_access = json.load(secrets)['s3']

	access_key_id = s3_access['access_key_id']
	secret_access_key = s3_access['secret_access_key']
	bucket_name = s3_access['default_bucket']
		
	s3 = boto3.client('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)
	s3.upload_file(file_from, bucket_name, file_to)


def download_file_s3(file_from = 'tests/gma_test.xlsx', file_to = './output/gma_test.xlsx', 
					 bucket_name = 'gma-ivrs'):

	with open('./gma_secrets.json') as secrets:
		s3_access = json.load(secrets)['s3']

	access_key_id = s3_access['access_key_id']
	secret_access_key = s3_access['secret_access_key']
	bucket_name = s3_access['default_bucket']
	
	s3 = boto3.resource('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)

	try: s3.Bucket(bucket_name).download_file(file_from, file_to)

	except Exception as e:

		print(e)
	

def delete_files(path = './output/', extension = '.csv'):
	
	for filename in os.listdir(path):
		if filename.endswith(extension): 
			os.unlink(path + filename)


def send_email(subject, msg):

	with open('../recipients.json') as r:
		recipients = json.load(r)

	with open('../gma_secrets.json') as secrets:
		credentials = json.load(secrets)['smtp']
	 
	user = credentials['user']
	password = credentials['password']
	region = credentials['region']
	
	smtp_server = 'email-smtp.' + region + '.amazonaws.com'
	smtp_port = 587
	sender = 'akshat.goel@ifmr.ac.in'
	text_subtype = 'html'
	
	msg = MIMEText(msg, text_subtype)
	msg['Subject']= subject
	msg['From'] = sender
	msg['To'] = ', '.join(recipients)
	
	conn = SMTP(smtp_server, smtp_port)
	conn.set_debuglevel(1)
	conn.ehlo()
	conn.starttls()
	conn.ehlo()
	conn.login(user, password)
	conn.sendmail(sender, recipients, msg.as_string())
	conn.close()


# Clean each item that goes through the pipeline given an 
# item and a list of fields in that item which are supposed
# to be in title case
def clean_item(item, title_fields):
	
	for field in item.keys():
		
		if type(item[field]) == str:
			item[field] = item[field].strip()

		if field in title_fields:
			item[field] = item[field].title()
	
	return(item)


# Input: * End date in format 2018-12-31 (Year-Month-Day), 
# 		 * Length in days of time window
def get_time_window(end_date, window_length):

	time_window = datetime.timedelta(days = window_length)
	start_date = str(datetime.datetime.strptime(end_date, '%Y-%m-%d').date() - time_window)

	return(start_date)
