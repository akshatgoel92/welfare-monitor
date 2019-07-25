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

	with open('./gma_secrets.json') as secrets: 
		sql_access = json.load(secrets)['mysql']
	
	return(sql_access)


def db_conn(test = 0):
	
	with open('./gma_secrets.json') as secrets:
		sql_access = json.load(secrets)['mysql']
	
	user = sql_access['username']
	password = sql_access['password']
	host = sql_access['host']
	db = sql_access['db']
	
	conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True) 
	cursor = conn.cursor()
	
	return(conn, cursor)


def db_engine(test = 0):
	
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


def get_object_s3(key):

	with open('./gma_secrets.json') as secrets:
		s3_access = json.load(secrets)['s3']

	access_key_id = s3_access['access_key_id']
	secret_access_key = s3_access['secret_access_key']
	bucket_name = s3_access['default_bucket']
	
	s3 = boto3.client('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)
	response = s3.get_object(Bucket= bucket_name, Key = key)
	file = response["Body"]
	
	return(file)


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

	with open('./recipients.json') as r:
		recipients = json.load(r)

	with open('./gma_secrets.json') as secrets:
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


# Input: String format date as found on scraped pages
# Output: String format date as we want to place in data-base
def format_date(date_string):

	date_object = datetime.datetime.strptime(date_string, '%d/%m/%Y')
	string_object = datetime.datetime.strftime(date_object, '%Y-%m-%d')

	return(string_object)


def get_matching_s3_objects(prefix="", suffix=""):
	"""
	Generate objects in an S3 bucket.
	:param prefix: Only fetch objects whose key starts with this prefix (optional).
	:param suffix: Only fetch objects whose keys end with this suffix (optional).
	Taken from: https://alexwlchan.net/2019/07/listing-s3-keys/
	Copyright © 2012–19 Alex Chan. Prose is CC-BY licensed, code is MIT.
    """
	
	with open('./gma_secrets.json') as secrets:
		s3_access = json.load(secrets)['s3']

	access_key_id = s3_access['access_key_id']
	secret_access_key = s3_access['secret_access_key']
	bucket_name = s3_access['default_bucket']
	
	s3 = boto3.client("s3", aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key)
	paginator = s3.get_paginator("list_objects_v2")
	kwargs = {'Bucket': bucket_name}
	
	# We can pass the prefix directly to the S3 API.  If the user has passed
	# a tuple or list of prefixes, we go through them one by one.
	if isinstance(prefix, str): prefixes = (prefix, )
	else: prefixes = prefix
	
	for key_prefix in prefixes: 
		kwargs["Prefix"] = key_prefix
	
		for page in paginator.paginate(**kwargs):
			
			try: contents = page["Contents"]
			except Exception as e: print(e) 
			
			for obj in contents:
				key = obj["Key"]
				if key.endswith(suffix): yield obj


def get_matching_s3_keys(prefix="", suffix=""):
	"""
	Generate the keys in an S3 bucket.
	:param bucket: Name of the S3 bucket.
	:param prefix: Only fetch keys that start with this prefix (optional).
	:param suffix: Only fetch keys that end with this suffix (optional).
	Taken from: https://alexwlchan.net/2019/07/listing-s3-keys/
	Copyright © 2012–19 Alex Chan. Prose is CC-BY licensed, code is MIT.
	"""
	for obj in get_matching_s3_objects(prefix, suffix): yield obj["Key"]


def format_date(date_string):
    
	date_string = date_string.strip()
	
	if date_string != None and date_string != '': 
		date_string = str(datetime.datetime.strptime(date_string, '%d/%m/%Y').date())
    
	elif date_string == None: 
		date_string = ''

	return(date_string)
