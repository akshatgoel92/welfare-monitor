# Import packages
import os
import json
import sys
import dropbox
import pymysql
import smtplib

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

pymysql.install_as_MySQLdb()


# Connect to AWS RDB
def sql_connect():
	
	# Open the secrets file
	# This gets credentials
	# Return statement	
	with open('./gma_secrets.json') as secrets:
		
		sql_access = json.load(secrets)['mysql']
	
	return(sql_access)

# Return connection and cursor
def db_conn():
	
	with open('./gma_secrets.json') as secrets:
		# This gets credentials
		sql_access = json.load(secrets)['mysql']
	
	# Store credentials
	user = sql_access['username']
	password = sql_access['password']
	host = sql_access['host']
	db = sql_access['db']
	
	# Create connection	
	conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
	cursor = conn.cursor()
	
	# Return statement
	return(conn, cursor)
	
	
def clean_item(item, title_fields):
	
	for field in item.keys():
		# Get rid of surrounding white-space for string variables
		item[field] = item[field].strip() if type(item[field]) == str else item[field]
		
		# Convert whatever fields that we can into title-case
		if field in title_fields:
			item[field] = item[field].title()
	
	# Return statement
	return(item)

# Get a table's keys
def get_keys(table):
	
	# Construct SQL table and return
	with open('./backend/db/table_keys.json') as file:
		tables = json.load(file)
		keys = tables[table]
	# Return statement	
	return(keys)
	
# Insert a record into the SQL data-base
def insert_data(item, keys, table, unique = 0):
		
	# Get the inputs to the SQL command
	keys = get_keys(table)
	fields = u','.join(keys)
	qm = u','.join([u'%s'] * len(keys))
	sql = "INSERT INTO " + table + " (%s) VALUES (%s)"
	sql_unique = "INSERT IGNORE INTO " + table + " (%s) VALUES (%s)"
	
	# Construct the SQL command and return
	insert = sql if unique == 0 else sql_unique
	sql = insert % (fields, qm)
	data = [item[k] for k in keys]
	return(sql, data)

# Send e-mail
def send_email(msg, subject, recipients):

	# Load credentials
	with open('./gma_secrets.json') as secrets:
		credentials = json.load(secrets)['smtp']
	
	# Store credentials	
	user = credentials['user']
	password = credentials['password']
	region = credentials['region']
	
	# SMTP server details
	smtp_server = 'email-smtp.' + region + '.amazonaws.com'
	smtp_port = 587
	sender = 'akshat.goel@ifmr.ac.in'
	text_subtype = 'html'
	
	# Compose message
	msg = MIMEText(msg, text_subtype)
	msg['Subject']= subject
	msg['From'] = sender
	msg['To'] = ', '.join(recipients)
	
	# Connection
	conn = SMTP(smtp_server, smtp_port)
	conn.set_debuglevel(1)
	conn.ehlo()
	conn.starttls()
	conn.ehlo()
	conn.login(user, password)
	conn.sendmail(sender, recipients, msg.as_string())
	conn.close()

# Upload file to Dropbox 	
def dropbox_upload(file_from, file_to):
    
    with open('./gma_secrets.json') as data_file:
        credentials = json.load(data_file)
	
	# Store credentials and create Dropbox object
    access_token = credentials['dropbox']['access_token']
    dbx = dropbox.Dropbox(access_token)
    
    # Upload the file with option to over-write if it already exists
    with open(file_from, 'rb') as f:
        dbx.files_upload(f.read(), 
                         file_to, 
                         mode = dropbox.files.WriteMode.overwrite)
