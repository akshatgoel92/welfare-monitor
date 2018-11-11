# Import packages
# Install SQL connector as MySQLDb to ensure
# back-ward compatability
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
# Open the secrets file
# This gets credentials
# Return statement
def sql_connect():
	
	with open('./gma_secrets.json') as secrets:
		sql_access = json.load(secrets)['mysql']
	
	return(sql_access)

# Return connection and cursor
# Store credentials
# Create connection	
def db_conn():
	
	with open('./gma_secrets.json') as secrets:
		sql_access = json.load(secrets)['mysql']
	
	user = sql_access['username']
	password = sql_access['password']
	host = sql_access['host']
	db = sql_access['db']
	
	conn = pymysql.connect(host, user, 
							password, db, 
							charset="utf8", 
							use_unicode=True)
	cursor = conn.cursor()
	
	return(conn, cursor)
	
# Get rid of surrounding white-space for string variables
# Convert whatever fields that we can into title-case
def clean_item(item, title_fields):
	
	for field in item.keys():
		item[field] = item[field].strip() if type(item[field]) == str else item[field]
		
		if field in title_fields:
			item[field] = item[field].title()
	
	return(item)

# Get a table's keys
# Construct SQL table and return
def get_keys(table):
	
	
	with open('./backend/db/table_keys.json') as file:
		tables = json.load(file)
		keys = tables[table]
	
	return(keys)
	
# Insert a record into the SQL data-base
# Get the inputs to the SQL command
# Construct the SQL command and return
def insert_data(item, keys, table, unique = 0):
		
	
	keys = get_keys(table)
	fields = u','.join(keys)
	qm = u','.join([u'%s'] * len(keys))
	sql = "INSERT INTO " + table + " (%s) VALUES (%s)"
	sql_unique = "INSERT IGNORE INTO " + table + " (%s) VALUES (%s)"
	
	insert = sql if unique == 0 else sql_unique
	sql = insert % (fields, qm)
	data = [item[k] for k in keys]

	return(sql, data)

# Send e-mail
# Load credentials
def send_email(msg, subject, recipients):

	with open('./gma_secrets.json') as secrets:
		credentials = json.load(secrets)['smtp']
	
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
# Open the credentials JSON file
# Store credentials and create Dropbox object
# Upload the file with option to over-write if it already exists	
def dropbox_upload(file_from, file_to):
    
    with open('./gma_secrets.json') as data_file:
        credentials = json.load(data_file)
	
    access_token = credentials['dropbox']['access_token']
    dbx = dropbox.Dropbox(access_token)
    
    with open(file_from, 'rb') as f:
        dbx.files_upload(f.read(), 
                         file_to, 
                         mode = dropbox.files.WriteMode.overwrite)

# Uploads log file
def process_log(log_file_from, log_file_to):
	
	helpers.dropbox_upload(log_file_from, log_file_to)
	os.unlink(log_file_from)

# Update the FTO nos table
def update_fto_nos(block):
	pass