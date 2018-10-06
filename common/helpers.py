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
	with open('./gma_secrets.json') as secrets:
		# This gets credentials
		sql_access = json.load(secrets)['mysql']
	# Return statement	
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
	
# Insert a record into the SQL data-base
def insert_data(item, insert):
		
	# Construct the SQL command
	keys = item.keys()
	fields = u','.join(keys)
	qm = u','.join([u'%s'] * len(keys))
	sql = insert % (fields, qm)
	# Get the data for the data-base
	data = [item[k] for k in keys]
	# Return
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
        dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

 	
 				

	
   
   
   

    	