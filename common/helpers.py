# Python standard
import os
import json

# SQL interface 
import pymysql
pymysql.install_as_MySQLdb()

# E-mail packages
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *


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
	
	# Store credentials
	user, password, host, db = sql_connect().values()
	# Create connection	
	conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
	# Create cursor	
	cursor = conn.cursor()
	# Return statement
	return(conn, cursor)
	
	
# Send an attachment via e-mail	
def send_file(file, subject, recipients):
	
	# Get credentials
	with open('./gma_secrets.json') as secrets:
		# E-mail ID and password
		mail_access = json.load(secrets)['e-mail']
	
	# Create mime object
	msg = MIMEMultipart()
	# Store subject
	msg['Subject'] = subject
	# Store source
	msg['From'] = 'python@python.com'
	# Store target
	msg['To'] = ', '.join(recipients)
	
	# Store mime-base object
	part = MIMEBase('application', "octet-stream")
	# Set payload
	part.set_payload(open(file, "rb").read())
	# Set the encoder object
	encoders.encode_base64(part)
	# Add a header to the message
	part.add_header('Content-Disposition', 'attachment; filename=' + file)
	# Attach file
	msg.attach(part)
	
	# Set up server
	s = smtplib.SMTP('smtp.gmail.com' , 587)
	# Start TLLS
	s.starttls()
	# Login to e-mail
	s.login(mail_access['user_name'], mail_access['password'])
	# Send e-mail
	s.sendmail(mail_access['user_name'], recipients, msg.as_string())
	# Quit
	s.quit()
	
   
   
   

    	