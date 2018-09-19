# Import packages
import os
import json 
import pymysql
pymysql.install_as_MySQLdb()

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *


# Connect to AWS RDB
def sql_connect():

	with open('./gma_secrets.json') as secrets:
		
		sql_access = json.load(secrets)['mysql']
		
	return(sql_access)


# Return connection and cursor
def db_conn():

	user, password, host, db = sql_connect().values()
		
	conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
		
	cursor = conn.cursor()
	
	return(conn, cursor)
	
# Send an attachment via e-mail	
def send_file(file, subject, recipients):
	
	with open('./gma_secrets.json') as secrets:
		
		mail_access = json.load(secrets)['e-mail']
	
	msg = MIMEMultipart()
	
	msg['Subject'] = subject
	
	msg['From'] = 'python@python.com'
	
	msg['To'] = ', '.join(recipients)
	
	part = MIMEBase('application', "octet-stream")
	
	part.set_payload(open(file, "rb").read())
	
	encoders.encode_base64(part)
	
	part.add_header('Content-Disposition', 'attachment; filename=' + file)
	
	msg.attach(part)
	
	s = smtplib.SMTP('smtp.gmail.com' , 587)
	
	s.starttls()
	
	s.login(mail_access['user_name'], mail_access['password'])

	s.sendmail(mail_access['user_name'], recipients, msg.as_string())
	
	s.quit()
	
   
   
   

    	