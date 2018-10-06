# Import packages
import os
import json
import sys
import dropbox
import pymysql
import smtplib
import pandas as pd

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *
from common.helpers import *

pymysql.install_as_MySQLdb()


# Get the FTO content stored in the data-base     
def get_fto_data():
	
	# Get FTO data
    conn, cursor = db_conn()
    fto_data = pd.read_sql("SELECT * FROM fto_content;", con = conn)
    fto_data.to_csv('./nrega_output/fto_data.csv', index = False)
    
    # Return statement
    conn.close()
    cursor.close()
    return(fto_data)


# Get call data    
def get_pilot_data(fto_data):
	
	# Load list of beneficiaries
	beneficiaries = pd.read_csv('./nrega_output/pilot_beneficiaries.csv', sep = ',')
	
	# Get and clean call data
	call_data = fto_data.dropna(how = 'any')
	call_data['credit_amt_actual'] = call_data.credit_amt_actual.astype(float)
	
	# Amount data processing
	amt_data = call_data.groupby('jcn', as_index = False).sum()
	
	# Rejection processing
	process_rejection = lambda x: (x['rejection_reason'] != 'x').sum()
	rejection_reason = call_data.groupby('jcn', as_index = False)
	rejection_reason = pd.DataFrame(rejection_reason.apply(process_rejection))
	rejection_reason.columns = ['rejection_reason']
	
	# Add stage data
	stage = pd.DataFrame([1, 2, 1, 2, 1, 2, 2, 2, 1, 2, 1, 2, 2, 1], columns = ['stage'])
	
	# Put data together and export
	call_data = pd.concat([amt_data, rejection_reason, stage], axis = 1)
	call_data.to_csv('./nrega_output/call_data.csv', index = False)
	
	# Join call data with beneficiary data
	call_sample = pd.concat([beneficiaries, call_data], axis = 1)
	
	# Create transaction date column 
	call_sample["transaction date"] = np.random.choice(pd.date_range('2018-04-04', '2018-04-20'), len(call_sample))
	
	# Forward fill for Niharika
	cols = ['jcn', 'credit_amt_actual', 'rejection_reason', 'stage']
	call_sample[cols] = call_sample[cols].ffill()
	call_sample.rename(columns={'credit_amt_actual':'amount'}, inplace=True)
	
	# Send output to file
	for col_no in range(1, 8):
		call_sample.to_csv('./nrega_output/call_' + str(col_no) + '.csv', index = False)
	

# Get pilot script	
def create_pilot_script():
	
	# Load beneficiaries data
	call_sample = pd.read_csv('./nrega_output/call_1.csv')
	beneficiaries = pd.read_csv('./nrega_output/pilot_beneficiaries.csv')
	
	# Create groups
	group_1 = call_sample.loc[(call_sample.rejection_reason == 1) & (call_sample.stage == 1)].index
	group_2 = call_sample.loc[(call_sample.rejection_reason == 1) & (call_sample.stage == 2)].index
	group_3 = call_sample.loc[call_sample.rejection_reason == 2].index
	
	# Create calling columns
	for col_no in range(1, 8):
		beneficiaries['Call Script ' + str(col_no)] = ''
	
	for col_no in range(1,8):
		
		beneficiaries.loc[group_1, 'Call Script ' + str(col_no)] = 'D E'
		beneficiaries.loc[group_2, 'Call Script ' + str(col_no)] = 'D F'
		beneficiaries.loc[group_3, 'Call Script ' + str(col_no)] = 'D G'
			
	# Output script to file 
	beneficiaries.to_csv('./nrega_output/sample.csv', index = False)

	
# Execute script	
if __name__ == '__main__':
	
	fto_data = get_fto_data()
	get_pilot_data(fto_data)
	create_pilot_script()
	