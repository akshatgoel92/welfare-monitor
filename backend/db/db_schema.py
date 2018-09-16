# Import packages
import os
import json 

# Import SQL modules
import pymysql
from sqlalchemy import *
pymysql.install_as_MySQLdb()

# Import item files
from nrega_scrape.items import FTOItem
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from common.helpers import sql_connect

# Store credentials for data-base access
user, password, host, db = sql_connect().values()

# Create engine
engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	
# Create meta-data object
metadata = MetaData()

# FTO summary table			 
fto_summary = Table('fto_summary', metadata, 
							   		Column('block_name', String(100)), 
							   		Column('total_fto', Integer), 
							   		Column('first_sign', Integer),
							   		Column('first_sign_pending', Integer),
							   		Column('second_sign', Integer),
							   		Column('second_sign_pending', Integer),
							   		Column('fto_sent_bank', Integer),
							   		Column('transact_sent_bank', Integer),
							   		Column('fto_processed_bank', Integer),
							   		Column('transact_processed_bank', Integer),
							   		Column('fto_partial_bank', Integer),
							   		Column('transact_partial_bank', Integer),
							   		Column('fto_pending_bank', Integer),
							   		Column('transact_pending_bank', Integer),
							   		Column('transact_processed_bank_resp', Integer), 
							   		Column('invalid_accounts_bank_resp', Integer), 
							   		Column('transact_rejected_bank_resp', Integer), 
							   		Column('transact_total_bank_resp', Integer), 
							   		Column('url', String(100)),
							   		Column('spider', String(100)),
							   		Column('server', String(100)),
							   		Column('date', String(100)))

# FTO numbers table
fto_numbers = Table('fto_numbers', metadata, 
							   	  Column('fto_no', String(100)), 
							   	  Column('fto_stage', String(100)), 
							   	  Column('state_code', String(100)),
							   	  Column('district_code', String(100)),
							   	  Column('block_code', String(100)),
							   	  Column('process_date', String(100)),
							   	  Column('url', String(100)),
							   	  Column('spider', String(100)),
							   	  Column('server', String(100)),
							   	  Column('date', String(100)))
							   	  
# FTO content table
fto_content = Table('fto_content', metadata, 
							   	 Column('block_name', String(100)), 
							   	 Column('jcn', String(100)), 
							   	 Column('transact_ref_no', String(100)),
							   	 Column('transact_date', String(100)),
							   	 Column('app_name', String(100)),
							   	 Column('prmry_acc_holder_name', String(100)),
							   	 Column('wage_list_no', String(100)),
							   	 Column('acc_no', String(100)),
							   	 Column('bank_code', String(100)),
							   	 Column('ifsc_code', String(100)),
							   	 Column('credit_amt_due', String(100)),
							   	 Column('credit_amt_actual', String(100)),
							   	 Column('status', String(100)),
							   	 Column('processed_date', String(100)),
							   	 Column('utr_no', String(100)), 
							   	 Column('rejection_reason', String(100)), 
							   	 Column('server', String(100)), 
							   	 Column('fto_no', String(100)), 
							   	 Column('scrape_date', String(100)),
							   	 Column('time_taken', String(100)),
							   	 Column('url', String(100)))

# Create the tables							   	 
metadata.create_all(engine)

	

    	