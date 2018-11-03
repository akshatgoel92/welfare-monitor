# Import packages
import os
import json 

# Import SQL modules
import pymysql
from sqlalchemy import *
from sqlalchemy.engine import reflection
pymysql.install_as_MySQLdb()

# Import item files
from nrega_scrape.items import FTOItem
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo
from common.helpers import sql_connect

# Creates the data-base
def create_db(engine):
	
	# Create engine
	# Create meta-data object
	metadata = MetaData()
	
	# FTO summary table			 
	fto_summary = Table('fto_summary', metadata,
										Column('id', Integer, 
													primary_key = True, 
													autoincrement = True), 
										Column('block_name', String(50), 
												ForeignKey('blocks.block_name')), 
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
										Column('scrape_date', String(50)), 
										Column('scrape_time', String(50))) 

	# FTO numbers table
	fto_numbers = Table('fto_nos', metadata, 
										Column('id', Integer, 
												primary_key = True, 
												autoincrement = True),
										Column('fto_no', String(50)), 
										Column('fto_stage', String(50)), 
										Column('state_code', String(50)),
										Column('district_code', String(50)),
										Column('block_code', Integer),
										Column('process_date', String(50)),
										Column('scrape_date', Date), 
										Column('scrape_time', Time))
	# FTO stages look-up table
	fto_stage = Table("fto_stages", metadata, 
										Column("fto_stage_name", String(50), 
												primary_key = True),
										Column("fto_stage_code", SmallInteger, 
												primary_key = True))
	
	# FTO scrape queue
	fto_scrape_queue = Table('fto_queue', metadata, 
										Column('fto_no', String(50),
												primary_key = True),
										Column('scrape_date', Date),
										Column('scrape_time', Time),
										Column('scrape_status', SmallInteger))
	# Transactions table
	transactions = Table('transactions', metadata, 
										Column('block_name', String(50), 
												ForeignKey("blocks.block_name")), 
										Column('jcn', String(50)), 
										Column('transact_ref_no', String(50), 
												primary_key = True),
										Column('transact_date', String(50)),
										Column('app_name', String(50)),
										Column('wage_list_no', String(50), 
												ForeignKey("wage_lists.wage_list_no")),
										Column('acc_no', String(50)),
										Column('credit_amt_due', Integer),
										Column('credit_amt_actual', Integer),
										Column('status', String(50)),
										Column('processed_date', String(50)),
										Column('utr_no', String(50)), 
										Column('rejection_reason', String(50)),
										Column('fto_no', String(50)))
	# FTO content table
	wage_list = Table('wage_lists', metadata, 
										Column('wage_list_no', String(50), 
												primary_key = True), 
										Column('block_name', String(50), 
												ForeignKey("blocks.block_name")))
	
	# Accounts table
	# Assumption: Primary account holder name and IFSC code is unique within a JCN and 
	# account number
	accounts = Table('accounts', metadata,
										Column("id", Integer, 
												primary_key = True, 
												autoincrement = True), 
										Column('jcn', String(50)), 
										Column('acc_no', String(50)),
										Column('ifsc_code', String(50), 
												ForeignKey('banks.ifsc_code')),
										Column('prmry_acc_holder_name', 
												String(50)))
	
	# Banks table
	banks = Table('banks', metadata, 
										Column('ifsc_code', String(50), 
												primary_key = True), 
										Column('bank_code', String(30)))
	
	# Blocks table
	blocks = Table('blocks', metadata, 
										Column('block_name', String(50), 
												primary_key = True), 
										Column('block_code', SmallInteger))
	
	# Create the tables
	metadata.create_all(engine)

def create_block_table(engine):

	with open('./backend/db/blocks.json') as block_codes:
		blocks = json.loads(block_codes)

	pass

# Create a JSON file with keys
def send_keys_to_file(engine):
	
	# Get the table names and store them as a dictionary
	inspector = reflection.Inspector.from_engine(engine)
	# Store the table names
	tables = dict.fromkeys(inspector.get_table_names(), '')
	# Now for each table:
	for table in tables:
		# First store the columns and then put them in the table dictionary 
		tables[table] = [column['name'] for column in inspector.get_columns(table)
						if 'autoincrement' not in column.keys()]
	# Dump the table dictionary to a JSON file 	
	with open('./backend/db/table_keys.json', 'w') as file:
		json.dump(tables, file, sort_keys=True, indent=4)
		
# Execute
if __name__ == '__main__':
	
	# Get values
	user, password, host, db = sql_connect().values()
	# Get engine
	engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)
	# Create data-base
	create_db(engine)
	# Send keys to file
	send_keys_to_file(engine)