# Import packages
import pandas as pd
import requests
import re
import os
import sys 

# Import sub-modules
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy.engine import reflection
from sqlalchemy import *


# Import helpers
sys.path.append(os.getcwd())
from common import helpers

# Construct URL 
def construct_url(basic, state_name, state_code, 
				  district_name, district_code, 
				  fin_year, meta):

	# Construct state component of URL
	url = (basic + 'state_name=' + state_name +
		   '&state_code=' + state_code +
		   '&district_name=' + district_name + 
		   '&district_code=' + district_code +
		   '&fin_year=' + fin_year + meta)

	return(url)

# Get source
def get_source(url):
	print(url)
	response = requests.get(url)
	soup = BeautifulSoup(response.text, 'lxml')
	print('Done')
	return(soup)

# Get links from the source
def get_links(soup):

	prefix = 'http://mnregaweb4.nic.in/netnrega/FTO/'
	links = [prefix + link.get('href') for link in soup.find_all('a', href=True)]
	
	return(links)

# Scrape the summary table
def get_summary_data(soup):

	pass

# Get FTO nos.
def get_ftos(soup, link):

	# Create place-holder for FTO data
	fto_nos = []
	print(link)
	# Store the table
	try:  
		table = soup.find_all('table')[-1]

	except Exception as e:
		print(e)
		print('Table not found...exiting program!')
		sys.exit()
			
	try: 

		# Store the text in the last span tag
		location = soup.find_all('span')[-1].get_text()

		# Store block
		block = re.findall('Block.*:(.+)', location)[-1].strip().title()
	
	except Exception as e:
		print('The block name has not been found...exiting program!')
		sys.exit()

	try: 
		# Scrape the table
		for row in table.find_all('tr')[1:]:
			cols = row.find_all('td')[1:]
			col_text = [col.get_text().strip() for col in cols]
		
			# We don't want any rows with the word Total in them 
			# We only want FTO nos.		
			if 'Total' not in col_text:
				fto_nos.append(col_text)

	except Exception as e:
		print(e)
		print('There is a table scrape error....exiting program!')
		sys.exit()
	
	# Convert the lists into a data-frame
	fto_nos = pd.DataFrame(fto_nos)

	# Store the block location
	fto_nos['block'] = block

	return(fto_nos)

# Allocate the stages
def get_stage(fto_nos, fto_stages):

	# Store the stages
	stages = ['pb', 'sb', 'sec_sig', 'fst_sig_not', 'sec_sig_not', 'fst_sig', 'pp', 'P']

	# For each stage store a list of indices in the stages table at that stage
	indices = {stage: [i for i in range(len(fto_stages)) if fto_stages[i] == stage] for stage in stages}

	# Now create the data-frames
	fto_nos = {stage: pd.concat([fto_nos[i] for i in indices[stage]]) for stage in indices}

	return(fto_nos)

# Make stage table
def get_stage_table(fto_type, fto_nos, engine):

	# Store the stage names to use as SQL table names
	stages =	{'pb': 'fto_processed_by_bank', 'sb': 'fto_sent_to_bank', 
				'sec_sig': 'fto_second_sign_done', 'fst_sig': 'fto_first_sign_done',
				'fst_sig_not': 'fto_first_sign_pending', 'sec_sig_not': 'fto_second_sign_pending',
				'pp': 'fto_partial_processed_by_bank', 'P': 'fto_pending_bank_processing'}

	# Block
	block = ['sec_sig', 'fst_sig', 'fst_sig_not', 'sec_sig_not']

	# Bank
	bank = [stage for stage in stages.keys() if stage not in block]

	# Tables
	table = stages[fto_type]

	columns = ['fto_no', 'institution', 'sign_date', 
			   'total_transact_due', 'total_amt_due',
			   'total_transact_processed', 'total_amt_processed',
			   'total_transact_rejected', 'total_amt_rejected',
			   'total_credited_amt','total_invalid_account', 'block']

	# Store the column names for the block stage
	if fto_type in bank:
		
		columns.remove('sign_date')

	# Rename the columns
	if fto_nos.empty:
		
		fto_nos = fto_nos.reindex(columns = columns)

	else: 
		fto_nos.columns = columns

	# Keep only those entries with the substring 'FTO' in them
	fto_nos = fto_nos.loc[fto_nos['fto_no'].apply(lambda x: 'FTO' in x)]
	
	# Write the stage-wise table to data-base	
	fto_nos.to_sql(table, con = engine, index = False, if_exists = 'replace', chunksize = 100)

	return(fto_nos)


# Get current stage
def get_current_stage_table(stage_tables, engine):
	
	stages =	{'pb': 8, 'sb': 5, 'sec_sig': 4, 'fst_sig': 2,
				'fst_sig_not': 1, 'sec_sig_not': 3, 'pp': 7, 'P': 6}
	
				
	for stage, fto_nos in stage_tables.items():
		
		fto_nos['stage'] = stages[stage]
		fto_nos = fto_nos[['fto_no', 'stage']]
		
	fto_current_stage = pd.concat(stage_tables.values())

	fto_current_stage['current_stage'] = fto_current_stage.groupby(['fto_no'])['stage'].transform(max)

	fto_current_stage = fto_current_stage[['fto_no', 'current_stage']]

	fto_current_stage['date'] = str(datetime.today().strftime('%d-%m-%Y'))

	fto_current_stage.drop_duplicates(inplace = True)

	print('There are ' + str(len(fto_current_stage)) + ' entries! This is nuts!')

	return(fto_current_stage)
	
# Get new FTOs and put them in the queue
def get_new_ftos(fto_current_stage, engine):

	# Store the new FTOs as today's date 
	date_today = str(datetime.today().strftime('%d-%m-%Y'))

	# Read the FTO queue
	fto_queue = pd.read_sql('SELECT fto_no FROM fto_queue', con = engine)

	# Do a left join because we want to keep all the FTOs in the current stage table
	new_ftos = pd.merge(fto_current_stage, fto_queue, how = 'left', 
						on = ['fto_no'], indicator = True)

	# Now keep only those observations from the stage table which did not merge 
	new_ftos = new_ftos.loc[new_ftos['_merge'] == 'left_only']

	# Now keep only the new FTO numbers column
	new_ftos = new_ftos['fto_no']

	new_ftos['done'] = 0

	new_ftos['fto_type'] = ''
	
	return(new_ftos)
		
# Execute 
if __name__ == '__main__':
	
	# Store the variables we need for the summary page URL
	basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&'
	state_name = 'CHHATTISGARH'
	state_code = '33'
	district_name = 'RAIPUR'
	district_code = '3316'
	fin_year = '2018-2019'
	meta = '&dstyp=B&source=national&Digest=tuhEXy+HR52YT8lJYijdtw'
	
	try: 
		# Construct the URL for the summary page
		url = construct_url(basic, state_name, state_code,
							district_name, district_code, fin_year, meta)

	except Exception as e: 

		print('There is an error in the URL construction in the stage scrape...')
		sys.exit()
	
	try: 
		# Get the source code for the summary page
		source = get_source(url)

	except Exception as e: 

		print('There is an error in getting the HTML from the summary page...')
		sys.exit()
	
	try: 
		# Store all the hyperlinks
		links = get_links(source)

	except Exception as e: 

		print('There is an error in getting the links from the HTML that we got from the summary page...')
		sys.exit()
	
	try: 
		# Link content as BeautifulSoup objects
		soup_links = [get_source(link) 
					  for link in links 
					  if 'typ' in link and 
					  'typ=R' not in link]

	except Exception as e:
		print(e)
		print('There is an error in making the link content into BeautifulSoup objects...now exiting the program.')
		sys.exit()

	try: 
		# Store the FTO types
		types = [re.findall('typ=(.+)&mode', link)[0] 
				for link in links
				if 'typ' in link and
				'typ=R' not in link]

	except Exception as e:
		print(e)
		print('There is an error getting FTO types....')
		sys.exit()

	# Store block names for each table		
	blocks = [re.findall('block_name=(.+)&block', link)[0].title()
			  for link in links
			  if 'typ' in link and 
			  'typ=R' not in link]

	try:
		# Get database credentials and then create an engine to use for the SQL connection
		user, password, host, db = helpers.sql_connect().values()
		# Create an engine object
		engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db)

		# Begin the transaction
		conn = engine.connect()
		trans = conn.begin()
	
	except Exception as e:
		print(e)
		print('Error creating engine...')
		sys.exit()

	try:
		# Now get the FTO nos.
		fto_final = [get_ftos(soup, link) for soup, link in zip(soup_links, links)]

	except Exception as e:
		print(e)
		print('Error getting FTO numbers...')
		trans.rollback()
		conn.close()
		sys.exit()

	try:
		# Now reorganize the data-sets by stage rather than block
		fto_stages = get_stage(fto_final, types)

	except Exception as e:
		print(e)
		print('There was an error organizing the data-sets by stage...')
		trans.rollback()
		conn.close()
		sys.exit()

	try:
		# Now add column names and write these tables to data-base
		fto_stages = {fto_stage: get_stage_table(fto_stage, fto_nos, conn) for fto_stage, fto_nos in fto_stages.items()}
		
	except Exception as e:
		print(e)
		print('There was an error writing the stage-wise tables to the data-base...')
		trans.rollback()
		conn.close()
		sys.exit()

	try:
		# Next create a table for the current stage of the FTO and write that to the data-base
		fto_current_stage = get_current_stage_table(fto_stages, conn)
		fto_current_stage.to_sql('fto_current_stage', con = conn, 
								index = False, if_exists = 'replace', chunksize = 100)
		

	except Exception as e:
		print(e)
		print('There was an error writing current stage table to the data-base...')
		trans.rollback()
		conn.close()
		sys.exit()

	try: 
		# Next figure out which FTOs to append to the FTO queue
		new_ftos = get_new_ftos(fto_current_stage, conn)
		# Now write this file to Excel and get 
		new_ftos.to_sql('fto_queue', con = conn, index = False, if_exists = 'append')
		trans.commit()
		conn.close()

	except Exception as e:
		print(e)
		print('Error appending to FTO queue...')
		trans.rollback()
		conn.close()
		sys.exit()