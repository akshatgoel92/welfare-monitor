# Import packages
import pandas as pd
import requests
import re
import os 

# Import sub-modules
from bs4 import BeautifulSoup

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
def get_ftos(soup):

	# Create place-holder for FTO data
	fto_nos = []

	# Store the table 
	table = soup.find_all('table')[-1]

	# Store the text in the last span tag
	location = soup.find_all('span')[-1].get_text()

	# Store block
	block = re.findall('Block.*:(.+)', location)[-1].strip().title()

	# Scrape the table
	for row in table.find_all('tr')[1:]:
		cols = row.find_all('td')[1:]
		col_text = [col.get_text().strip() for col in cols]
		
		if 'Total' in col_text: 
			pass
		
		else: 

			fto_nos.append(col_text)
	
	# Convert the lists into a data-frame
	fto_nos = pd.DataFrame(fto_nos)
	
	# Store the block location
	fto_nos['block'] = block

	return(fto_nos)

# Allocate the stages
def get_stage(fto_no, fto_stages):

	# Store the stages
	stages = ['pb', 'sb', 'sec_sig', 'fst_sig_not', 'sec_sig_not', 'fst_sig', 'pp', 'P']

	# Create a dictionary of indices 
	indices = { stage: [i for i in range(len(fto_stages)) if fto_stages[i] == stage] for stage in stages}

	# Now create the data-frames
	fto_nos = {stage: pd.concat([fto_nos[i] for i in indices[stage]]) for stage in indices}

	return(fto_nos)

# Make columns
def make_data(fto_no):

	# Store the stage names to use as SQL table names
	stages =	{'pb': 'fto_processed_by_bank', 'sb': 'fto_sent_to_bank', 
				'sec_sig': 'fto_second_sign_done', 'fst_sig': 'fto_first_sign_done',
				'fst_sig_not': 'fto_first_sign_pending', 'sec_sig_not': 'fto_second_sign_pending',
				'pp': 'fto_partial_processed_by_bank', 'P': 'fto_pending_bank_processing'}

	# Block
	block = ['sec_sig', 'fst_sig', 'fst_sig_not', 'sec_sig_not']

	# Bank
	bank = [stage for stage in stages.keys if stage not in block]

	# Store the column names for the block stage
	if fto_type in block:
	
		columns = ['fto_no', 'institution', 'sign_date', 
				   ]
	
	# Store the column names for the bank stage
	elif fto_type in bank:

		columns = ['']

	pass

def write_data():
	pass

		
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
	
	# Construct the URL for the summary page
	url = construct_url(basic, state_name, state_code,
						district_name, district_code, fin_year, meta)
	
	# Get the source code for the summary page
	source = get_source(url)
	
	# Store all the hyperlinks
	links = get_links(source)
	
	# Link content as BeautifulSoup objects
	soup_links = [get_source(link) 
				  for link in links 
				  if 'typ' in link and 
				  'typ=R' not in link]
	
	# Store the FTO types
	types = [re.findall('typ=(.+)&mode', link)[0] 
			for link in links
			if 'typ' in link and
			'typ=R' not in link]

	blocks = [re.findall('block_name=(.+)&block', link)[0].title()
			  for link in links
			  if 'typ' in link and 
			  'typ=R' not in link]
	
	# Get the FTO nos
	fto_final = [get_ftos(soup) for soup in soup_links]


