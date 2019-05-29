# Imports
import requests
import pandas as pd
import numpy as np
import re 
import os
import glob
from bs4 import BeautifulSoup


# Get soup
def get_soup(url):

	source = requests.get(url)
	content = source.text 
	soup = BeautifulSoup(content, "lxml")
	print('Soup')
	
	return(soup)
	
# Get the links on the page
def get_links(soup):

	# Need the prefix because links are shortened in the HTML
	prefix = 'http://mnregaweb4.nic.in/netnrega/FTO/'
	all_links = [prefix + link.get('href') for link in soup.find_all('a', href=True)]
	rejected_links = [link for link in all_links if 'typ=R' in link]
	
	return(rejected_links)


# Get table
def get_table(soup):
	
	table = soup.find_all("table")[-1]
	print('table')
	
	return(table)

# Input: 
# Output:
def get_data(table):

	all_data = []	
	columns = ['sr_no', 'fto_no', 'ref_no', 'utr_no', 'transact_date', 'app_name', 'acc_name', 
			   'wage_list_no', 'bank_code', 'ifsc_code', 'amt_credit_due', 'rejection_date',
			   'rejection_reason']                               

	for row in table.find_all('tr')[1:]:
		cols = row.find_all('td')
		row_data=[col.text.strip() for col in cols]
		all_data.append(row_data)

	print('done')	

	return(pd.DataFrame(all_data, columns = columns))

# Append data-frames
def get_location_codes(target_urls, path):

	district_name = [re.findall('district_name=(.+)&district_code', link)[-1] for link in target_urls]
	district_code = [re.findall('district_code=(.+)&fin', link)[-1] for link in target_urls]
	location_codes = pd.DataFrame([district_name, district_code], ).T
	location_codes.columns = ['district_name', 'district_code']
	location_codes.to_csv(path + 'location_codes.csv')

	return

def append(path):

	# Change directory
	os.chdir(path)
	# Initialize empty data-frame
	df = pd.DataFrame()
	# For each file
	for file in glob.glob("*_rejected_payments_*.csv"):
    
		# Print files
		print(file)
		# Print concatenate
		df = pd.concat([df,pd.read_csv(file)])

	# Create the district code	
	df['district_code'] = df['fto_no'].apply(lambda x: re.findall('.{2}(.{4}).+', x)[-1])

	df.to_csv(path + 'all_data.csv')

	# Return the full data-frame 	
	return(df)




# Execute
if __name__ == '__main__':

 	base_url = ('http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?' 
 				'page=s&mode=B'
 				'&flg=W&state_name=MADHYA%20PRADESH'
 				'&state_code=17'
 				'&fin_year=2018-2019' 
 				'&dstyp=B'
 				'&source=national'
 				'&Digest=5DA1ZSxezvKXndS3loV25w')


 	path = '/Users/akshatgoel/Desktop/mp_rp_18_19/'	
 	base_soup = get_soup(base_url)
 	target_urls = get_links(base_soup)
 	get_location_codes(target_urls, path)
 	

 	i = 0

 	for url in target_urls: 
 		i += 1
 		print(i)
 		try:
 			soup = get_soup(url)
 			table = get_table(soup)
 			data = get_data(table)
 			data.to_csv(path + 'mp_rejected_payments_' + str(i) + '.csv')

 		except Exception as e:
 			print('Error')
		
 	for path in paths: 
 		append(path)

 	