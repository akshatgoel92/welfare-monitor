'''
This file will scrape the URLS from the summary page of the
FTO tracking system and store them in a .json file. It will 
also upload them to the data-base. These URLs can then be used
as needed to scrape FTO content block by block rather than en masse.
'''

# Import packages
import requests
import json
import sys
import re
from bs4 import BeautifulSoup


def construct_summary_url(basic, state_name, state_code, 
				  		  district_name, district_code,
				  		  fin_year, meta):
	'''
	This file takes in input parameters from the calling code
	and joins them up to make the URL for the summary page.
	'''
	
	url = (basic + 'state_name=' + state_name +
		   '&state_code=' + state_code +
		   '&district_name=' + district_name + 
		   '&district_code=' + district_code +
		   '&fin_year=' + fin_year + meta)

	return(url)

def get_source(url):
	'''This function takes in a URL and outputs the source
	code as a bs4 object.'''
	
	response = requests.get(url)
	soup = BeautifulSoup(response.text, 'lxml')
	
	return(soup)

def get_links(soup, prefix = 'http://mnregaweb4.nic.in/netnrega/FTO/'):
	'''This will get the links from the page.'''

	links = [prefix + link.get('href') for link in soup.find_all('a', href=True)]

	links = [link for link in links if 'typ=' in link and 'typ=R' not in link]
	
	return(links)


# Get each stage of each FTO
def get_stages(links):

	stages = [re.findall('typ=(.+)&mode', link)[0] 
			  for link in links
			  if 'typ' in link and
			  'typ=R' not in link]

	return(stages)



def get_blocks(links):
	'''Get the block name of each FTO.'''

	blocks = [re.findall('block_name=(.+)&block', link)[0].title()
			  for link in links
			  if 'typ' in link and 
			  'typ=R' not in link]

	return(blocks)


def parse_links(blocks, stages, links):
	'''This will parse the links list and organize it 
	into a dictionary.'''

	parsed_links = {block: {stage: '' for stage in set(stages)} for block in set(blocks)}

	for index, link in enumerate(links):
		block = blocks[index]
		stage = stages[index]
		parsed_links[block][stage] = link
	
	return(parsed_links)


def output_links(parsed_links):
	'''This will output the returned dictionary as a .json file.'''

	with open('urls.json', 'w') as urls:

		json.dump(parsed_links, urls, indent = 4, sort_keys = True)


def main():
	'''This will execute the file.'''

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
		url = construct_summary_url(basic, state_name, state_code,
									district_name, district_code, 
									fin_year, meta)

	except Exception as e: 

		print('There is an error in the URL construction in the stage scrape...')
		sys.exit()
	
	try: 
		
		# Get the source code for the summary page as a bs4 object
		source = get_source(url)
		print(source)

	except Exception as e: 
		
		print(e)
		print('There is an error in getting the HTML from the summary page...')
		sys.exit()
	
	try: 
		# Store all the hyperlinks
		links = get_links(source)

	except Exception as e: 

		print('There is an error in getting the links from the HTML that we got from the summary page...')
		sys.exit()
	
	try: 
		# Store the FTO stages
		stages = get_stages(links)

	except Exception as e:
		
		print(e)
		print('There is an error getting FTO stages....')
		sys.exit()

	try: 
		
		# Store block names for each table		
		blocks = get_blocks(links)

	except Exception as e:
		
		print(e)
		print('There is an error getting the FTO blocks...')
		sys.exit()

	try:
		
		parsed_links = parse_links(blocks, stages, links)
		print(parsed_links)
		print('Parse done')

	except Exception as e:

		print(e)
		print('There is an error parsing the FTO links...')
		sys.exit()

	try: 

		output_links(parsed_links)

	except Exception as e:

		print(e)
		print('There is an error sending the FTO links to output')
		sys.exit()

		
if __name__ == '__main__':
	'''This executes the program.'''

	main()