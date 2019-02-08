#------------------------------------------------------------------#
# Author: Akshat Goel
# Date: 8th August 2018
# Python version: 3.6.3
# Dependencies:

# [Only modules outside Python standard listed]
# 1) scrapy 
# 2) pandas 
# 3) numpy 
# 4) Selenium
#------------------------------------------------------------------#

# Import packages
import pandas as pd
import argparse
import requests
import json
import re
import os
import sys


from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy.engine import reflection
from sqlalchemy import *


sys.path.append(os.getcwd())
from common import helpers


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
	'''This will get the stages from each link.'''

	stages = [re.findall('typ=(.+)&mode', link)[0] 
			  for link in links
			  if 'typ' in link and
			  'typ=R' not in link]

	return(stages)



def get_blocks(links):
	'''This will get the block name of each FTO.'''

	blocks = [re.findall('block_name=(.+)&block', link)[0].title()
			  for link in links
			  if 'typ' in link and 
			  'typ=R' not in link]

	return(blocks)


def parse_links(blocks, stages, links):
	'''This will parse the links list and organize it into a dictionary.'''

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


def get_table(soup):
	'''Get table'''

	table = soup.find_all('table')[-1]

	return(table)

# Get FTO nos.
def get_ftos(table):

	fto_nos = [] 
		
	for row in table.find_all('tr')[1:]:
		cols = row.find_all('td')[1:]
		col_text = [col.get_text().strip() for col in cols]
		
		# We don't want any rows with the word Total in them 	
		if 'Total' not in col_text:
			
			fto_nos.append(col_text)

	return(fto_nos)

# Add the block name to the FTO list
def clean_ftos(fto_nos, block):

	fto_nos = pd.DataFrame(fto_nos)
	
	fto_nos['block'] = block

	return(fto_nos)


# Depending on the stage
def get_stage_table_columns(fto_type):

	table_name = stages[fto_type]
	
	if fto_type in bank:
		
		columns.remove('sign_date')

	return(table_name, columns)


# Write a table for each stage to the DB
def stage_tables_to_db(fto_type, fto_nos, engine):

	if fto_nos.empty:
		
		fto_nos = fto_nos.reindex(columns = columns) 

	else: 
		
		fto_nos.columns = columns

	fto_nos = fto_nos.loc[fto_nos['fto_no'].apply(lambda x: 'FTO' in x)]
	
	fto_nos.to_sql(table, con = engine, index = False, if_exists = 'replace', chunksize = 100)

	return(fto_nos)


# Get current stage
def current_stage_to_db(stage_tables, engine):
	
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

	date_today = str(datetime.today().strftime('%d-%m-%Y'))

	fto_queue = pd.read_sql('SELECT fto_no FROM fto_queue', con = engine)

	new_ftos = pd.merge(fto_current_stage, fto_queue, how = 'left', on = ['fto_no'], indicator = True)

	new_ftos = new_ftos.loc[new_ftos['_merge'] == 'left_only']

	new_ftos.drop(['_merge'], inplace = True)

	new_ftos['done'] = 0

	new_ftos['fto_type'] = ''

	new_ftos.to_sql('fto_queue', con = conn, index = False, if_exists = 'append')
	
	return(new_ftos)