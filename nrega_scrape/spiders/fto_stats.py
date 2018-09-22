# -*- coding: utf-8 -*-

# Import packages
import scrapy
import datetime
import socket
import re
import os
import logging
import pandas as pd
import numpy as np

# Import sub-modules
from itertools import chain
from scrapy import Spider
from scrapy.contrib.spiders import CrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from scrapy.utils.log import configure_logging
from dateutil.parser import parse
from nrega_scrape.items import NREGAItem
from nrega_scrape.items import FTONo

# Spider class
class FtoSpider(CrawlSpider):
    '''Input: MNREGA FTO URL 
      Output: Scraped FTO summary statistics'''
    
    # Construct the target URL
    name = 'fto_stats'
    # Basic URL
    basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&'
    # State name 
    state_name = 'CHHATTISGARH'
    # State code
    state_code = '33'
    # District name
    district_name = 'RAIPUR'
    # District code
    district_code = '3316'
    # Financial year
    fin_year = '2018-2019'
    
    # Construct state component of URL
    state = 'state_name=' + state_name +'&state_code=' + state_code
    # Construct district component of URL
    district = '&district_name=' + district_name + '&district_code=' + district_code 
    # Construct financial year component of URL
    fin_year_url = '&fin_year=' + fin_year
    # Construct remaining part of URL
    meta = '&dstyp=B&source=national&Digest=tuhEXy+HR52YT8lJYijdtw'
    
    # Concatenate all components and put them together
    start_urls = [basic + state + district +fin_year_url + meta]
    
    
    # Parse the response	        					
    def parse(self, response):
    	
    	# Store the last table of the response
    	table = response.xpath('//table')[-1]
    	# Store the URLs in the table
    	# We will follow these in the next parse function
    	urls = table.xpath('*//a//@href').extract()
    	
    	# Iterate through each row of the table
    	for row in self.parse_fto_summary(table):
    		# Yield it to the processing pipeline
    		yield(row)
    	
    	# Now go through each hyperlink on the table
    	for url in urls:
 			
 			# Call the FTO list parser on each URL 
 			# Yield the result of the response to processing pipeline
    		yield(response.follow(url, self.parse_fto_list))	
    
    	
    # Parse the FTO summary table
    def parse_fto_summary(self, table):
    	
    	# Store table data
    	table_data = [table.xpath('*/td[' + str(col) + ']//text()').extract() for col in range(21)]
    	# Clean table data
    	table_data = [MapCompose(lambda s: s.replace("\r\n", ''), str.strip)(col) for col in table_data]
    	# Clean table data
    	table_data = [list(filter(None, col))[-6:] for col in table_data]
    	# Get rid of unneeded elements
    	table_data = [table_col for table_col in table_data if table_col != []]
    	# Convert into a data-frame
    	table_data = pd.DataFrame(table_data).T
    	# Discard unneeded rows
    	table_data = table_data.iloc[1:-1,1:-1]
    	
    	# Create item object	
    	item = NREGAItem()
        
        # Iterate through rows	
    	for row in table_data.index:
    		
    		# Block name
    		item['block_name'] = table_data.loc[row ,1]
    		# Total FTOs
    		item['total_fto'] = table_data.loc[row, 2]
         	# First sign
    		item['first_sign'] = table_data.loc[row , 3]
    		# First sign pending
    		item['first_sign_pending'] = table_data.loc[row , 4]
    		
    		# Second sign
    		item['second_sign'] = table_data.loc[row , 5]
    		# Second sign pending
    		item['second_sign_pending'] = table_data.loc[row , 6]
    		# FTO sent to bank
    		item['fto_sent_bank'] = table_data.loc[row , 7]
    		# Transactions sent to bank
    		item['transact_sent_bank'] = table_data.loc[row, 8]
    		
    		# FTO processed by bank
    		item['fto_processed_bank'] = table_data.loc[row, 9]
    		# Transaction processed by bank
    		item['transact_processed_bank'] = table_data.loc[row, 10]
    		# FTO partially processed by bank
    		item['fto_partial_bank'] = table_data.loc[row, 11]
    		# Transactions partially processed by banks
    		item['transact_partial_bank'] = table_data.loc[row, 12]
    		
    		# Total FTO pending at bank
    		item['fto_pending_bank'] = table_data.loc[row, 13]
    		# Transaction pending at bank
    		item['transact_pending_bank'] = table_data.loc[row, 14]
    		# Transaction processed bank
    		item['transact_processed_bank_resp'] = table_data.loc[row, 15]
    		# Invalid accounts bank
    		item['invalid_accounts_bank_resp'] = table_data.loc[row, 16]
    		
    		# No. of transactions rejected by bank
    		item['transact_rejected_bank_resp'] = table_data.loc[row, 17]
    		# No. of transactions processed by bank
    		item['transact_total_bank_resp'] = table_data.loc[row, 18]
    	
    		# URL
    		item['url'] = self.basic
    		# Spider
    		item['spider'] = self.name
    		# Server
    		item['server'] = socket.gethostname()
    		# Date
    		item['date'] = str(datetime.datetime.now()) 
    		
    		# Yield item to processing pipe-line
    		yield(item)
    
    
    # Get FTO content
    def get_fto_data(self, response):
    	
    	# Store FTO pattern
    	fto_pattern = re.compile('(\d{2})(\d{2})(\d{3})_(\d{6})\D{0,4}FTO')
    	# Get text from response
    	fto_nos = response.xpath('*//a/text()').extract()
    	# Store all FTO nos
    	fto_nos = [fto_no for fto_no in fto_nos if 'FTO' in fto_no]
    	# Initialize data-frame where data will be stored
    	fto_scraped = pd.DataFrame([])
    	
    	# If there are FTO nos. on the page:
    	if len(fto_nos) > 0:
    		
    		# Create a data-frame of FTO info.
    		fto_info = pd.DataFrame([chain.from_iterable(re.findall(fto_pattern, fto_no)) for fto_no in fto_nos])
    		# Store only FTO nos
    		fto_nos = pd.DataFrame(fto_nos)
    		# Label first column
    		fto_nos.columns = ['0']
    		# Label information columns
    		fto_info.columns = ['1', '2', '3', '4'] 
    		# Join them
    		fto_scraped = pd.concat([fto_nos, fto_info], axis = 1)
    	
    	# Return statement 	
    	return(fto_scraped)   	
    
    	
    # Get FTO numbers		    		
    def parse_fto_list(self, response):
    	
    	# Create FTO number item
    	item = FTONo()
    	
    	# Store scraped data
    	fto_scraped = self.get_fto_data(response)
    	# Store FTO stage
    	fto_stage = response.xpath('//*[@id="ctl00_ContentPlaceHolder1_lblHeader"]//text()').extract_first().strip()
    	
    	# If there's at least one FTO:
    	if len(fto_scraped) > 0:
    		
    		# Go through each row of the FTO list
    		for row in fto_scraped.index:	
    			
    			# FTO no
    			item['fto_no'] = fto_scraped.loc[row, '0']
    			# State code
    			item['state_code'] = fto_scraped.loc[row, '1']
    			# District code
    			item['district_code'] = fto_scraped.loc[row, '2']
    			# Block code
    			item['block_code'] = fto_scraped.loc[row, '3']
    			
    			# Process date
    			item['process_date'] = parse(fto_scraped.loc[row, '4'])
    			# URL for item
    			item['url'] = self.basic
    			# Spider name
    			item['spider'] = self.name
    			# Server
    			item['server'] = socket.gethostname()
    			
    			# Date
    			item['date'] = str(datetime.datetime.now())
    			# FTO stage
    			item['fto_stage'] = fto_stage
    			
    			# Log
    			self.logger.info(re.findall('FTO_(.*)', item['fto_no'])[0])
    			
    			# Yield item
    			yield(item)
