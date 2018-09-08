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
    
    # Spider name
    name = 'fto_stats'
    
    # Target URL         
    basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&'
    
    state_name = 'CHHATTISGARH'
    
    state_code = '33'
    
    district_name = 'RAIPUR'
    
    district_code = '3316'
    
    fin_year = '2018-2019'
    
    state = 'state_name=' + state_name +'&state_code=' + state_code
    
    district = '&district_name=' + district_name + '&district_code=' + district_code 
    
    fin_year_url = '&fin_year=' + fin_year
    
    meta = '&dstyp=B&source=national&Digest=tuhEXy+HR52YT8lJYijdtw'
    
    start_urls = [basic + state + district +fin_year_url + meta]
    
    # Parse the response	        					
    def parse(self, response):
    
    	table = response.xpath('//table')[-1]
    	
    	urls = table.xpath('*//a//@href').extract()
    	
    	for row in self.parse_fto_summary(table):
    		
    		yield(row)
    	
    	for url in urls:
 		
    		yield(response.follow(url, self.parse_fto_list))	
    	
    # Parse the FTO summary table
    def parse_fto_summary(self, table):
    	
    	table_data = [table.xpath('*/td[' + str(col) + ']//text()').extract() for col in range(21)]
    	
    	table_data = [MapCompose(lambda s: s.replace("\r\n", ''), str.strip)(col) for col in table_data]
    	
    	table_data = [list(filter(None, col))[-6:] for col in table_data]
    	
    	table_data = [table_col for table_col in table_data if table_col != []]
    	
    	table_data = pd.DataFrame(table_data).T
    	
    	table_data = table_data.iloc[1:-1,1:-1]
    		
    	item = NREGAItem()
        	
    	for row in table_data.index:
    		
    		item['block_name'] = table_data.loc[row ,1]
    
    		item['total_fto'] = table_data.loc[row, 2]
         
    		item['first_sign'] = table_data.loc[row , 3]
    
    		item['first_sign_pending'] = table_data.loc[row , 4]
    
    		item['second_sign'] = table_data.loc[row , 5]
    
    		item['second_sign_pending'] = table_data.loc[row , 6]
    
    		item['fto_sent_bank'] = table_data.loc[row , 7]
    
    		item['transact_sent_bank'] = table_data.loc[row, 8]
    
    		item['fto_processed_bank'] = table_data.loc[row, 9]
    
    		item['transact_processed_bank'] = table_data.loc[row, 10]
    
    		item['fto_partial_bank'] = table_data.loc[row, 11]
    
    		item['transact_partial_bank'] = table_data.loc[row, 12]
    
    		item['fto_pending_bank'] = table_data.loc[row, 13]
    
    		item['transact_pending_bank'] = table_data.loc[row, 14]
    
    		item['transact_processed_bank_resp'] = table_data.loc[row, 15]
    
    		item['invalid_accounts_bank_resp'] = table_data.loc[row, 16]
    
    		item['transact_rejected_bank_resp'] = table_data.loc[row, 17]
    
    		item['transact_total_bank_resp'] = table_data.loc[row, 18]
    	
    		item['url'] = self.basic
    		
    		item['spider'] = self.name
    
    		item['server'] = socket.gethostname()
    
    		item['date'] = str(datetime.datetime.now()) 
    	
    		yield(item)
    
    # Get FTO content
    def get_fto_data(self, response):
    	
    	fto_pattern = re.compile('(\d{2})(\d{2})(\d{3})_(\d{6})\D{0,4}FTO')
    	    	
    	fto_nos = response.xpath('*//a/text()').extract()
    	
    	fto_nos = [fto_no for fto_no in fto_nos if 'FTO' in fto_no]
    	
    	fto_scraped = pd.DataFrame([ ])
    	
    	if len(fto_nos) > 0:
    	
    		fto_info = pd.DataFrame([chain.from_iterable(re.findall(fto_pattern, fto_no)) for fto_no in fto_nos])
    		
    		fto_nos = pd.DataFrame(fto_nos)
    		
    		fto_nos.columns = ['0']
    	
    		fto_info.columns = ['1', '2', '3', '4'] 
    		
    		fto_scraped = pd.concat([fto_nos, fto_info], axis = 1)
    		
    	return(fto_scraped)   	
    	
    # Get FTO numbers		    		
    def parse_fto_list(self, response):
    	
    	item = FTONo()
    	
    	fto_scraped = self.get_fto_data(response)
    	
    	fto_stage = response.xpath('//*[@id="ctl00_ContentPlaceHolder1_lblHeader"]//text()').extract_first().strip()
    	
    	if len(fto_scraped) > 0:
    	
    		for row in fto_scraped.index:	
    		
    			item['fto_no'] = fto_scraped.loc[row, '0']
    		
    			item['state_code'] = fto_scraped.loc[row, '1']
    		
    			item['district_code'] = fto_scraped.loc[row, '2']
    		
    			item['block_code'] = fto_scraped.loc[row, '3']
    		
    			item['process_date'] = parse(fto_scraped.loc[row, '4'])
    		
    			item['url'] = self.basic
    		
    			item['spider'] = self.name
    
    			item['server'] = socket.gethostname()
    
    			item['date'] = str(datetime.datetime.now())
    			
    			item['fto_stage'] = fto_stage
    		
    			yield(item)
