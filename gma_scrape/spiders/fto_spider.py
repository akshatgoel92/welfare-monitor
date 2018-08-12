# -*- coding: utf-8 -*-

# Import packages
import scrapy
import datetime
import socket
import pandas as pd
import numpy as np

# Import sub-modules
from scrapy import Spider
from scrapy.contrib.spiders import CrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request
from nrega.items import NREGAItem

class FtoSpider(CrawlSpider):
    '''Input: MNREGA FTO URL 
      Output: Scraped FTO summary statistics
    '''
    
    #urls = table.xpath('*//a//@href').extract()
    
    # Spider name
    name = 'fto_spider'
    
    # Target URL         
    basic = 'http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?page=d&mode=B&flg=W&' 
    
    inputs = 'state_name=CHHATTISGARH&state_code=33&district_name=RAIPUR&district_code=3316&fin_year=2018-2019'
    
    meta = '&dstyp=B&source=national&Digest=tuhEXy+HR52YT8lJYijdtw'
    
    start_urls = [basic + inputs + meta]
    		        					
    def parse(self, response):
    	
    	# Create an item object	
    	item = NREGAItem()
    
    	# Get data and store it in a table
    	tables = response.xpath('//table')
    	
    	table = tables[4] 
    	
    	table_data = [table.xpath('*/td[' + str(col) + ']//text()').extract() for col in range(21)]
    	
    	table_data = [MapCompose(lambda s: s.replace("\r\n", ''), str.strip)(col) for col in table_data]
    	
    	table_data = [list(filter(None, col))[-6:] for col in table_data]
    	
    	table_data = [table_col for table_col in table_data if table_col != []]
    	
    	table_data = pd.DataFrame(table_data).T
    	
    	table_data = table_data.iloc[1:-1,1:-1]
    	
    	# Populate item for each row
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
    	
    		# Housekeeping fields
    		item['url'] = self.basic
    		
    		item['spider'] = self.name
    
    		item['server'] = socket.gethostname()
    
    		item['date'] = str(datetime.datetime.now()) 
    	
    		yield(item)
    		
    		
