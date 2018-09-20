# -*- coding: utf-8 -*-

#------------------------------------------------------------------#
'''
Author: Akshat Goel
Date: 8th August 2018
Python version: 3.6.3
Dependencies: 

[Only modules outside Python standard listed]
1) scrapy 
2) pandas 
3) numpy 
4) Selenium 
'''
#------------------------------------------------------------------#

# Scraping and cleaning modules
import scrapy
import datetime
import time
import socket
import re
import os
import pandas as pd
import numpy as np

# Scrapy sub-modules
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request

# Selenium sub-modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from datetime import date, timedelta

# Item class
from nrega_scrape.items import FTOItem

# Helpers
from common.helpers import *

# FTO Scraping class
class FtoContentSpider(scrapy.Spider):
    
    # Set globals
    name = "fto_content"
    
    basic = "http://mnregaweb4.nic.in/netnrega/fto/fto_status_dtl.aspx?"
    
    fin_year = "2018-2019"
    
    state_code = "33"
    
    start_time = time.time()
 	   
    output_dir = os.path.abspath(".")
    
    # path_to_chrome_driver = os.path.abspath("./../software/chromedriver")
    
    path_to_chrome_driver = os.path.abspath("/home/ec2-user/chromedriver/")
    	
    start_urls = []

    # Create a connection to the data-base
    conn, cursor = db_conn()
    
    # FTO nos.
    fto_nos_db = pd.read_sql("SELECT fto_no FROM fto_numbers LIMIT 10;", con = conn).values.tolist()
    
    # Get target FTO list
    fto_nos =  [fto_no[0] for fto_no in fto_nos_db]

    # Iterate through each FTO number and construct URL
    for fto_no in fto_nos:
	
    	url = basic + "fto_no=" + fto_no + "&fin_year=" + fin_year + "&state_code=" + state_code
    	
    	start_urls.append(url)
    			
    # Get selector object for file
    def get_source(self, response):
    	
    	options = Options()
    	
    	options.add_argument('--headless')
    	
    	driver = webdriver.Chrome(self.path_to_chrome_driver, chrome_options = options)
    	
    	driver.get(response.url)
    	
    	time.sleep(4)
    	 
    	fto_drop_down = Select(driver.find_element_by_css_selector("#ctl00_ContentPlaceHolder1_Ddfto"))
    	
    	time.sleep(3)
    	
    	fto_drop_down.select_by_index(1)
    	
    	time.sleep(4)
    	
    	page_source = driver.page_source
    	
    	page_source = Selector(text = page_source)
    	
    	return(page_source)
    	
    # Get the response and parse it 	
    def parse(self, response):
    	 
    	item = FTOItem()
    	
    	source = self.get_source(response)
    	
    	tables = source.xpath('//table')
    	
    	table = tables[4]
    	
    	rows = table.xpath('*//tr')
    	
    	for row in rows:
    		
    		item['block_name'] = row.xpath('td[1]//text()').extract_first() 
    
    		item['jcn'] = row.xpath('td[2]//text()').extract_first()
    
    		item['transact_ref_no'] = row.xpath('td[3]//text()').extract_first()
    
    		item['transact_date'] = row.xpath('td[4]//text()').extract_first()
    
    		item['app_name'] = row.xpath('td[5]//text()').extract_first()
    
    		item['prmry_acc_holder_name'] = row.xpath('td[6]//text()').extract_first()
    
    		item['wage_list_no'] = row.xpath('td[7]//text()').extract_first()
    
    		item['acc_no'] = row.xpath('td[8]//text()').extract_first()
    
    		item['bank_code'] = row.xpath('td[9]//text()').extract_first()
    
    		item['ifsc_code'] = row.xpath('td[10]//text()').extract_first()
    
    		item['credit_amt_due'] = row.xpath('td[11]//text()').extract_first()
    
    		item['credit_amt_actual'] = row.xpath('td[12]//text()').extract_first()
    
    		item['status'] = row.xpath('td[13]//text()').extract_first()
    
    		item['processed_date'] = row.xpath('td[14]//text()').extract_first()
    
    		item['utr_no'] = row.xpath('td[15]//text()').extract_first()
    
    		item['rejection_reason'] = row.xpath('td[16]//text()').extract_first()
    		
    		item['server'] = socket.gethostname()
    		
    		item['fto_no'] = re.findall('fto_no=(.*FTO_\d+)&fin_year', response.url)[0]
    		
    		item['scrape_date'] = str(datetime.datetime.now())
    		
    		item['time_taken'] = time.time() - self.start_time
    		
    		item['url'] = response.url
    			
    		yield(item)