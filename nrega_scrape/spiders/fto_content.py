# -*- coding: utf-8 -*-

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
from common.helpers import *


# FTO Scraping class
class FtoContentSpider(scrapy.Spider):
    
    # Set globals
    name = "fto_content"
    # Basic URL
    basic = "http://mnregaweb4.nic.in/netnrega/fto/fto_status_dtl.aspx?"
    # Financial year
    fin_year = "2018-2019"
    # State code
    state_code = "33"
    # Start time
    start_time = time.time()
 	# Output directory
    output_dir = os.path.abspath(".")
    # Path to Chrome
    path_to_chrome_driver = os.path.abspath("./../software/chromedriver/")
    # path_to_chrome_driver = os.path.abspath("/home/ec2-user/chromedriver/")
    # List of URLs	
    start_urls = []
    # Create a connection to the data-base
    conn, cursor = db_conn()
    # FTO nos.
    fto_nos = pd.read_sql("SELECT fto_no FROM fto_numbers LIMIT 1000;", con = conn).values.tolist()
    # Get target FTO list
    fto_nos = [fto_no[0] for fto_no in fto_nos]

    # Iterate through each FTO number
    for fto_no in fto_nos:
		
		# Construct URL for each FTO no
    	url = basic + "fto_no=" + fto_no + "&fin_year=" + fin_year + "&state_code=" + state_code
    	# Append each constructed URL to the list
    	start_urls.append(url)
	
	# Close connections
    conn.close()
    cursor.close()
    	
    # Create options object for Chrome driver
    options = Options()
    # Add the headless option to the options object
    options.add_argument('--headless')
    # Create a browser object
    driver = webdriver.Chrome(path_to_chrome_driver, chrome_options = options)
    			
    # Get selector object for file
    def get_source(self, response, driver):

    	# Navigate to the target URL
    	driver.get(response.url)
    	# Sleep for 4 seconds to wait for response URL
    	time.sleep(4)
    	
    	# Create drop down object 
    	fto_drop_down = Select(driver.find_element_by_css_selector("#ctl00_ContentPlaceHolder1_Ddfto"))
    	# Sleep for 3 seconds
    	time.sleep(3)
    	
    	# Select the FTO number from the drop down box
    	fto_drop_down.select_by_index(1)
    	# Store it as a scrapy selector object
    	time.sleep(4)
    	
    	# Get the page source text
    	page_source = driver.page_source
    	# Store it as a selector object
    	page_source = Selector(text = page_source)
    	
    	# Return page source
    	return(page_source)
    	
    # Get the response and parse it 	
    def parse(self, response):
    	 
    	# Create FTO item
    	item = FTOItem()
    	# Store source code
    	source = self.get_source(response, self.driver)
    	
    	# Store all tables on page
    	tables = source.xpath('//table')
    	# Get the last table on page from the tables list - this is the table we need
    	table = tables[4]
    	
    	# Store the rows
    	rows = table.xpath('*//tr')
    	
    	# Iterate through each row
    	for row in rows:
    		
    		# Block name
    		item['block_name'] = row.xpath('td[1]//text()').extract_first() 
    		# Job card number
    		item['jcn'] = row.xpath('td[2]//text()').extract_first()
    		# Transaction reference no.
    		item['transact_ref_no'] = row.xpath('td[3]//text()').extract_first()
    		# Transaction date
    		item['transact_date'] = row.xpath('td[4]//text()').extract_first()
    		
    		# Application name
    		item['app_name'] = row.xpath('td[5]//text()').extract_first()
    		# Primary account holder name
    		item['prmry_acc_holder_name'] = row.xpath('td[6]//text()').extract_first()
    		# Wage list no.
    		item['wage_list_no'] = row.xpath('td[7]//text()').extract_first()
    		# Account no.
    		item['acc_no'] = row.xpath('td[8]//text()').extract_first()
    		
    		# Bank code
    		item['bank_code'] = row.xpath('td[9]//text()').extract_first()
    		# IFSC code
    		item['ifsc_code'] = row.xpath('td[10]//text()').extract_first()
    		# Credit amount due
    		item['credit_amt_due'] = row.xpath('td[11]//text()').extract_first()
    		# Credit amount actual
    		item['credit_amt_actual'] = row.xpath('td[12]//text()').extract_first()
    		
    		# Status
    		item['status'] = row.xpath('td[13]//text()').extract_first()
    		# Processed date of FTO
    		item['processed_date'] = row.xpath('td[14]//text()').extract_first()
    		# Universal transaction reference
    		item['utr_no'] = row.xpath('td[15]//text()').extract_first()
    		# Rejection reason
    		item['rejection_reason'] = row.xpath('td[16]//text()').extract_first()
    		
    		# Server name
    		item['server'] = socket.gethostname()
    		# FTO no.
    		item['fto_no'] = re.findall('fto_no=(.*FTO_\d+)&fin_year', response.url)[0]
    		# Scrape date
    		item['scrape_date'] = str(datetime.datetime.now())
    		# Time taken
    		item['time_taken'] = time.time() - self.start_time
    		# URL 
    		item['url'] = response.url
    		
    		# Yield the item to processing pipeline
    		yield(item)
