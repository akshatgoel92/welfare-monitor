# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

# Import scrapy
import scrapy

class NREGAItem(scrapy.Item):
    
    # Table fields
    block_name = scrapy.Field()
    
    total_fto = scrapy.Field()
         
    first_sign = scrapy.Field()
    
    first_sign_pending = scrapy.Field() 
    
    second_sign = scrapy.Field() 
    
    second_sign_pending = scrapy.Field() 
    
    fto_sent_bank = scrapy.Field() 
    
    transact_sent_bank = scrapy.Field() 
    
    fto_processed_bank = scrapy.Field()
    
    transact_processed_bank = scrapy.Field()
    
    fto_partial_bank = scrapy.Field()
    
    transact_partial_bank = scrapy.Field()
    
    fto_pending_bank = scrapy.Field()
    
    transact_pending_bank = scrapy.Field()
    
    transact_processed_bank_resp = scrapy.Field()
    
    invalid_accounts_bank_resp = scrapy.Field() 
    
    transact_rejected_bank_resp = scrapy.Field() 
    
    transact_total_bank_resp = scrapy.Field()
    
    # Housekeeping fields
    url = scrapy.Field()
    
    spider = scrapy.Field() 
    
    server = scrapy.Field() 
    
    date = scrapy.Field()
    
    
class FTONo(scrapy.Item):
	
	# These are scraped FTO numbers
	fto_no = scrapy.Field()
	
	fto_stage = scrapy.Field()
	
	state_code = scrapy.Field()
	
	district_code = scrapy.Field()
	
	block_code = scrapy.Field()
	
	process_date = scrapy.Field()
	
	# Housekeeping fields
	url = scrapy.Field()
	
	spider = scrapy.Field()
	
	server = scrapy.Field()
	
	date = scrapy.Field()
	
	
	
