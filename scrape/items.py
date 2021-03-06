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
    scrape_date = scrapy.Field()
    
    scrape_time = scrapy.Field()
    
    
class FTONo(scrapy.Item):
	
	# These are scraped FTO numbers
	fto_no = scrapy.Field()
	
	state_code = scrapy.Field()
	
	district_code = scrapy.Field()
	
	block_code = scrapy.Field()
	
	transact_date = scrapy.Field()
	
	# Housekeeping fields
	url = scrapy.Field()


class FTOItem(scrapy.Item):
    
    # Data fields
    block_name = scrapy.Field()
    
    jcn = scrapy.Field()
    
    transact_ref_no = scrapy.Field()
    
    transact_date = scrapy.Field()
    
    app_name = scrapy.Field()
    
    prmry_acc_holder_name = scrapy.Field()
    
    wage_list_no = scrapy.Field()
    
    acc_no = scrapy.Field()
    
    bank_code = scrapy.Field()
    
    ifsc_code = scrapy.Field()
    
    credit_amt_due = scrapy.Field()
    
    credit_amt_actual = scrapy.Field()
    
    status = scrapy.Field()
    
    processed_date = scrapy.Field()
    
    utr_no = scrapy.Field()
    
    rejection_reason = scrapy.Field()
    
    server = scrapy.Field()
    
    fto_no = scrapy.Field()
    
    scrape_date = scrapy.Field()
    
    scrape_time = scrapy.Field()



class FTOOverviewItem(scrapy.Item):
    
    state = scrapy.Field()
    
    district = scrapy.Field()
    
    block_name = scrapy.Field()

    fto_no = scrapy.Field()

    fto_type = scrapy.Field()
    
    pay_mode = scrapy.Field()
    
    acc_signed_dt = scrapy.Field()

    po_signed_dt = scrapy.Field()

    acc_signed_dt_p2w = scrapy.Field()
    
    po_signed_dt_p2w = scrapy.Field()

    cr_processed_dt = scrapy.Field()
    
    cr_processed_dt_P = scrapy.Field()



class FTOMaterialItem(scrapy.Item):

    sr_no = scrapy.Field()
    
    block_name = scrapy.Field()
    
    transact_ref_no = scrapy.Field()

    transact_date = scrapy.Field()
    
    vendor_name = scrapy.Field()
    
    vendor_id = scrapy.Field()
                
    voucher_no = scrapy.Field()

    prmry_acc_holder_name = scrapy.Field()
    
    bank_code = scrapy.Field()

    ifsc_code = scrapy.Field()
    
    credit_amt_due = scrapy.Field()
    
    credit_amt_actual = scrapy.Field()
            
    status = scrapy.Field()
    
    processed_date = scrapy.Field()
                
    utr_no = scrapy.Field()
            
    rejection_reason = scrapy.Field()
                
    server = scrapy.Field()
                
    fto_no = scrapy.Field()
            
    scrape_date = scrapy.Field()
    
    scrape_time = scrapy.Field()
    


