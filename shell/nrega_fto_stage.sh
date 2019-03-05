#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Then execute the stage spider
scrapy crawl fto_urls -a stage=fst_sig

# First sign pending 
scrapy crawl fto_urls -a stage=fst_sig_not 

# Second sign done  
scrapy crawl fto_urls -a stage=sec_sig 

# Second sign pending 
scrapy crawl fto_urls -a stage=sec_sig_not 

# Sent to bank 
scrapy crawl fto_urls -a stage=sb 

# Partially processed by bank 
scrapy crawl fto_urls -a stage=pp 

# Pending for response by bank/PFMS 
scrapy crawl fto_urls -a stage=P 

# Processed by bank
scrapy crawl fto_urls -a stage=pb