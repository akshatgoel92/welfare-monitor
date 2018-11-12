#!/bin/sh

# Execute the ETL
cd /home/ec2-user/fba-bank-scrape/
scrapy crawl fto_content
python ./common/update_ftos.py
python ./common/process_log.py
