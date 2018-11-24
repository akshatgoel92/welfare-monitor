#!/bin/sh

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fba-bank-scrape/

# Then execute the content spider
scrapy crawl fto_content

# Then update the queue in the SQL database 
# Then update the log
python ./common/update_ftos.py
python ./common/process_log.py
