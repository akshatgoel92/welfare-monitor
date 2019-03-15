#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Then execute the content spider
scrapy crawl fto_content

# Then execute the branch spider 
scrapy crawl fto_branch  

# Then update the queue in the SQL database 
python ./common/update_ftos.py fto_queue

# Then update the log
python ./common/process_log.py './output/log.csv' '/Female Mobile Phones Phase I/Data/Secondary Data/MIS Scrapes/Logs/log'
