#!/bin/sh

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Then execute the content spider
scrapy crawl fto_content

# Then update the queue in the SQL database 
# Then update the log
python ./common/update_ftos.py fto_queue
python ./common/process_log.py './nrega_output/log.csv' '/Female Mobile Phones Phase I/Data/Secondary Data/MIS Scrapes/Logs/log'
