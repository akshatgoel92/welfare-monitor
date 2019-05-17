#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Make sure that project directory is in Python path for imports
export PYTHONPATH=.

# Then execute the content spider
scrapy crawl fto_content

# Then execute the branch spider 
# scrapy crawl fto_branch  

# Then update the queue in the SQL database 
python ./queue/update.py

# Then update and upload the log
python ./backend/logs/process.py './backend/logs/log.csv' 'logs/log'

# Then download the data
python ./queue/download.py 0 1 90 './output/transactions.csv' 'tests/transactions'
