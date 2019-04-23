#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Then execute the content spider
scrapy crawl fto_content

# Then execute the branch spider 
# scrapy crawl fto_branch  

# Then update the queue in the SQL database 
python ./queue/update.py

# Then update the log
python ./backend/logs/process.py './output/log.csv' 'logs/log'

python ./queue/download.py 1 1 './output/transactions.csv' 'tests/transactions'
