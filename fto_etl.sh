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

# Then update the queue in the SQL database 
python ./tracker/update.py

# Then update and upload the log
python ./scrape/logs/process.py './logs/log.csv' 'logs/log' 0

# Then download the data
python ./tracker/download.py 0 1 30 './output/transactions.csv' 'nrega/transactions'
