#!/bin/sh

# Execute the ETL
cd /home/ec2-user/gma-scrape/
scrapy crawl fto_stats
scrapy crawl fto_content
python ./backend/logs/process_log.py