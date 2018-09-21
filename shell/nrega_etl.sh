#!/bin/sh

cd /home/ec2-user/gma-scrape/

scrapy crawl fto_stats

scrapy crawl fto_content 