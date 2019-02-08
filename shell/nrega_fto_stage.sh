#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Execute the ETL
# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Then execute the stage spider 
python ./nrega_scrape/spiders/fto_stage.py 