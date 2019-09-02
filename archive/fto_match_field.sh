#!/bin/sh

# Make sure we're using the correct Python
export PATH=/home/ec2-user/anaconda3/bin:$PATH

# Make sure that the Python path contains the current folder
export PYTHONPATH=.

# Switch to the working directory
cd /home/ec2-user/fto-scrape/

# Clear existing .csv files
# Do I want to do this?
#rm output/*.csv
#./script/dummy_calls_btt_pilot3.csv

python ./script/scrape_info_to_btt_csv.py