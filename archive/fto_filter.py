# Import packages
import scrapy
import datetime
import time
import socket
import re
import os
import pandas as pd
import numpy as np

# Scrapy sub-modules
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.http import Request

# Selenium sub-modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

# Date and time sub-modules 
from datetime import date, timedelta

# Twisted errors
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

# Item class
from nrega_scrape.items import FTOItem
from common.helpers import *

# Get list of FTOs from table
def update_fto_queue():

    # Create data-base connection
    conn, cursor = db_conn()

    # Read the table
    fto_nos = pd.read_sql_table("SELECT fto_no FROM fto_nos LIMIT 1000;", con = conn)

    # Get the stages table
    fto_stages = pd.read_sql_table("SELECT * from fto_stages", con = conn)

    # Join these two data-frames
    fto_nos = pd.merge(fto_nos, fto_stages, how= 'left_only', on = 'fto_no', con = conn)
    
    # Group by FTO no. and pick the one with the largest stage 
    fto_scraped = fto_nos['fto_no'].groupby(fto_no).max(fto_nos.stage)

    # Get FTO queue 
    fto_queue_table = pd.read_sql_table("SELECT * from fto_queue", con = conn)

    # Join with FTO scraped 
    fto_new = pd.merge(fto_scraped, fto_queue_table, how = 'left_only', on = 'fto_no')

    # Insert that into FTO queue if doesn't already exist 
    pass

    # End function
    return()

def get_fto_queue():

    # Read the table
    return(pd.read_sql_table("SELECT fto_no FROM fto_queue WHERE scrape_status = 0;"))
    

