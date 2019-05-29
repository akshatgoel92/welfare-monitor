#--------------------------------------------------------#
# Author: Akshat Goel
# Purpose: Add FTO nos. to scraping queue manually
# Contact: akshat.goel@ifmr.ac.in
#--------------------------------------------------------#
from sqlalchemy import *
from sqlalchemy.engine import reflection
from common import helpers
from make import put_fto_nos
import sys
import os
import pandas as pd
import numpy as np
import pymysql
import argparse

pymysql.install_as_MySQLdb()


def main():

	parser = argparse.ArgumentParser(description = 'Append to or replace the queue?')
	parser.add_argument('if_exists',
						 type = str, 
						 help = 'Append or replace?')

	args = parser.parse_args()
	if_exists = args.if_exists
	path = os.path.abspath('./output/fto_queue.csv')
	
	engine = helpers.db_engine()
	put_fto_nos(engine, path, if_exists)

if __name__ == '__main__':

	main()