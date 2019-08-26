import pandas as pd
import numpy as np
import random
import sys

from common import errors as er
from datetime import datetime
from common import helpers
from script import utils
from sqlalchemy import *


def check_welcome_script(df, welcome_code = "P0 P1 P2 00", welcome_code_alt = "P0 P1 P2 00 P0"):
	
	query = "SELECT id FROM scripts where day1 = '{}' OR day1 = '{}';"
	engine = helpers.db_engine()
	
	try: welcome_script = pd.read_sql(query.format(welcome_code, welcome_code_alt), con = engine)
	
	except Exception as e: 
		print(e)
		sys.exit()
	
	df = pd.merge(df, welcome_script, how = 'left', on = 'id', indicator = 'got_welcome')
	

		
	return(df)


def get_welcome_script_indicator(df):
	
	df['got_welcome_int'] = 0
	df.loc[(df['got_welcome'] == 'both'), 'got_welcome_int'] = 1
	
	df.drop(['got_welcome'], inplace = True, axis = 1)
	df = df.rename(columns={'got_welcome_int': 'got_welcome'})
	
	return(df)
	
	
def check_static_nrega_script(df, static_script_code = "P0 P1 P2 P3 Q A P0 Z1 Z2"):
	
	engine = helpers.db_engine()
	
	try: static_nrega = pd.read_sql("SELECT id FROM scripts WHERE day1 = '{}'".format(static_script_code), con = engine)
	
	except Exception as e:
		print(e)
		sys.exit()
	
	df = pd.merge(df, static_nrega, how = 'left', on = 'id', indicator = 'got_static_nrega')
			
	return(df)


def get_static_nrega_script_indicator(df):
	
	# Format columns
	df['got_static_nrega_int'] = 0
	df.loc[(df['got_static_nrega'] == 'both'), 'got_static_nrega_int'] = 1
	
	# Rename column
	df.drop(['got_static_nrega'], inplace = True, axis = 1)
	df = df.rename(columns = {'got_static_nrega_int': 'got_static_nrega'})
	
	return(df)