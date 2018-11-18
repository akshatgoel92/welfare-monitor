# Import packages
# MySQL driver 
# Install this as MySQLdb to ensure backward compatibility
import os
import json
import pymysql
import pandas as pd
import numpy as np
pymysql.install_as_MySQLdb()

# Make SQL connections using this module
from common.helpers import sql_connect
from sqlalchemy import *

# Run a SELECT statement
def get_data(table, block):

    # Need to finish this SQL query
    query = "SELECT * from " + table + WHERE fto_no IN " + block + " AND;"
    user, password, host, db = sql_connect().values()
    conn = pymysql.connect(host, user, password, db, charset="utf8", use_unicode=True)
    data = pd.read_sql("SELECT * from " + table + WHERE fto_no IN " + block +;", con = conn)
    conn.close()
    return(data)


if __name__ == '__main__':
            
    transactions = get_data('transactions')
    accounts = get_data('accounts')
    banks = get_data('banks')
    wage_lists = get_data('wage_lists')



