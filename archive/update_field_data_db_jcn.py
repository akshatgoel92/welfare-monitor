import pandas as pd
from sqlalchemy import *
import helpers 
import errors as er
import numpy as np

engine = helpers.db_engine()
conn = engine.connect()
print('opened engine')
get_field_data = "SELECT * from field_data;"


df_field_data = pd.read_sql(get_field_data, con = conn)
print('got field')
print(df_field_data.head())
df_field_data['jcn'] = df_field_data['jcn'].str.replace('-014-','-015-')
print('replaced')
print(df_field_data)



df_field_data.to_sql('field_data', con=conn, if_exists='replace')

df_field.to_sql('field_data', con = conn, index = False, if_exists = 'replace',
		dtype = {'id': Integer(), 'respondent_name': VARCHAR(50), 'sky_phone': VARCHAR(50),
		'jcn': VARCHAR(50), 'jcn_extra': VARCHAR(50)})

conn.execute('ALTER TABLE field_data ADD PRIMARY KEY (id);')


print('to sql')
conn.close()