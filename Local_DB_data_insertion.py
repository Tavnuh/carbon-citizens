import time
from dotenv import load_dotenv
import os
import glob
import pandas as pd
import csv
import psycopg2 as pg
import codecs

load_dotenv()

start_time = time.time()





# Search for the latest file, define file name and prepared file path
list_of_files = glob.glob('1. Original Data/*.csv') 
origin_file_path = max(list_of_files, key=os.path.getctime)
file_name = origin_file_path.split('\\')[-1]
prepared_file_path = '2. Prepared Data/{}'.format(file_name)

# Read CSV to DF, convert first col to 'yyyy-mm-dd' format
df = pd.read_csv(origin_file_path, parse_dates=[0])


#Check if file already exists in Prepared data - if so, delete
#Write altered CSV to Prepared Data folder with same file name as origin
if os.path.exists(prepared_file_path):
    os.remove(prepared_file_path)
    df.to_csv(prepared_file_path,index=False)
else: df.to_csv(prepared_file_path,index=False)    


# DB connection parameters
hostname = os.environ.get('host'),
database = os.environ.get('database')
user = os.environ.get('user')
password = os.environ.get('password')
sslmode = 'disable'

# Create connection and cursor
conn_string = "host={} dbname={} user={} password={} sslmode={}".format(hostname[0],
                                                                        database,
                                                                        user,
                                                                        password,
                                                                        sslmode)
conn = pg.connect(conn_string)
cur = conn.cursor()

# Define table name in DB
table_name = 'dev_trend_data'

#check initial row count
row_counter = f"""SELECT COUNT(*) FROM {table_name}"""

cur.execute(row_counter)
first_row_count = cur.fetchone()[0]

#upsert from prepared file into DB
with codecs.open(prepared_file_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(f""" INSERT INTO {table_name} 
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)
            ON CONFLICT
            DO NOTHING
            """,
        row)
  
#commit insert
conn.commit()
    
#check new row count
cur.execute(row_counter)
new_row_count = cur.fetchone()[0]

rows_affected = new_row_count - first_row_count

cur.close()
conn.close()

# print elapsed time
print('Copied {} rows from {} to {} in'.format(rows_affected,
                                               file_name,
                                               table_name),round((time.time() - start_time),2), 
      'seconds after checking for duplicates')

