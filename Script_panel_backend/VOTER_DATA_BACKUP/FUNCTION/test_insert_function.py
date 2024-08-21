import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# from unknown.test2 import desired_order as dataframe
dataframe = pd.read_csv('desired_order.csv')


server = '192.168.0.27'
database = 'voter'
username = 'sa'
password = 'NXT@LKJHGFDSA'
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={
    server};DATABASE={database};UID={username};PWD={password}"

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()
connection_url = f"mssql+pyodbc:///?odbc_connect={
    urllib.parse.quote_plus(connection_string)}"
engine = create_engine(connection_url)

# -----------------------------------------------------------------------------#



# -----------------------------------------------------------------------------#
table_name = 'ALL_STATE_TEST_DATA_BC'

batch_size = 10000
first_batch = True
total_rows = len(dataframe)
inserted_rows = 0

for start in range(0, total_rows, batch_size):
    end = start + batch_size
    df_batch = dataframe.iloc[start:end]

    if first_batch:
        df_batch.to_sql(table_name, engine, if_exists='replace', index=False)
        first_batch = False
    else:
        df_batch.to_sql(table_name, engine, if_exists='append', index=False)

    inserted_rows += len(df_batch)
    print(f"Inserted batch from row {start} to {end}. Total inserted rows: {inserted_rows}/{total_rows}")

print("Dataframe Inserted in Database Successfully ...... :)")

# -----------------------------------------------------------------------------#
engine.dispose()
cursor.close()
connection.close()




