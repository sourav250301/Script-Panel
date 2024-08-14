import pyodbc
import pandas as pd
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Queries import queries_BC
from Queries import queries_EP
from Queries import queries_PP
pd.set_option('display.max_columns', None)

def fetch_specific_queries_from_module(module, query_names):
    queries = []
    for name in query_names:
        if hasattr(module, name):
            query_str = getattr(module, name)
            if isinstance(query_str, str):
                queries.append(query_str.strip())
    return queries

def execute_queries_and_save_to_dataframe(server, database, username, password):
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
    results = []
    total_rows = 0
    batch_size = 100000
    try:
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            
            # Specify the queries you want to import from each module
            queries_pp_names = ['pp_mh', 'pp_hr','pp_tn','pp_up']
            queries_ep_names = ['ep_mh', 'ep_hr','ep_tn','ep_up']
            queries_bc_names = ['bc_mh', 'bc_hr','bc_tn','bc_up']

            
            # Fetch the specific queries
            queries_pp = fetch_specific_queries_from_module(queries_PP, queries_pp_names)
            queries_ep = fetch_specific_queries_from_module(queries_EP, queries_ep_names)
            queries_bc = fetch_specific_queries_from_module(queries_BC, queries_bc_names)
            
            # Combine all queries
            queries = queries_pp + queries_ep + queries_bc

            for query in queries:
                cursor.execute(query)
                data = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                df = pd.DataFrame.from_records(data, columns=columns)
                results.append(df)
                
                total_rows += len(df)
                if total_rows >= batch_size:
                    print(f"{total_rows} no of rows proccessed.")
                    batch_size += 100000 
                
            # Additional SQL query directly executed against database
            sql_query = 'SELECT * FROM PY_TEST_INDIA_DATA'
            cursor.execute(sql_query)
            data = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df_sql = pd.DataFrame.from_records(data, columns=columns)
            df_sql.rename(columns={'PC_REGION': 'REGION'}, inplace=True)
            df_sql['AC_ID'] = df_sql['State_Code'] + '-AC-' + df_sql['AC_NO'].astype(str)
            df2_reduced = df_sql[['AC_ID', 'PC_ID', 'REGION']]

        if results:
            combined_df = pd.concat(results, ignore_index=True)
            final_df = pd.merge(combined_df, df2_reduced, how='left', on=['AC_ID', 'PC_ID'])            
            return final_df
        else:
            return None
    except pyodbc.Error as e:
        print(f"Database error occurred: {e}")
        return None

server = '192.168.0.27'
database = 'voter'
username = 'sa'
password = 'NXT@LKJHGFDSA'

dataframe = execute_queries_and_save_to_dataframe(server, database, username, password)
