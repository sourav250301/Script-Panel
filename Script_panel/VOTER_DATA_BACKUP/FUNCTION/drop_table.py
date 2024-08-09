import pyodbc

def drop_table_if_exists(server, database, table_name, user, password):
    try:
        # Establish connection to SQL Server
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Drop table if it exists
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Table {table_name} dropped successfully.")
    except Exception as e:
        print(f"Error occurred while dropping table {table_name}: {e}")


if __name__ == "__main__":
    # Example parameters (replace with actual values)
    server = '192.168.0.27'
    user = 'sa'
    password = 'NXT@LKJHGFDSA'
    database = 'voter'
    table_name='ALL_STATE_TEST_DATA_BC'

    # Drop the table if it exists
    drop_table_if_exists(server, database, table_name, user, password)