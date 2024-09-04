from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine

driver_path = r"D:\DOWNLOAD\ojdbc11.jar"
server = '192.168.0.27'
user = 'sa'
password = 'NXT@LKJHGFDSA'
database = 'voter'
port = '1433'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
url = f"jdbc:sqlserver://{server}:1433;databaseName={database};user={user};password={password}"
final_table = "ALL_STATE_TEST_DATA_BC"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to SQL Server") \
    .config("spark.driver.extraClassPath", driver_path) \
    .getOrCreate()

# Path to the CSV file
csv_file_path = "D:\\DOWNLOAD\\india data\\REPLACING_NEW_ROUND_WISE_DATA\\BC_Round\\BC_Round_final_data.csv"

# Read the CSV file into a DataFrame
print("Reading CSV file into DataFrame...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_file_path)

# Print the schema of the DataFrame to check the structure
print("Schema of the DataFrame:")
df.printSchema()
pdf = df.toPandas()
# Create SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver={driver}')

# Write Pandas DataFrame to SQL Server
pdf.to_sql(final_table, engine, if_exists='replace', index=False)


# # Write the DataFrame to SQL Server
# print("Writing DataFrame to SQL Server...")
# df.write \
#     .format("jdbc") \
#     .option("url", url) \
#     .option("dbtable", final_table) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("driver", driver) \
#     .mode("overwrite") \
#     .save()

print(df.count())
print("DataFrame successfully written to SQL Server.")

# Stop Spark session
spark.stop()
print("Spark session stopped.")
