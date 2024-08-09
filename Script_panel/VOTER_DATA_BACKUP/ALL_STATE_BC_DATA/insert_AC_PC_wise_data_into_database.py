from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import lit, col
import time
import importlib
import sys
import os
from pyspark.sql import Window

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.utils import calculate_elapsed_time, export_csv_file, insert_dataframe_into_database

driver_path = r"D:\DOWNLOAD\ojdbc11.jar"
start_time = time.time()

server = '192.168.0.27'
user = 'sa'
password = 'NXT@LKJHGFDSA'
database = 'voter'
port = '1433'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
url = f"jdbc:sqlserver://{server}:1433;databaseName={database};user={user};password={password}"
table_name = "ALL_STATE_TEST_DATA_BC"

spark = SparkSession.builder \
    .appName("ALL_STATE_Voter_Data_Analysis") \
    .config("spark.driver.extraClassPath", driver_path) \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.executor.memory", "32g") \
    .config("spark.executor.memoryOverhead", "16g") \
    .config("spark.driver.memory", "32g") \
    .config("spark.driver.memoryOverhead", "16g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "32g") \
    .config("spark.local.dir", "C:/spark_temp") \
    .config("spark.cleaner.referenceTracking.blocking", "true") \
    .config("spark.cleaner.referenceTracking.blocking.shuffle", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .getOrCreate()

# Load the tables into DataFrames
df_ac_wise = spark.read.jdbc(url=url, table="CH_TEST_DATA_BC_AC_WISE", properties={"driver": driver})
df_pc_wise = spark.read.jdbc(url=url, table="CH_TEST_DATA_BC_PC_WISE", properties={"driver": driver})

# Get all columns from both DataFrames
columns_ac_wise = set(df_ac_wise.columns)
columns_pc_wise = set(df_pc_wise.columns)

# Identify common columns
common_columns = columns_ac_wise.intersection(columns_pc_wise)

# Select all columns from df_ac_wise and only non-duplicate columns from df_pc_wise
df_joined = df_ac_wise.join(
    df_pc_wise, on=["AC_ID", "PC_ID", "N_PARTY", "RES1"], how="left"
).select(
    [df_ac_wise[col] for col in columns_ac_wise] + [df_pc_wise[col] for col in columns_pc_wise if
                                                    col not in common_columns]
)
desired_order = [
    'AC_ID', 'PC_ID', 'AC_NO', 'PC_NO', 'state', 'N_PARTY', 'RES1',
    'RES_ACWISE', 'AC_WISE_PERCENTAGE', 'MAX_PER_ACWISE', 'WINNER_ACWISE', '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE',
    'MARGIN_ACWISE', 'MARGIN_GROUP_ACWISE', 'SUMMERY_ACWISE',
    'RES_PCWISE', 'PC_WISE_PERCENTAGE', 'MAX_PER_PCWISE', 'WINNER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE',
    'MARGIN_PCWISE', 'MARGIN_GROUP_PCWISE', 'SUMMERY_PCWISE',
    'REGION', 'ROUND'
]

df_reordered = df_joined.select(*desired_order)

print("Reorederd table is....")
df_reordered.show(10)
print(df_joined.count())

res = insert_dataframe_into_database(df_reordered, table_name, url, driver)
print(res)

end_time = time.time()
elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
print(
    f"Total time taken to calculating adn inserting the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")


# ------------------------------------------------------------------------------------------------#


spark.stop()
