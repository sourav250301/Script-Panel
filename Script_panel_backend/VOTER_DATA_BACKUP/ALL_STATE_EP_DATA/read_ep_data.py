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
from FUNCTION.utils import calculate_elapsed_time
from FUNCTION.Function_for_calculation import main_processing_function

driver_path = r"D:\DOWNLOAD\ojdbc11.jar"
start_time = time.time()

server = '192.168.0.27'
user = 'sa'
password = 'NXT@LKJHGFDSA'
database = 'voter'
port = '1433'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
url = f"jdbc:sqlserver://{server}:1433;databaseName={database};user={user};password={password}"
newTableName = f"ALL_STATE_TEST_DATA_EP"

spark = SparkSession.builder \
    .appName("ALL_STATE_Voter_Data_Analysis") \
    .config("spark.sql.broadcastTimeout", "3600") \
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

statesEP = ['ep_ap', 'ep_asm', 'ep_br', 'ep_cg', 'ep_ch', 'ep_dl', 'ep_ga', 'ep_gj', 'ep_hp', 'ep_tn', 'ep_hr', 'ep_jh', 'ep_ka', 'ep_kl', 'ep_mh', 'ep_mp', 'ep_od', 'ep_pb', 'ep_rj', 'ep_tg', 'ep_uk', 'ep_up', 'ep_wb']


# statesBC = ['bc_ap', 'bc_asm', 'bc_br', 'bc_cg', 'bc_ch', 'bc_dl', 'bc_ga', 'bc_gj', 'bc_hp', 'bc_tn', 'bc_hr', 'bc_jh',
#             'bc_ka', 'bc_kl', 'bc_mh', 'bc_mp', 'bc_od', 'bc_pb', 'bc_rj', 'bc_tg', 'bc_uk', 'bc_up', 'bc_wb']


# statesBC = ['bc_ch']


def import_queries(module_name, state_list):
    module = importlib.import_module(module_name)
    return {state: getattr(module, state) for state in state_list}


ep_queries = import_queries('Queries.queries_EP', statesEP)

# ------------------------------------------------------------------------------------------------#
print("Executing SQL Queries...................")


def create_df_from_queries(queries):
    queries_union = " UNION ALL ".join([queries[state] for state in queries])
    df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({queries_union}) AS query_result") \
        .option("driver", driver) \
        .load()
    return df


print("Completed SQL Queries...................")
print(" ")
# ------------------------------------------------------------------------------------------------#

ep_df = create_df_from_queries(ep_queries)
# ------------------------------------------------------------------------------------------------#

print("Second query Execute Start ....")
sql_query = 'SELECT State_Code, AC_NO, PC_REGION FROM PY_TEST_INDIA_DATA'
df_sql = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"({sql_query}) AS query_result_sql") \
    .option("driver", driver) \
    .load()

df_sql = df_sql.withColumnRenamed("PC_REGION", "REGION")
df_sql = df_sql.withColumn("AC_ID", F.concat(F.col("State_Code"), F.lit('-AC-'), F.col("AC_NO").cast("string")))
#
final_ep_df = ep_df.join(
    df_sql.select("AC_ID", "REGION"),
    on=["AC_ID"],
    how="left"
)

print("Second query Execute completed ....")
print(" ")
# ------------------------------------------------------------------------------------------------#

# rows_count = final_bc_df.count()
final_merged_df = final_ep_df
# if rows_count > 0:
#     # print("👇👇👇👇👇👇 Merged Data 👇👇👇👇👇👇")
#     # final_bc_df.show(10)
#     # print(rows_count)
#     end_time = time.time()
#     elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
#     print(
#         f"Total time taken to fetched Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")

# ---------------------------------------------------------------------------------------------------------------------#
# ---------------------------------------------------------------------------------------------------------------------#
# ---------------------------------------------------------------------------------------------------------------------#
print("EP Rount data calculation start .....😊😊😊😊")

final_dataframe = main_processing_function(final_merged_df)
final_dataframe = final_dataframe.withColumn('ROUND', lit('EP'))
# final_dataframe.printSchema()
# final_dataframe = final_dataframe.repartition(100, "AC_ID")

# result = insert_dataframe_into_database(final_dataframe, url, newTableName, driver)
# print(result)

# desired_order = [
#     'AC_ID', 'PC_ID', 'AC_NO', 'PC_NO', 'state', 'N_PARTY', 'RES1',
#     'RES_ACWISE', 'AC_WISE_PERCENTAGE', 'MAX_PER_ACWISE', 'WINNER_ACWISE', '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE',
#     'MARGIN_ACWISE', 'MARGIN_GROUP_ACWISE', 'SUMMERY_ACWISE',
#     'RES_PCWISE', 'PC_WISE_PERCENTAGE', 'MAX_PER_PCWISE', 'WINNER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE',
#     'MARGIN_PCWISE', 'MARGIN_GROUP_PCWISE', 'SUMMERY_PCWISE',
#     'REGION', 'ROUND'
# ]

# df_reordered = final_dataframe.select(*desired_order)

# print("Reorederd table is....")

# sorted_df_reordered = df_reordered.orderBy(F.col("AC_ID"), F.col("PC_ID"))
# res = insert_dataframe_into_database(df_reordered, newTableName3, url, driver)
# print(res)

def end_df():
    # Convert the Spark DataFrame to Pandas DataFrame
    # pandas_df = final_dataframe.toPandas()
    return final_dataframe

end_time = time.time()
elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
print(
    f"Total time taken to calculating adn inserting the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")
print("EP Round data Execution completed .....😊😊😊😊")

spark.stop()
