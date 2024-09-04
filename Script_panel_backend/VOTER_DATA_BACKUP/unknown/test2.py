
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import lit,col
import time
import importlib
import sys
import os
from pyspark.sql import Window

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.utils import calculate_elapsed_time,export_csv_file,insert_dataframe_into_database
from FUNCTION.AC_WISE_Calculation_function import main_processing_function as AC_Wise_calculation
from FUNCTION.PC_WISE_Calculation_function import main_processing_function as PC_Wise_calculation
from FUNCTION.SEQ_WISE_AC_calculation_function import main_processing_function as SEQ_Wise_calculation_AC
from FUNCTION.SEQ_WISE_PC_calculation_function import main_processing_function as SEQ_Wise_calculation_PC

driver_path = r"D:\DOWNLOAD\ojdbc11.jar"
start_time = time.time()
server = '192.168.0.27'
user = 'sa'
password = 'NXT@LKJHGFDSA'
database = 'voter'
port = '1433'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
url = f"jdbc:sqlserver://{server}:1433;databaseName={database};user={user};password={password}"
newTableName1=f"CH_TEST_DATA_BC_AC_WISE"
newTableName2=f"CH_TEST_DATA_BC_PC_WISE"
newTableName3=f"CH_TEST_DATA_BC_SEQ_WISE_AC"
newTableName4=f"CH_TEST_DATA_BC_SEQ_WISE_PC"


final_table = "ALL_STATE_TEST_DATA_BC"



#---------------------------------------------------------------------------------------------------------------------#
spark = SparkSession.builder \
    .appName("ALL_STATE_Voter_Data_Analysis") \
    .config("spark.driver.extraClassPath", driver_path) \
    .config("spark.sql.shuffle.partitions", "8") \
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

statesBC = ['bc_ap', 'bc_asm', 'bc_br', 'bc_cg', 'bc_ch', 'bc_dl', 'bc_ga', 'bc_gj', 'bc_hp', 'bc_tn', 'bc_hr', 'bc_jh', 'bc_ka', 'bc_kl', 'bc_mh', 'bc_mp', 'bc_od', 'bc_pb', 'bc_rj', 'bc_tg', 'bc_uk', 'bc_up', 'bc_wb']
# statesBC = ['bc_od']

def import_queries(module_name, state_list):
    module = importlib.import_module(module_name)
    return {state: getattr(module, state) for state in state_list}

bc_queries = import_queries('Queries.queries_BC', statesBC)


#------------------------------------------------------------------------------------------------#
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
#------------------------------------------------------------------------------------------------#

bc_df = create_df_from_queries(bc_queries)
#------------------------------------------------------------------------------------------------#

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
final_bc_df = bc_df.join(
    df_sql.select("AC_ID", "REGION"),
    on=["AC_ID"],
    how="left"
)

print("Second query Execute completed ....")
print(" ")
#------------------------------------------------------------------------------------------------#

final_merged_df=final_bc_df

#---------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------#
print("BC Round AC_Wise data calculation start .....😊😊😊😊")

ac_final_dataframe = AC_Wise_calculation(final_merged_df)
ac_final_dataframe = ac_final_dataframe.withColumn('ROUND', lit('BC'))
# result1 = insert_dataframe_into_database(ac_final_dataframe, newTableName1,url, driver)
# print(result1)
print("BC Round AC_Wise data Execution completed .....😊😊😊😊")

print("BC Round PC_Wise data calculation start .....😊😊😊😊")
pc_final_dataframe = PC_Wise_calculation(final_merged_df)
# result2 = insert_dataframe_into_database(pc_final_dataframe, newTableName2,url, driver)
# print(result2)
print("BC Round PC_Wise data Execution completed .....😊😊😊😊")

print("BC Round SEQ_Wise_AC data calculation start .....😊😊😊😊")
seqAC_final_dataframe = SEQ_Wise_calculation_AC(final_merged_df)
# result3 = insert_dataframe_into_database(seqAC_final_dataframe, newTableName3,url, driver)
# print(result3)
print("BC Round SEQ_Wise_AC data Execution completed .....😊😊😊😊")

print("BC Round SEQ_Wise_PC data calculation start .....😊😊😊😊")
seqPC_final_dataframe = SEQ_Wise_calculation_PC(final_merged_df)
# result4 = insert_dataframe_into_database(seqPC_final_dataframe, newTableName4,url, driver)
# print(result4)
print("BC Round SEQ_Wise_PC data Execution completed .....😊😊😊😊")


# First, merge ac_final_dataframe and pc_final_dataframe
merged_df1 = ac_final_dataframe.join(pc_final_dataframe, ['AC_ID', 'PC_ID','AC_NO','PC_NO','N_PARTY','RES1','state','REGION'], how='left')

# Then, merge the result with seqAC_final_dataframe
merged_df2 = merged_df1.join(seqAC_final_dataframe, ['AC_ID', 'PC_ID','AC_NO','PC_NO','N_PARTY','RES1','state','REGION'], how='left')

# Finally, merge the result with seqPC_final_dataframe
final_merged_df = merged_df2.join(seqPC_final_dataframe, ['AC_ID', 'PC_ID','AC_NO','PC_NO','N_PARTY','RES1','state','REGION'], how='left')

desired_order = [
    'AC_ID', 'PC_ID', 'AC_NO', 'PC_NO', 'state', 'N_PARTY', 'RES1',
    'RES_ACWISE', 'AC_WISE_PERCENTAGE', 'MAX_PER_ACWISE', 'WINNER_ACWISE', '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE',
    'MARGIN_ACWISE', 'MARGIN_GROUP_ACWISE', 'SUMMERY_ACWISE',
    'RES_PCWISE', 'PC_WISE_PERCENTAGE', 'MAX_PER_PCWISE', 'WINNER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE',
    'MARGIN_PCWISE', 'MARGIN_GROUP_PCWISE', 'SUMMERY_PCWISE',
    'RES_SEQWISE_AC','SEQ_WISE_PERCENTAGE_AC', 'MAX_PER_SEQWISE_AC', 'WINNER_SEQWISE_AC', '2ND_PER_SEQWISE_AC',
    'RUNNER_UP_SEQWISE_AC','MARGIN_SEQWISE_AC','MARGIN_GROUP_SEQWISE_AC', 'SUMMERY_SEQWISE_AC',
    'RES_SEQWISE_PC','SEQ_WISE_PERCENTAGE_PC', 'MAX_PER_SEQWISE_PC', 'WINNER_SEQWISE_PC', '2ND_PER_SEQWISE_PC',
    'RUNNER_UP_SEQWISE_PC', 'MARGIN_SEQWISE_PC', 'MARGIN_GROUP_SEQWISE_PC', 'SUMMERY_SEQWISE_PC',
    'REGION', 'ROUND'
]


df_reordered = final_merged_df.select(*desired_order)
df_reordered.to_csv('desired_order.csv', index=False)

# res = insert_dataframe_into_database(df_reordered,url,final_table, driver)
# print(res)
# res=export_csv_file(df_reordered,final_table)
# print(res)



end_time = time.time()
elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
print(f"Total time taken to calculating the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")
# print('BC ROUND final Data', df.count())



# spark.stop()

