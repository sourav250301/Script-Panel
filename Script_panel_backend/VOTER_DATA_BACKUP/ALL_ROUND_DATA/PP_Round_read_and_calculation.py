
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import lit,col
import time
import importlib
import sys
import os
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType, FloatType


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.utils import calculate_elapsed_time,export_csv_file,insert_dataframe_into_database
from FUNCTION.AC_WISE_Calculation_function import main_processing_function as AC_Wise_calculation
from FUNCTION.PC_WISE_Calculation_function import main_processing_function as PC_Wise_calculation
from FUNCTION.SEQ_WISE_AC_calculation_function import main_processing_function as SEQ_Wise_calculation_AC
from FUNCTION.SEQ_WISE_PC_calculation_function import main_processing_function as SEQ_Wise_calculation_PC

driver_path = r"D:\DOWNLOAD\ojdbc11.jar"
# start_time = time.time()
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
    .getOrCreate()

# statesPP = ['pp_ap', 'pp_asm', 'pp_br', 'pp_cg', 'pp_ch', 'pp_dl', 'pp_ga', 'pp_gj', 'pp_hp', 'pp_tn', 'pp_hr', 'pp_jh', 'pp_ka', 'pp_kl', 'pp_mh', 'pp_mp', 'pp_od', 'pp_pb', 'pp_rj', 'pp_tg', 'pp_uk', 'pp_up', 'pp_wb']
statesPP = ['pp_od','pp_ch']


def import_queries(module_name, state_list):
    module = importlib.import_module(module_name)
    return {state: getattr(module, state) for state in state_list}

pp_queries = import_queries('Queries.queries_PP', statesPP)


#------------------------------------------------------------------------------------------------#
print("PP Round calculation start .....😊😊😊😊")
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

pp_df = create_df_from_queries(pp_queries)
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
final_pp_df = pp_df.join(
    df_sql.select("AC_ID", "REGION"),
    on=["AC_ID"],
    how="left"
)

print("Second query Execute completed ....")
print(" ")
#------------------------------------------------------------------------------------------------#

final_merged_df=final_pp_df

#---------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------#
print("PP Round AC_Wise data calculation start .....")
ac_final_dataframe = AC_Wise_calculation(final_merged_df)
print("PP Round AC_Wise data Execution completed .....")

print("PP Round PC_Wise data calculation start .....")
pc_final_dataframe = PC_Wise_calculation(final_merged_df)
print("PP Round PC_Wise data Execution completed .....")

print("PP Round SEQ_Wise_AC data calculation start .....")
seqAC_final_dataframe = SEQ_Wise_calculation_AC(final_merged_df)
print("PP Round SEQ_Wise_AC data Execution completed .....")

print("PP Round SEQ_Wise_PC data calculation start .....")
seqPC_final_dataframe = SEQ_Wise_calculation_PC(final_merged_df)
print("PP Round SEQ_Wise_PC data Execution completed .....")


final_df_PP = ac_final_dataframe.join(pc_final_dataframe, on=['AC_ID', 'PC_ID'], how='left') \
                             .join(seqAC_final_dataframe, on=['AC_ID', 'PC_ID'], how='left') \
                             .join(seqPC_final_dataframe, on=['AC_ID', 'PC_ID'], how='left')

final_df_PP = final_df_PP.withColumn('ROUND', lit('PP'))

#
# end_time = time.time()
# elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
# print(f"Total time taken to calculating the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")
print("PP Round calculation completed .....😊😊😊😊")





