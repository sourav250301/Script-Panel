
from pyspark.sql import SparkSession, DataFrame, functions as F
# from pyspark.sql.functions import lit,col
import time
import importlib
import sys
import os
from pyspark.sql import Window
from pyspark.sql.functions import col, count, to_date, round, sum as spark_sum, max, row_number, when, lit, concat


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from FUNCTION.utils import calculate_elapsed_time,export_csv_file,insert_dataframe_into_database
# from FUNCTION.AC_WISE_Calculation_function import main_processing_function as AC_Wise_calculation

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



#---------------------------------------------------------------------------------------------------------------------#
spark = SparkSession.builder \
    .appName("ALL_STATE_Voter_Data_Analysis") \
    .config("spark.driver.extraClassPath", driver_path) \
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


def calculate_responses(df):
    print(" ")
    print('Start calculating responses ....')

    window_acwise = Window.partitionBy('AC_ID', 'RES1')
    df = df.withColumn('RES_ACWISE', count('DN').over(window_acwise))
    print("Response calculation completed....")
    print(" ")
    return df


def calculate_percentage(df):
    print(" ")
    print("percentage calculation start....")

    window_ac_wise_total = Window.partitionBy("AC_ID")
    df = df.withColumn('RES_ACWISE_TOTAL', count('DN').over(window_ac_wise_total))
    df = df.withColumn('AC_WISE_PERCENTAGE', round((col('RES_ACWISE') / col('RES_ACWISE_TOTAL')) * 100, 2))

    print("percentage calculation completed....")
    return df


def calculate_max_percentage(df):
    print(" ")
    print("max-percentage calculation start....")

    window_max_acwise = Window.partitionBy("AC_ID")
    df = df.withColumn('MAX_PER_ACWISE', max('AC_WISE_PERCENTAGE').over(window_max_acwise))

    print("max-percentage calculation completed....")
    return df


def calculate_winner(df):
    print(" ")
    print("start calculating Winner ....")

    # ac wise winner calculation section
    acwise_grouped = df.groupBy(['AC_ID', 'RES1']).agg(
        count('DN').alias('RES_ACWISE_COUNT'),
        max(col('AC_WISE_PERCENTAGE')).alias('MAX_PER_ACWISE')
    )

    percentile_approx_window = Window.partitionBy('AC_ID').orderBy(col('RES_ACWISE_COUNT').desc())
    acwise_max_counts = acwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    acwise_max_counts = acwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_ACWISE_COUNT') != col('RES_ACWISE_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_ACWISE',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(acwise_max_counts[['AC_ID', 'WINNER_ACWISE']], on='AC_ID', how='left')

    print("completed calculating runner's up....")
    return  df


def calculate_runner_up(df):
    print(" ")
    print("start calculating runner-up ....")

    ##AC_WISE Runner-up adn 2nd max calculation section
    runner_up_grouped_ac_wise = df.groupBy(['AC_ID', 'RES1']).agg(
        F.count('DN').alias('RES_ACWISE_COUNT'),
        F.max(F.col('AC_WISE_PERCENTAGE')).alias('MAX_PER_ACWISE')
    )
    runner_up_window_ac_wise = Window.partitionBy('AC_ID').orderBy(F.col('MAX_PER_ACWISE').desc(),
                                                                   F.col('RES_ACWISE_COUNT').desc())
    runner_up_counts_ac_wise = runner_up_grouped_ac_wise.withColumn(
        'ROW_INDEX', F.row_number().over(runner_up_window_ac_wise)
    ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
    runner_up_counts_ac_wise = runner_up_counts_ac_wise.withColumnRenamed('RES1', 'RUNNER_UP_ACWISE').withColumnRenamed(
        'MAX_PER_ACWISE', '2ND_PER_ACWISE')
    df = df.join(runner_up_counts_ac_wise[['AC_ID', 'RUNNER_UP_ACWISE', '2ND_PER_ACWISE']], on='AC_ID', how='left')

    print("completed calculating runner-up....")
    return df


# def calculate_ac_wise_runner_up(df):
#     # Print current columns
#     column_names = df.columns
#     print("Columns in calculate_ac_wise_runner_up:", column_names)
#
#     # Define the window specification
#     window_spec = Window.partitionBy('AC_ID').orderBy(col('AC_WISE_PERCENTAGE').desc())
#
#     # Use dense_rank to handle ties correctly
#     df = df.withColumn('RANK_PER_ACWISE', F.dense_rank().over(window_spec))
#
#     # Filter for the second highest AC_WISE_PERCENTAGE
#     second_highest_df = df.filter(col('RANK_PER_ACWISE') == 2)
#
#     # Print columns in second_highest_df to verify
#     second_highest_df_columns = second_highest_df.columns
#     print("Columns in second_highest_df:", second_highest_df_columns)
#
#     # Rename columns to avoid ambiguity
#     second_highest_df = second_highest_df.withColumnRenamed('AC_WISE_PERCENTAGE', 'SECOND_MAX_AC_WISE_PERCENTAGE') \
#         .withColumnRenamed('RES1', 'RUNNER_UP_AC_WISE')
#
#     # Perform the join with unique column names
#     df = df.join(
#         second_highest_df.select('AC_ID', 'SECOND_MAX_AC_WISE_PERCENTAGE', 'RUNNER_UP_AC_WISE'),
#         on='AC_ID',
#         how='left'
#     ).withColumnRenamed('SECOND_MAX_AC_WISE_PERCENTAGE', '2ND_PER_ACWISE') \
#         .withColumnRenamed('RUNNER_UP_AC_WISE', 'RUNNER_UP_ACWISE')
#
#     df.show(10)
#     return df
# def calculate_pc_wise_runner_up(df):
#     # Print current columns
#     column_names = df.columns
#     print("Columns in calculate_pc_wise_runner_up:", column_names)
#
#     # Define the window specification
#     window_spec = Window.partitionBy('PC_ID').orderBy(col('PC_WISE_PERCENTAGE').desc())
#
#     # Use dense_rank to handle ties correctly
#     df = df.withColumn('RANK_PER_PCWISE', F.dense_rank().over(window_spec))
#
#     # Filter for the second highest AC_WISE_PERCENTAGE
#     second_highest_df = df.filter(col('RANK_PER_PCWISE') == 2)
#
#     # Print columns in second_highest_df to verify
#     second_highest_df_columns = second_highest_df.columns
#     print("Columns in second_highest_df:", second_highest_df_columns)
#
#     # Rename columns to avoid ambiguity
#     second_highest_df = second_highest_df.withColumnRenamed('PC_WISE_PERCENTAGE', 'SECOND_MAX_PC_WISE_PERCENTAGE') \
#         .withColumnRenamed('RES1', 'RUNNER_UP_PC_WISE')
#
#     # Perform the join with unique column names
#     df = df.join(
#         second_highest_df.select('PC_ID', 'SECOND_MAX_PC_WISE_PERCENTAGE', 'RUNNER_UP_PC_WISE'),
#         on='PC_ID',
#         how='left'
#     ).withColumnRenamed('SECOND_MAX_PC_WISE_PERCENTAGE', '2ND_PER_PCWISE') \
#         .withColumnRenamed('RUNNER_UP_PC_WISE', 'RUNNER_UP_PCWISE')
#     df.show(10)
#
#     return df
# def runner_up(df):
#     # Define the window specification
#     window_spec = Window.partitionBy('AC_ID').orderBy(col('AC_WISE_PERCENTAGE').desc())
#
#     # Use dense_rank to handle ties correctly
#     df = df.withColumn('RANK_PER_ACWISE', F.dense_rank().over(window_spec))
#
#     # Filter for the second highest AC_WISE_PERCENTAGE
#     second_highest_df = df.filter(col('RANK_PER_ACWISE') == 2)
#
#     # Print columns in second_highest_df to verify
#     second_highest_df_columns = second_highest_df.columns
#     print("Columns in second_highest_df:", second_highest_df_columns)
#
#     # Rename columns to avoid ambiguity
#     second_highest_df = second_highest_df.withColumnRenamed('AC_WISE_PERCENTAGE', 'SECOND_MAX_AC_WISE_PERCENTAGE') \
#         .withColumnRenamed('RES1', 'RUNNER_UP_AC_WISE')
#
#     # Perform the join with unique column names
#     df = df.join(
#         second_highest_df.select('AC_ID', 'SECOND_MAX_AC_WISE_PERCENTAGE', 'RUNNER_UP_AC_WISE'),
#         on='AC_ID',
#         how='left'
#     ).withColumnRenamed('SECOND_MAX_AC_WISE_PERCENTAGE', '2ND_PER_ACWISE') \
#         .withColumnRenamed('RUNNER_UP_AC_WISE', 'RUNNER_UP_ACWISE')
#
#     # ------------------------------------------------------------------------------------------------------------------#
#     # Define the window specification
#     window_spec_pc = Window.partitionBy('PC_ID').orderBy(col('PC_WISE_PERCENTAGE').desc())
#
#     # Use dense_rank to handle ties correctly
#     df = df.withColumn('RANK_PER_PCWISE', F.dense_rank().over(window_spec_pc))
#
#     # Filter for the second highest AC_WISE_PERCENTAGE
#     second_highest_df_pc = df.filter(col('RANK_PER_PCWISE') == 2)
#
#     # Print columns in second_highest_df to verify
#     second_highest_df_columns = second_highest_df_pc.columns
#     print("Columns in second_highest_df:", second_highest_df_columns)
#
#     # Rename columns to avoid ambiguity
#     second_highest_df_pc = second_highest_df_pc.withColumnRenamed('PC_WISE_PERCENTAGE', 'SECOND_MAX_PC_WISE_PERCENTAGE') \
#         .withColumnRenamed('RES1', 'RUNNER_UP_PC_WISE')
#
#     # Perform the join with unique column names
#     df = df.join(
#         second_highest_df_pc.select('PC_ID', 'SECOND_MAX_PC_WISE_PERCENTAGE', 'RUNNER_UP_PC_WISE'),
#         on='PC_ID',
#         how='left'
#     ).withColumnRenamed('SECOND_MAX_PC_WISE_PERCENTAGE', '2ND_PER_PCWISE') \
#         .withColumnRenamed('RUNNER_UP_PC_WISE', 'RUNNER_UP_PCWISE')
#     df.show(10)
#
#     return df


def calculate_margin_and_group(df):
    print(" ")
    print("start calculating Margin ....")

    #AC wise margin calculating section
    df = df.withColumn("MARGIN_ACWISE", F.round(F.col('MAX_PER_ACWISE') - F.col('2ND_PER_ACWISE'), 2))
    df = df.withColumn(
        'MARGIN_GROUP_ACWISE',
        F.when((F.col('MARGIN_ACWISE') >= 0) & (F.col('MARGIN_ACWISE') <= 5.00), '0-5%')
        .when((F.col('MARGIN_ACWISE') > 5.00) & (F.col('MARGIN_ACWISE') <= 10.00), '6-10%')
        .when((F.col('MARGIN_ACWISE') > 10.00) & (F.col('MARGIN_ACWISE') <= 20.00), '11-20%')
        .when((F.col('MARGIN_ACWISE') > 20.00) & (F.col('MARGIN_ACWISE') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

    print("completed calculating Margin ....")
    return df


def calculate_summery(df):
    print(" ")
    print("Summery calclation start....")

    #Ac wise summery calculation section
    df = df.withColumn(
        'SUMMERY_ACWISE',
        F.when(F.col('WINNER_ACWISE') == 'DRAW', 'DRAW')
        .otherwise(
            concat(
                F.col('WINNER_ACWISE'),
                lit(' '),
                F.col('MARGIN_ACWISE').cast('string'),
                lit(' ('),
                F.col('RUNNER_UP_ACWISE'),
                lit(')')
            )
        )
    )


    print("Summery calclation completed....")
    return df

def reorder_columns(df):
    desired_order = [
        'RES_ACWISE','AC_WISE_PERCENTAGE', 'MAX_PER_ACWISE', 'WINNER_ACWISE', '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE', 'MARGIN_ACWISE',
        'MARGIN_GROUP_ACWISE', 'SUMMERY_ACWISE',
    ]

    # Get the current columns in the DataFrame
    current_columns = df.columns

    # Columns to keep in the order specified, ensuring they are in the DataFrame
    ordered_columns = [col for col in desired_order if col in current_columns]

    # Remaining columns that are not in the desired order
    remaining_columns = [col for col in current_columns if col not in ordered_columns]

    # Combine the remaining columns with the ordered columns
    new_order = remaining_columns + ordered_columns

    # Select the columns in the new order
    df = df.select(*new_order)

    return df

def clean_data(df):
    print(" ")
    print("clean start")
    columns_to_drop = ['DN', 'PART_NO', 'Start_Time', 'DTMF_REP', 'RES2', 'CASTE', 'GENDER', 'AGE', 'RES_ACWISE_TOTAL',
                       'RANK_PER_ACWISE']
    df = df.drop(*columns_to_drop)
    df = df.dropDuplicates(subset=['AC_ID', 'PC_ID', 'N_PARTY', 'RES1'])

    print("clean completed")
    return  df




















ac_final_dataframe = calculate_responses(final_merged_df)
ac_final_dataframe=calculate_percentage(ac_final_dataframe)
ac_final_dataframe=calculate_max_percentage(ac_final_dataframe)
ac_final_dataframe=calculate_winner(ac_final_dataframe)
ac_final_dataframe=calculate_runner_up(ac_final_dataframe)
ac_final_dataframe=calculate_margin_and_group(ac_final_dataframe)
ac_final_dataframe=calculate_summery(ac_final_dataframe)
ac_final_dataframe=clean_data(ac_final_dataframe)
ac_final_dataframe=reorder_columns(ac_final_dataframe)
ac_final_dataframe = ac_final_dataframe.withColumn('ROUND', lit('BC'))

print("converted start")
pandas_datframe=ac_final_dataframe.toPandas()
print("converted completed")










# result1 = insert_dataframe_into_database(ac_final_dataframe, newTableName1,url, driver)
# print(result1)
print("BC Round AC_Wise data Execution completed .....😊😊😊😊")



end_time = time.time()
elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
print(f"Total time taken to calculating the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")
# print('BC ROUND final Data', df.count())



# spark.stop()

