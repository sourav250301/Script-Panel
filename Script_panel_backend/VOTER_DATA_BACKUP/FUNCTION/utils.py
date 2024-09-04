

def calculate_elapsed_time(start_time, end_time):
    elapsed_time_seconds = end_time - start_time
    minutes = int(elapsed_time_seconds // 60)
    seconds = elapsed_time_seconds % 60
    return elapsed_time_seconds, minutes, seconds


def insert_dataframe_into_database(dataframe,table_name,url,driver):
    print("start inserting in database ....")
    try:
        dataframe.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("driver", driver) \
            .mode("overwrite") \
            .save()

        return "DataFrame inserted successfully into table: " + table_name
    except Exception as e:
        return f"Error occurred while inserting data: {e}"

# def insert_dataframe_into_database(dataframe, table_name, url, driver, batch_size=1000):
#     from pyspark.sql import DataFrame
#
#     print("Start inserting into the database ...")
#
#     def write_partition(partition: DataFrame):
#         partition.write \
#             .format("jdbc") \
#             .option("url", url) \
#             .option("dbtable", table_name) \
#             .option("driver", driver) \
#             .option("batchsize", batch_size) \
#             .mode("append") \
#             .save()
#
#     try:
#         # Repartition DataFrame to increase parallelism
#         num_partitions = dataframe.rdd.getNumPartitions()
#         dataframe = dataframe.repartition(num_partitions)
#
#         # Use foreachPartition to write partitions individually
#         dataframe.foreachPartition(write_partition)
#
#         return "DataFrame inserted successfully into table: " + table_name
#     except Exception as e:
#         return f"Error occurred while inserting data: {e}"
#

# def insert_dataframe_into_database(dataframe, table_name, url, driver):
#     print("start inserting ....")
#     try:
#         # Get the total number of rows in the DataFrame
#         total_rows = dataframe.count()
#
#         # Define batch size
#         batch_size = 10000
#
#         # Number of batches
#         num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)
#
#         # Process each batch
#         for batch_num in range(num_batches):
#             start_row = batch_num * batch_size
#             end_row = min(start_row + batch_size, total_rows)
#
#             # Select the current batch
#             batch_df = dataframe.limit(end_row).subtract(dataframe.limit(start_row))
#
#             # Write the batch to the database
#             batch_df.write \
#                 .format("jdbc") \
#                 .option("url", url) \
#                 .option("dbtable", table_name) \
#                 .option("driver", driver) \
#                 .mode("overwrite") \
#                 .save()
#
#             # Print the status message
#             print(f"Inserted rows {start_row + 1} to {end_row} successfully into table: {table_name}")
#
#         return f"DataFrame with {total_rows} rows inserted successfully into table: {table_name}"
#     except Exception as e:
#         return f"Error occurred while inserting data: {e}"

def export_csv_file(dataframe, filename):
    try:
        dataframe.toPandas().to_csv(f"{filename}.csv", index=False)
        return "DataFrame inserted successfully in CSV file: " + filename
    except Exception as e:
        print("Error Occurred while exporting CSV file:", e)

