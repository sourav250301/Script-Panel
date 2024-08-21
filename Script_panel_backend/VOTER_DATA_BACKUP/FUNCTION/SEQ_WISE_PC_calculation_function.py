from pyspark.sql import Window
from pyspark.sql.functions import col, count, to_date, round, sum as spark_sum, max, row_number, when, lit, concat
from pyspark.sql import functions as F

def calculate_responses(df):
    print(" ")
    print('Start calculating responses ....')

    window_seqwise_pc = Window.partitionBy('PC_ID', 'N_PARTY', 'RES1')
    df = df.withColumn('RES_SEQWISE_PC', count('DN').over(window_seqwise_pc))

    print("Response calculation completed....")
    return df


def calculate_percentage(df):
    print(" ")
    print("percentage calculation start....")

    window_seq_wise_pc_total = Window.partitionBy("PC_ID", "N_PARTY")
    df = df.withColumn('RES_SEQWISE_TOTAL_PC', count('DN').over(window_seq_wise_pc_total))
    df = df.withColumn('SEQ_WISE_PERCENTAGE_PC', round((col('RES_SEQWISE_PC') / col('RES_SEQWISE_TOTAL_PC')) * 100, 2))

    print("percentage calculation completed....")
    return df


def calculate_max_percentage(df):
    print(" ")
    print("max-percentage calculation start....")

    window_max_seqwise_pc = Window.partitionBy("PC_ID", "N_PARTY")
    df = df.withColumn('MAX_PER_SEQWISE_PC', max('SEQ_WISE_PERCENTAGE_PC').over(window_max_seqwise_pc))

    print("max-percentage calculation completed....")
    return df


def calculate_winner(df):
    print(" ")
    print("start calculating Winner ....")

    # seq-wise PC based winner calculation section
    seq_pcwise_grouped = df.groupBy(['PC_ID', 'N_PARTY', 'RES1']).agg(
        count('DN').alias('RES_SEQWISE_PC_COUNT'),
    )

    percentile_approx_window = Window.partitionBy('PC_ID', 'N_PARTY').orderBy(col('RES_SEQWISE_PC_COUNT').desc())
    seq_pcwise_max_counts = seq_pcwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    seq_pcwise_max_counts = seq_pcwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_SEQWISE_PC_COUNT') != col('RES_SEQWISE_PC_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_SEQWISE_PC',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(seq_pcwise_max_counts[['PC_ID', 'N_PARTY', 'WINNER_SEQWISE_PC']], on=['PC_ID', 'N_PARTY'], how='left')

    print("winner calculation completed ....")
    return df


def calculate_runner_up(df):
    print("start calculation runner-up and 2nd Max....")

    ##SEQ_WISE_PC Runner-up adn 2nd max calculation section
    runner_up_grouped_seq_wise_pc = df.groupBy(['PC_ID','N_PARTY','RES1']).agg(
        F.count('DN').alias('RES_SEQWISE_PC_COUNT'),
        F.max(F.col('SEQ_WISE_PERCENTAGE_PC')).alias('MAX_PER_SEQWISE_PC')
    )
    runner_up_window_seq_wise_pc = Window.partitionBy('PC_ID','N_PARTY').orderBy(F.col('MAX_PER_SEQWISE_PC').desc(),
                                                                   F.col('RES_SEQWISE_PC_COUNT').desc())
    runner_up_counts_seq_wise_pc = runner_up_grouped_seq_wise_pc.withColumn(
        'ROW_INDEX', F.row_number().over(runner_up_window_seq_wise_pc)
    ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
    runner_up_counts_seq_wise_pc = runner_up_counts_seq_wise_pc.withColumnRenamed('RES1', 'RUNNER_UP_SEQWISE_PC').withColumnRenamed(
        'MAX_PER_SEQWISE_PC', '2ND_PER_SEQWISE_PC')
    df = df.join(runner_up_counts_seq_wise_pc[['PC_ID','N_PARTY','RUNNER_UP_SEQWISE_PC', '2ND_PER_SEQWISE_PC']], on=['PC_ID','N_PARTY'], how='left')
    print("calculation runner-up and 2nd Max completed....")
    return df



def calculate_margin_and_group(df):
    print(" ")
    print("start calculating Margin ....")

    # SEQ_WISE PC wise margin calculating section
    df = df.withColumn("MARGIN_SEQWISE_PC", F.round(F.col('MAX_PER_SEQWISE_PC') - F.col('2ND_PER_SEQWISE_PC'), 2))
    df = df.withColumn(
        'MARGIN_GROUP_SEQWISE_PC',
        F.when((F.col('MARGIN_SEQWISE_PC') >= 0) & (F.col('MARGIN_SEQWISE_PC') <= 5.00), '0-5%')
        .when((F.col('MARGIN_SEQWISE_PC') > 5.00) & (F.col('MARGIN_SEQWISE_PC') <= 10.00), '6-10%')
        .when((F.col('MARGIN_SEQWISE_PC') > 10.00) & (F.col('MARGIN_SEQWISE_PC') <= 20.00), '11-20%')
        .when((F.col('MARGIN_SEQWISE_PC') > 20.00) & (F.col('MARGIN_SEQWISE_PC') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

    print("completed calculating Margin ....")
    return df


def calculate_summery(df):
    print(" ")
    print("Summery calclation start....")

    # seq_wise Pc wise summery calculation section
    df = df.withColumn(
        'SUMMERY_SEQWISE_PC',
        F.when(F.col('WINNER_SEQWISE_PC') == 'DRAW', 'DRAW')
        .otherwise(
            concat(
                F.col('WINNER_SEQWISE_PC'),
                lit(' '),
                F.col('MARGIN_SEQWISE_PC').cast('string'),
                lit(' ('),
                F.col('RUNNER_UP_SEQWISE_PC'),
                lit(')')
            )
        )
    )

    print("Summery calclation completed....")
    return df


def reorder_columns(df):
    desired_order = [
        'SEQ_WISE_PERCENTAGE_PC', 'MAX_PER_SEQWISE_PC', 'WINNER_SEQWISE_PC', '2ND_PER_SEQWISE_PC',
        'RUNNER_UP_SEQWISE_PC','MARGIN_SEQWISE_PC', 'MARGIN_GROUP_SEQWISE_PC', 'SUMMERY_SEQWISE_PC',
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
                       'RES_PCWISE_TOTAL', 'RES_SEQWISE_TOTAL', 'RES_SEQWISE_TOTAL_PC']

    df = df.drop(*columns_to_drop)
    df = df.dropDuplicates(subset=['AC_ID', 'PC_ID', 'N_PARTY', 'RES1'])
    # print("After clean no of rows in dataframe is : ",df.count())
    print("clean completed")
    return df


# def print_data(df):
#     print("👇👇👇👇👇👇 Final BC Round Data 👇👇👇👇👇👇")
#     filtered_df = df.filter(col('AC_ID') == 'OD-AC-10').limit(10)
#     filtered_df.show(truncate=False)
#     return  df

def main_processing_function(df):
    df = calculate_responses(df)
    df = calculate_percentage(df)
    df = calculate_max_percentage(df)
    df = calculate_winner(df)
    df = calculate_runner_up(df)
    df = calculate_margin_and_group(df)
    df = calculate_summery(df)
    df = reorder_columns(df)
    df = clean_data(df)
    # df=print_data(df)
    return df

# --------------------------------------------------------------------------------------------------------- #
