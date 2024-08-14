from pyspark.sql import Window
from pyspark.sql.functions import col, count, to_date, round, sum as spark_sum, max, row_number, when, lit, concat
from pyspark.sql import functions as F
import time

from FUNCTION.utils import calculate_elapsed_time

start_time = time.time()


def calculate_responses(df):
    print(" ")
    print('Start calculating responses ....')

    window_acwise = Window.partitionBy('AC_ID', 'RES1')
    df = df.withColumn('RES_ACWISE', count('DN').over(window_acwise))

    window_pcwise = Window.partitionBy('PC_ID', 'RES1')
    df = df.withColumn('RES_PCWISE', count('DN').over(window_pcwise))

    window_seqwise_ac = Window.partitionBy('AC_ID', 'N_PARTY', 'RES1')
    df = df.withColumn('RES_SEQWISE_AC', count('DN').over(window_seqwise_ac))

    window_seqwise_pc = Window.partitionBy('PC_ID', 'N_PARTY', 'RES1')
    df = df.withColumn('RES_SEQWISE_PC', count('DN').over(window_seqwise_pc))
    #
    # if 'Start_Time' in df.columns:
    #     df = df.withColumn('Start_Time', to_date(col('Start_Time'), 'dd-MM-yyyy'))
    #     window_datewise = Window.partitionBy('AC_ID', 'Start_Time', 'RES1')
    #     df = df.withColumn('RES_DATEWISE', count('DN').over(window_datewise))
    #
    # window_regionwise = Window.partitionBy('AC_ID', 'REGION', 'RES1')
    # df = df.withColumn('RES_REGIONWISE_AC', count('DN').over(window_regionwise))

    print("Response calculation completed....")

    print(" ")
    return df


def calculate_percentage(df):
    print(" ")
    print("percentage calculation start....")

    window_pc_wise_total = Window.partitionBy("PC_ID")
    df = df.withColumn('RES_PCWISE_TOTAL', count('DN').over(window_pc_wise_total))
    window_ac_wise_total = Window.partitionBy("AC_ID")
    df = df.withColumn('RES_ACWISE_TOTAL', count('DN').over(window_ac_wise_total))
    window_seq_wise_ac_total = Window.partitionBy("AC_ID", "N_PARTY")
    df = df.withColumn('RES_SEQWISE_TOTAL', count('DN').over(window_seq_wise_ac_total))
    window_seq_wise_pc_total = Window.partitionBy("PC_ID", "N_PARTY")
    df = df.withColumn('RES_SEQWISE_TOTAL_PC', count('DN').over(window_seq_wise_pc_total))

    df = df.withColumn('AC_WISE_PERCENTAGE', round((col('RES_ACWISE') / col('RES_ACWISE_TOTAL')) * 100, 2))
    df = df.withColumn('PC_WISE_PERCENTAGE', round((col('RES_PCWISE') / col('RES_PCWISE_TOTAL')) * 100, 2))
    df = df.withColumn('SEQ_WISE_PERCENTAGE_AC', round((col('RES_SEQWISE_AC') / col('RES_SEQWISE_TOTAL')) * 100, 2))
    df = df.withColumn('SEQ_WISE_PERCENTAGE_PC', round((col('RES_SEQWISE_PC') / col('RES_SEQWISE_TOTAL_PC')) * 100, 2))

    print("percentage calculation completed....")
    return df


def calculate_max_percentage(df):
    print(" ")
    print("max-percentage calculation start....")

    window_max_acwise = Window.partitionBy("AC_ID")
    df = df.withColumn('MAX_PER_ACWISE', max('AC_WISE_PERCENTAGE').over(window_max_acwise))

    window_max_pcwise = Window.partitionBy("PC_ID")
    df = df.withColumn('MAX_PER_PCWISE', max('PC_WISE_PERCENTAGE').over(window_max_pcwise))

    window_max_seqwise_ac = Window.partitionBy("AC_ID", "N_PARTY")
    df = df.withColumn('MAX_PER_SEQWISE_AC', max('SEQ_WISE_PERCENTAGE_AC').over(window_max_seqwise_ac))

    window_max_seqwise_pc = Window.partitionBy("PC_ID", "N_PARTY")
    df = df.withColumn('MAX_PER_SEQWISE_PC', max('SEQ_WISE_PERCENTAGE_PC').over(window_max_seqwise_pc))

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

    # pc wise winner calculation section
    pcwise_grouped = df.groupBy(['PC_ID', 'RES1']).agg(
        count('DN').alias('RES_PCWISE_COUNT'),
        max(col('PC_WISE_PERCENTAGE')).alias('MAX_PER_PCWISE')
    )

    percentile_approx_window = Window.partitionBy('PC_ID').orderBy(col('RES_PCWISE_COUNT').desc())
    pcwise_max_counts = pcwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    pcwise_max_counts = pcwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_PCWISE_COUNT') != col('RES_PCWISE_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_PCWISE',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(pcwise_max_counts[['PC_ID', 'WINNER_PCWISE']], on='PC_ID', how='left')

    # seq-wise ac based winner calculation section
    seq_acwise_grouped = df.groupBy(['AC_ID', 'N_PARTY', 'RES1']).agg(
        count('DN').alias('RES_SEQWISE_AC_COUNT'),
    )

    percentile_approx_window = Window.partitionBy('AC_ID', 'N_PARTY').orderBy(col('RES_SEQWISE_AC_COUNT').desc())
    seq_acwise_max_counts = seq_acwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    seq_acwise_max_counts = seq_acwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_SEQWISE_AC_COUNT') != col('RES_SEQWISE_AC_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_SEQWISE_AC',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(seq_acwise_max_counts[['AC_ID', 'N_PARTY', 'WINNER_SEQWISE_AC']], on=['AC_ID', 'N_PARTY'], how='left')

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

#
# def calculate_runner_up_ac(df):
#     runner_up_grouped = df.groupBy(['AC_ID', 'RES1']).agg(
#         F.count('DN').alias('RES_ACWISE_COUNT'),
#         F.max(F.col('AC_WISE_PERCENTAGE')).alias('MAX_PER_ACWISE')
#     )
#
#     runner_up_window = Window.partitionBy('AC_ID').orderBy(F.col('MAX_PER_ACWISE').desc(),
#                                                            F.col('RES_ACWISE_COUNT').desc())
#     runner_up_counts = runner_up_grouped.withColumn(
#         'ROW_INDEX', F.row_number().over(runner_up_window)
#     ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
#
#     runner_up_counts = runner_up_counts.withColumnRenamed('RES1', 'RUNNER_UP_ACWISE').withColumnRenamed(
#         'MAX_PER_ACWISE', '2ND_PER_ACWISE')
#
#     df = df.join(runner_up_counts[['AC_ID', 'RUNNER_UP_ACWISE', '2ND_PER_ACWISE']], on='AC_ID', how='left')
#
#     return df
#
# def calculate_runner_up_pc(df):
#     runner_up_grouped = df.groupBy(['PC_ID', 'RES1']).agg(
#         F.count('DN').alias('RES_PCWISE_COUNT'),
#         F.max(F.col('PC_WISE_PERCENTAGE')).alias('MAX_PER_PCWISE')
#     )
#
#     runner_up_window = Window.partitionBy('PC_ID').orderBy(F.col('MAX_PER_PCWISE').desc(),
#                                                            F.col('RES_PCWISE_COUNT').desc())
#     runner_up_counts = runner_up_grouped.withColumn(
#         'ROW_INDEX', F.row_number().over(runner_up_window)
#     ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
#
#     runner_up_counts = runner_up_counts.withColumnRenamed('RES1', 'RUNNER_UP_PCWISE').withColumnRenamed(
#         'MAX_PER_PCWISE', '2ND_PER_PCWISE')
#
#     df = df.join(runner_up_counts[['PC_ID', 'RUNNER_UP_PCWISE', '2ND_PER_PCWISE']], on='PC_ID', how='left')
#
#     return df
#
# def calculate_runner_up_seqac(df):
#     runner_up_grouped_seq_wise_ac = df.groupBy(['AC_ID', 'N_PARTY', 'RES1']).agg(
#         F.count('DN').alias('RES_SEQWISE_AC_COUNT'),
#         F.max(F.col('SEQ_WISE_PERCENTAGE_AC')).alias('MAX_PER_SEQWISE_AC')
#     )
#     runner_up_window_seq_wise_ac = Window.partitionBy('AC_ID', 'N_PARTY').orderBy(F.col('MAX_PER_SEQWISE_AC').desc(),
#                                                                                   F.col('RES_SEQWISE_AC_COUNT').desc())
#     runner_up_counts_seq_wise_ac = runner_up_grouped_seq_wise_ac.withColumn(
#         'ROW_INDEX', F.row_number().over(runner_up_window_seq_wise_ac)
#     ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
#     runner_up_counts_seq_wise_ac = runner_up_counts_seq_wise_ac.withColumnRenamed('RES1',
#                                                                                   'RUNNER_UP_SEQWISE_AC').withColumnRenamed(
#         'MAX_PER_SEQWISE_AC', '2ND_PER_SEQWISE_AC')
#     df = df.join(runner_up_counts_seq_wise_ac[['AC_ID', 'N_PARTY', 'RUNNER_UP_SEQWISE_AC', '2ND_PER_SEQWISE_AC']],
#                  on=['AC_ID', 'N_PARTY'], how='left')
#     return df
#
# def calculate_runner_up_seqpc(df):
#     runner_up_grouped_seq_wise_pc = df.groupBy(['PC_ID', 'N_PARTY', 'RES1']).agg(
#         F.count('DN').alias('RES_SEQWISE_PC_COUNT'),
#         F.max(F.col('SEQ_WISE_PERCENTAGE_PC')).alias('MAX_PER_SEQWISE_PC')
#     )
#     runner_up_window_seq_wise_pc = Window.partitionBy('PC_ID', 'N_PARTY').orderBy(F.col('MAX_PER_SEQWISE_PC').desc(),
#                                                                                   F.col('RES_SEQWISE_PC_COUNT').desc())
#     runner_up_counts_seq_wise_pc = runner_up_grouped_seq_wise_pc.withColumn(
#         'ROW_INDEX', F.row_number().over(runner_up_window_seq_wise_pc)
#     ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
#     runner_up_counts_seq_wise_pc = runner_up_counts_seq_wise_pc.withColumnRenamed('RES1',
#                                                                                   'RUNNER_UP_SEQWISE_PC').withColumnRenamed(
#         'MAX_PER_SEQWISE_PC', '2ND_PER_SEQWISE_PC')
#     df = df.join(runner_up_counts_seq_wise_pc[['PC_ID', 'N_PARTY', 'RUNNER_UP_SEQWISE_PC', '2ND_PER_SEQWISE_PC']],
#                  on=['PC_ID', 'N_PARTY'], how='left')
#
#     return df
#
#


def calculate_runner_up(df):
    print("start calculation runner-up and 2nd Max....")

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


    ##PC_WISE Runner-up adn 2nd max calculation section
    runner_up_grouped_pc_wise = df.groupBy(['PC_ID', 'RES1']).agg(
        F.count('DN').alias('RES_PCWISE_COUNT'),
        F.max(F.col('PC_WISE_PERCENTAGE')).alias('MAX_PER_PCWISE')
    )
    runner_up_window_pc_wise = Window.partitionBy('PC_ID').orderBy(F.col('MAX_PER_PCWISE').desc(),
                                                                   F.col('RES_PCWISE_COUNT').desc())
    runner_up_counts_pc_wise = runner_up_grouped_pc_wise.withColumn(
        'ROW_INDEX', F.row_number().over(runner_up_window_pc_wise)
    ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
    runner_up_counts_pc_wise = runner_up_counts_pc_wise.withColumnRenamed('RES1', 'RUNNER_UP_PCWISE').withColumnRenamed(
        'MAX_PER_PCWISE', '2ND_PER_PCWISE')
    df = df.join(runner_up_counts_pc_wise[['PC_ID', 'RUNNER_UP_PCWISE', '2ND_PER_PCWISE']], on='PC_ID', how='left')


    ##SEQ_WISE_AC Runner-up adn 2nd max calculation section
    runner_up_grouped_seq_wise_ac = df.groupBy(['AC_ID','N_PARTY','RES1']).agg(
        F.count('DN').alias('RES_SEQWISE_AC_COUNT'),
        F.max(F.col('SEQ_WISE_PERCENTAGE_AC')).alias('MAX_PER_SEQWISE_AC')
    )
    runner_up_window_seq_wise_ac = Window.partitionBy('AC_ID','N_PARTY').orderBy(F.col('MAX_PER_SEQWISE_AC').desc(),
                                                                   F.col('RES_SEQWISE_AC_COUNT').desc())
    runner_up_counts_seq_wise_ac = runner_up_grouped_seq_wise_ac.withColumn(
        'ROW_INDEX', F.row_number().over(runner_up_window_seq_wise_ac)
    ).filter(F.col('ROW_INDEX') == 2).drop('ROW_INDEX')
    runner_up_counts_seq_wise_ac = runner_up_counts_seq_wise_ac.withColumnRenamed('RES1', 'RUNNER_UP_SEQWISE_AC').withColumnRenamed(
        'MAX_PER_SEQWISE_AC', '2ND_PER_SEQWISE_AC')
    df = df.join(runner_up_counts_seq_wise_ac[['AC_ID','N_PARTY','RUNNER_UP_SEQWISE_AC', '2ND_PER_SEQWISE_AC']], on=['AC_ID','N_PARTY'], how='left')


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

    # AC wise margin calculating section
    df = df.withColumn("MARGIN_ACWISE", F.round(F.col('MAX_PER_ACWISE') - F.col('2ND_PER_ACWISE'), 2))
    df = df.withColumn(
        'MARGIN_GROUP_ACWISE',
        F.when((F.col('MARGIN_ACWISE') >= 0) & (F.col('MARGIN_ACWISE') <= 5.00), '0-5%')
        .when((F.col('MARGIN_ACWISE') > 5.00) & (F.col('MARGIN_ACWISE') <= 10.00), '6-10%')
        .when((F.col('MARGIN_ACWISE') > 10.00) & (F.col('MARGIN_ACWISE') <= 20.00), '11-20%')
        .when((F.col('MARGIN_ACWISE') > 20.00) & (F.col('MARGIN_ACWISE') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

    # PC wise margin calculating section
    df = df.withColumn("MARGIN_PCWISE", F.round(F.col('MAX_PER_PCWISE') - F.col('2ND_PER_PCWISE'), 2))
    df = df.withColumn(
        'MARGIN_GROUP_PCWISE',
        F.when((F.col('MARGIN_PCWISE') >= 0) & (F.col('MARGIN_PCWISE') <= 5.00), '0-5%')
        .when((F.col('MARGIN_PCWISE') > 5.00) & (F.col('MARGIN_PCWISE') <= 10.00), '6-10%')
        .when((F.col('MARGIN_PCWISE') > 10.00) & (F.col('MARGIN_PCWISE') <= 20.00), '11-20%')
        .when((F.col('MARGIN_PCWISE') > 20.00) & (F.col('MARGIN_PCWISE') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

    # SEQ_WISE AC wise margin calculating section
    df = df.withColumn("MARGIN_SEQWISE_AC", F.round(F.col('MAX_PER_SEQWISE_AC') - F.col('2ND_PER_SEQWISE_AC'), 2))
    df = df.withColumn(
        'MARGIN_GROUP_SEQWISE_AC',
        F.when((F.col('MARGIN_SEQWISE_AC') >= 0) & (F.col('MARGIN_SEQWISE_AC') <= 5.00), '0-5%')
        .when((F.col('MARGIN_SEQWISE_AC') > 5.00) & (F.col('MARGIN_SEQWISE_AC') <= 10.00), '6-10%')
        .when((F.col('MARGIN_SEQWISE_AC') > 10.00) & (F.col('MARGIN_SEQWISE_AC') <= 20.00), '11-20%')
        .when((F.col('MARGIN_SEQWISE_AC') > 20.00) & (F.col('MARGIN_SEQWISE_AC') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

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

    # Ac wise summery calculation section
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

    # Pc wise summery calculation section
    df = df.withColumn(
        'SUMMERY_PCWISE',
        F.when(F.col('WINNER_PCWISE') == 'DRAW', 'DRAW')
        .otherwise(
            concat(
                F.col('WINNER_PCWISE'),
                lit(' '),
                F.col('MARGIN_PCWISE').cast('string'),
                lit(' ('),
                F.col('RUNNER_UP_PCWISE'),
                lit(')')
            )
        )
    )

    # seq_wise Ac wise summery calculation section
    df = df.withColumn(
        'SUMMERY_SEQWISE_AC',
        F.when(F.col('WINNER_SEQWISE_AC') == 'DRAW', 'DRAW')
        .otherwise(
            concat(
                F.col('WINNER_SEQWISE_AC'),
                lit(' '),
                F.col('MARGIN_SEQWISE_AC').cast('string'),
                lit(' ('),
                F.col('RUNNER_UP_SEQWISE_AC'),
                lit(')')
            )
        )
    )

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

    end_time = time.time()
    elapsed_time_seconds, minutes, seconds = calculate_elapsed_time(start_time, end_time)
    print(
        f"Total time taken to calculating the Data: {elapsed_time_seconds:.2f} seconds ({minutes} minutes {seconds:.2f} seconds)")
    print("Summery calclation completed....")
    return df


def reorder_columns(df):
    desired_order = [
        'AC_WISE_PERCENTAGE', 'MAX_PER_ACWISE', 'WINNER_ACWISE', '2ND_PER_ACWISE', 'RUNNER_UP_ACWISE', 'MARGIN_ACWISE',
        'MARGIN_GROUP_ACWISE', 'SUMMERY_ACWISE',
        'PC_WISE_PERCENTAGE', 'MAX_PER_PCWISE', 'WINNER_PCWISE', '2ND_PER_PCWISE', 'RUNNER_UP_PCWISE', 'MARGIN_PCWISE',
        'MARGIN_GROUP_PCWISE', 'SUMMERY_PCWISE',
        'SEQ_WISE_PERCENTAGE_AC', 'MAX_PER_SEQWISE_AC', 'WINNER_SEQWISE_AC', '2ND_PER_SEQWISE_AC',
        'RUNNER_UP_SEQWISE_AC',
        'MARGIN_SEQWISE_AC', 'MARGIN_GROUP_SEQWISE_AC', 'SUMMERY_SEQWISE_AC',
        'SEQ_WISE_PERCENTAGE_PC', 'MAX_PER_SEQWISE_PC', 'WINNER_SEQWISE_PC', '2ND_PER_SEQWISE_PC',
        'RUNNER_UP_SEQWISE_PC',
        'MARGIN_SEQWISE_PC', 'MARGIN_GROUP_SEQWISE_PC', 'SUMMERY_SEQWISE_PC',
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

    # df = calculate_runner_up_pc(df)
    # df = calculate_runner_up_ac(df)
    # df = calculate_runner_up_seqac(df)
    # df = calculate_runner_up_seqpc(df)

    df = calculate_runner_up(df)
    df = calculate_margin_and_group(df)
    df = calculate_summery(df)
    df = reorder_columns(df)
    df = clean_data(df)
    # df=print_data(df)
    return df

# --------------------------------------------------------------------------------------------------------- #
