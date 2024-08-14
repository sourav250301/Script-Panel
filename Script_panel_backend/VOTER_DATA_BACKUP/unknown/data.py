
def calculate_pc_metrics(df):
    print(" ")
    print("PC wise calculation start....")

    window_pc_wise_total = Window.partitionBy("PC_ID")
    df = df.withColumn('RES_PCWISE_TOTAL', count('DN').over(window_pc_wise_total))

    df = df.withColumn('PC_WISE_PERCENTAGE', round((col('RES_PCWISE') / col('RES_PCWISE_TOTAL')) * 100, 2))

    window_max_pcwise = Window.partitionBy("PC_ID")
    df = df.withColumn('MAX_PER_PCWISE', max('PC_WISE_PERCENTAGE').over(window_max_pcwise))

    pcwise_grouped = df.groupBy(['PC_ID', 'RES1']).agg(
        count('DN').alias('RES_PCWISE_COUNT'),
        max(col('PC_WISE_PERCENTAGE')).alias('MAX_PER_PCWISE')
    )

    # Efficiently find max values using percentile_approx
    percentile_approx_window = Window.partitionBy('PC_ID').orderBy(col('RES_PCWISE_COUNT').desc())
    pcwise_max_counts = pcwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    # Identify draws and winners
    pcwise_max_counts = pcwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_PCWISE_COUNT') != col('RES_PCWISE_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_PCWISE',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(pcwise_max_counts[['PC_ID', 'WINNER_PCWISE']], on='PC_ID', how='left')

    # Determine the runner-up PC-wise
    df = df.withColumn('RANK_PER_PCWISE',
                       F.dense_rank().over(Window.partitionBy('PC_ID').orderBy(F.desc('PC_WISE_PERCENTAGE'))))
    second_max_df = df.filter(F.col('RANK_PER_PCWISE') == 2).select('PC_ID', 'RES1', 'PC_WISE_PERCENTAGE') \
        .withColumnRenamed('RES1', 'RUNNER_UP_PCWISE') \
        .withColumnRenamed('PC_WISE_PERCENTAGE', '2ND_PER_PCWISE')

    df = df.join(second_max_df, on='PC_ID', how='left')

    df = df.withColumn("MARGIN_PCWISE", F.round(F.col('MAX_PER_PCWISE') - F.col('2ND_PER_PCWISE'), 2))

    # Categorize margin using F.when
    df = df.withColumn(
        'MARGIN_GROUP_PCWISE',
        F.when((F.col('MARGIN_PCWISE') >= 0) & (F.col('MARGIN_PCWISE') <= 5.00), '0-5%')
        .when((F.col('MARGIN_PCWISE') > 5.00) & (F.col('MARGIN_PCWISE') <= 10.00), '6-10%')
        .when((F.col('MARGIN_PCWISE') > 10.00) & (F.col('MARGIN_PCWISE') <= 20.00), '11-20%')
        .when((F.col('MARGIN_PCWISE') > 20.00) & (F.col('MARGIN_PCWISE') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )


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
    print("PC wise calculation completed....")
    print(" ")
    return df

def calculate_ac_metrics(df):
    print(" ")
    print("AC wise calculation start....")

    window_ac_wise_total = Window.partitionBy("AC_ID")
    df = df.withColumn('RES_ACWISE_TOTAL', count('DN').over(window_ac_wise_total))

    df = df.withColumn('AC_WISE_PERCENTAGE', round((col('RES_ACWISE') / col('RES_ACWISE_TOTAL')) * 100, 2))

    window_max_acwise = Window.partitionBy("AC_ID")
    df = df.withColumn('MAX_PER_ACWISE', max('AC_WISE_PERCENTAGE').over(window_max_acwise))

    acwise_grouped = df.groupBy(['AC_ID', 'RES1']).agg(
        count('DN').alias('RES_ACWISE_COUNT'),
        max(col('AC_WISE_PERCENTAGE')).alias('MAX_PER_ACWISE')
    )

    # Efficiently find max values using percentile_approx
    percentile_approx_window = Window.partitionBy('AC_ID').orderBy(col('RES_ACWISE_COUNT').desc())
    acwise_max_counts = acwise_grouped.withColumn(
        'MAX_ROW_INDEX', row_number().over(percentile_approx_window)
    ).filter(col('MAX_ROW_INDEX') == 1).drop('MAX_ROW_INDEX')

    # Identify draws and winners
    acwise_max_counts = acwise_max_counts.withColumn(
        'DRAW',
        when(col('RES_ACWISE_COUNT') != col('RES_ACWISE_COUNT').cast('long'), lit(True)).otherwise(lit(False))
    ).withColumn(
        'WINNER_ACWISE',
        when(col('DRAW'), lit('DRAW')).otherwise(col('RES1'))
    )
    df = df.join(acwise_max_counts[['AC_ID', 'WINNER_ACWISE']], on='AC_ID', how='left')

    # Determine the runner-up AC-wise
    df = df.withColumn('RANK_PER_ACWISE',
                       F.dense_rank().over(Window.partitionBy('AC_ID').orderBy(F.desc('AC_WISE_PERCENTAGE'))))
    second_max_df = df.filter(F.col('RANK_PER_ACWISE') == 2).select('AC_ID', 'RES1', 'AC_WISE_PERCENTAGE') \
        .withColumnRenamed('RES1', 'RUNNER_UP_ACWISE') \
        .withColumnRenamed('AC_WISE_PERCENTAGE', '2ND_PER_ACWISE')

    df = df.join(second_max_df, on='AC_ID', how='left')

    df = df.withColumn("MARGIN_ACWISE", F.round(F.col('MAX_PER_ACWISE') - F.col('2ND_PER_ACWISE'), 2))

    # Categorize margin using F.when
    df = df.withColumn(
        'MARGIN_GROUP_ACWISE',
        F.when((F.col('MARGIN_ACWISE') >= 0) & (F.col('MARGIN_ACWISE') <= 5.00), '0-5%')
        .when((F.col('MARGIN_ACWISE') > 5.00) & (F.col('MARGIN_ACWISE') <= 10.00), '6-10%')
        .when((F.col('MARGIN_ACWISE') > 10.00) & (F.col('MARGIN_ACWISE') <= 20.00), '11-20%')
        .when((F.col('MARGIN_ACWISE') > 20.00) & (F.col('MARGIN_ACWISE') <= 30.00), '21-30%')
        .otherwise('Above 30%')
    )

    # Create SUMMERY_ACWISE column
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
    print("AC wise calculation completed....")
    print(" ")
    return df

