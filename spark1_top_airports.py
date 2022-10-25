from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, sum

spark = (
    SparkSession
        .builder
        .appName('top_airports')
        .getOrCreate()
    )

file_path = 'gs://globallogic-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations/flights.csv'
input_options = {'header': True}
output_path = 'gs://globallogic-procamp-bigdata-datasets/spark1_top_airports_output/'

df = spark.read.csv(file_path, **input_options)

df_popular_airports_base = df.select('MONTH', 'DESTINATION_AIRPORT')
df_airport_visits_per_month = (
    df_popular_airports_base
        .groupBy('MONTH', 'DESTINATION_AIRPORT')
        .agg(count('*').alias('NUMBER_OF_VISITS'))
        .orderBy('MONTH', col('NUMBER_OF_VISITS').desc())
)
df_airport_visits_per_month.write.mode('overwrite').option('header', 'true').csv(output_path+'airport_visits_per_month')

df_top_visit_per_month = (
    df_airport_visits_per_month
        .select('*')
        .groupBy('MONTH')
        .agg(max('NUMBER_OF_VISITS').alias('MAX_NUMBER_OF_VISITS'))
        .orderBy('MONTH')
)
df_top_airports = (
    df_top_visit_per_month
        .join(
            df_airport_visits_per_month,
            (df_top_visit_per_month.MONTH == df_airport_visits_per_month.MONTH) & (col('MAX_NUMBER_OF_VISITS') == col('NUMBER_OF_VISITS')),
            'inner'
        )
        .select(df_top_visit_per_month.MONTH, 'DESTINATION_AIRPORT', 'MAX_NUMBER_OF_VISITS')
        .orderBy('MONTH')
)
df_top_airports.write.mode('overwrite').option('delimiter', '\t').option('header', 'true').csv(output_path + 'top_airport_per_month')
