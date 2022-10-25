from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

flights_path = 'gs://globallogic-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations/flights.csv'
airlines_path = 'gs://globallogic-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations/airlines.csv'
airports_path = 'gs://globallogic-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations/airports.csv'
input_options = {'header': True}
output_path = 'gs://globallogic-procamp-bigdata-datasets/spark1_cancelled_flights_output/'

spark = (
    SparkSession
        .builder
        .appName('cancellation_rates')
        .getOrCreate()
    )

df_flights = spark.read.csv(flights_path, **input_options)
df_airlines = spark.read.csv(airlines_path, **input_options)
df_airports = spark.read.csv(airports_path, **input_options)


df_cancelled_flights_counted = (
    df_flights
        .groupBy('AIRLINE', 'ORIGIN_AIRPORT')
        .agg(
            sum('CANCELLED').cast('int').alias('CANCELLED_NUMBER'),
            count('CANCELLED').alias('TOTAL_NUMBER')
        )
)

df_cancelled_flights_all_info = (
	df_cancelled_flights_counted
		.select(
			'*',
			(col('TOTAL_NUMBER') - col('CANCELLED_NUMBER')).alias('PROCESSED_NUMBER'),
			(col('CANCELLED_NUMBER') * 100 / col('TOTAL_NUMBER')).alias('CANCELLED_PERCENTAGE')
		)
)

df_cancelled_flights_all_joined = (
	df_cancelled_flights_all_info
		.join(
			df_airlines,
			df_cancelled_flights_all_info.AIRLINE == df_airlines.IATA_CODE,
			'inner'
		)
		.join(
			df_airports,
			df_cancelled_flights_all_info.ORIGIN_AIRPORT == df_airports.IATA_CODE,
			'inner'
		)
		.select(df_airlines.AIRLINE, col('AIRPORT').alias('ORIGIN_AIRPORT'), 'CANCELLED_PERCENTAGE', 'CANCELLED_NUMBER', 'PROCESSED_NUMBER', 'TOTAL_NUMBER')
		.orderBy('AIRLINE', col('CANCELLED_PERCENTAGE').desc())
)

df_flights_per_airlines = (
	df_cancelled_flights_all_joined
		.groupBy('AIRLINE')
		.agg(sum('TOTAL_NUMBER').cast('int').alias('TOTAL_FLIGHTS'))
		.orderBy('AIRLINE')
)
df_flights_per_airlines.write.mode('overwrite').option('header', 'true').csv(output_path+'flights_per_airline/')

df_wako_final = (
	df_cancelled_flights_all_joined
	.drop('TOTAL_NUMBER')
	.filter(col('AIRPORT') == 'Waco Regional Airport')
)
df_wako_final.write.mode('overwrite').option('header', 'true').csv(output_path+'cancelled_wako/')

df_others_final = (
	df_cancelled_flights_all_joined
	.drop('TOTAL_NUMBER')
	.filter(col('AIRPORT') != 'Waco Regional Airport')
)
df_others_final.write.mode('overwrite').json(output_path+'cancelled_others/')
