import dateparser
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import UserDefinedFunction, when, month, hour, year, minute, \
    second
from pyspark.sql.types import TimestampType, DateType

spark = SparkSession.builder.master(
    "spark://Dis-iMac-Pro:7077").appName("apc").getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

pathname = '/Users/abhishek/Vanderbilt/TransitHub - General/datasets/carta-occupancy/CARTA-raw-DATA/CARTA APC 2021'
newfilename = 'RIDECHECK_DATA_MAY 2021.TXT'
df = sqlContext.read.csv(f'{pathname}/{newfilename}', inferSchema=True, header=True).dropDuplicates(
    ['TRIP_KEY', 'SURVEY_DATE', 'ROUTE_NUMBER', 'DIRECTION_NAME', 'STOP_ID', 'SORT_ORDER'])


def convertdate(x):
    try:
        out = dateparser.parse(str(x).split(" ")[0])
        return out
    except:
        return None


def convertstamp(x):
    try:
        out = dateparser.parse(str(x).split(" ")[1])
        return out
    except:
        return None


converttimeudf = UserDefinedFunction(
    lambda x: convertstamp(x), TimestampType())
convertdateudf = UserDefinedFunction(lambda x: convertdate(x), TimestampType())

df3 = df.withColumn("TIME_SCHEDULED",
                    converttimeudf(df['TIME_SCHEDULED'])). \
    withColumn("TRIP_START_TIME", converttimeudf(df['TRIP_START_TIME'])). \
    withColumn("TIME_ACTUAL_ARRIVE", converttimeudf(df['TIME_ACTUAL_ARRIVE'])). \
    withColumn("TIME_ACTUAL_DEPART", converttimeudf(df['TIME_ACTUAL_DEPART'])). \
    withColumn("SURVEY_DATE", convertdateudf(df["SURVEY_DATE"]).cast(DateType()))

df = df3.withColumn("MONTH", month(df3["SURVEY_DATE"])). \
    withColumn("YEAR", year(df3["SURVEY_DATE"])). \
    withColumn("TIME_SCHEDULED_HOUR", hour(df3["TIME_SCHEDULED"])). \
    withColumn("TIME_SCHEDULED_MIN", minute(df3["TIME_SCHEDULED"])). \
    withColumn("TIME_SCHEDULED_SEC", second(df3["TIME_SCHEDULED"])). \
    withColumn("TRIP_START_TIME_HOUR", hour(df3["TRIP_START_TIME"])). \
    withColumn("TRIP_START_TIME_MIN", minute(df3["TRIP_START_TIME"])). \
    withColumn("TRIP_START_TIME_SEC", second(df3["TRIP_START_TIME"])). \
    withColumn("TIME_ACTUAL_ARRIVE_HOUR", hour(df3["TIME_ACTUAL_ARRIVE"])). \
    withColumn("TIME_ACTUAL_ARRIVE_MIN", minute(df3["TIME_ACTUAL_ARRIVE"])). \
    withColumn("TIME_ACTUAL_ARRIVE_SEC", second(df3["TIME_ACTUAL_ARRIVE"])). \
    withColumn("TIME_ACTUAL_DEPART_HOUR", hour(df3["TIME_ACTUAL_DEPART"])). \
    withColumn("TIME_ACTUAL_DEPART_MIN", minute(df3["TIME_ACTUAL_DEPART"])). \
    withColumn("TIME_ACTUAL_DEPART_SEC", second(df3["TIME_ACTUAL_DEPART"]))

df = df.withColumn('DIRECTION_NAME',
                   when(df.DIRECTION_NAME == "OUTYBOUND", "OUTBOUND").when(df.DIRECTION_NAME == "0", "OUTBOUND").when(
                       df.DIRECTION_NAME == "1", "INBOUND").otherwise(df.DIRECTION_NAME))

df = df.withColumnRenamed('trip_key', 'trip_id')

df = df.select([F.col(x).alias(x.lower()) for x in df.columns])

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# df.limit(2)


columns_to_drop = ['condition_number', 'odom_end', 'division_name', 'garage_name', 'checker_time', 'signup_name',
                   'comments', 'comment_number', 'non_student_fare', 'checker_name', 'timepoint',
                   'signup_name', 'schedule_id', 'odom_start', 'odom_end', 'schedule_name', 'time_period', 'kneels',
                   'revenue_start', 'nr_board', 'nr_alight', 'revenue_end', 'running_time_actual', 'dwell_time']

df = df.drop(*columns_to_drop)

df.write.option("mapreduce.fileoutputcommitter.algorithm.version", "2").partitionBy(
    "year", "month").mode("append").format("parquet").save("/Users/abhishek/spark/carta/apcdata")
