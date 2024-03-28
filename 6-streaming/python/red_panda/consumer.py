import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

schema = types.StructType(
    [
        types.StructField("lpep_pickup_datetime", types.StringType(), True),
        types.StructField("lpep_dropoff_datetime", types.StringType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("passenger_count", types.DoubleType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
    ]
)

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = (
    SparkSession.builder.master("local[*]")
    .appName("GreenTripsConsumer")
    .config("spark.jars.packages", kafka_jar_package)
    .getOrCreate()
)

green_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "green-trips")
    .option("startingOffsets", "earliest")
    .load()
)


green_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "green-trips")
    .option("startingOffsets", "earliest")
    .option("checkpointLocation", "checkpoint")
    .load()
)

green_stream = green_stream.select(
    F.from_json(F.col("value").cast("STRING"), schema).alias("data")
).select("data.*")


def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])


query = green_stream.writeStream.foreachBatch(peek).start()

df_green_stream = green_stream.withColumn("timestamp", F.current_timestamp())
df_windowed_aggregation = df_green_stream.groupBy(
    F.window(timeColumn=df_green_stream.timestamp, windowDuration="5 minutes"),
    df_green_stream.DOLocationID,
).count()
popular_destinations = df_windowed_aggregation.orderBy("count", ascending=False)


query = (
    popular_destinations.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
