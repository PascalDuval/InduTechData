from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "localhost:19092"
TOPIC = "client_tickets"

spark = (
    SparkSession.builder
    .appName("ReadTickets")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# lecture Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

tickets = df.selectExpr("CAST(value AS STRING)")

# affichage console
query = (
    tickets.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()