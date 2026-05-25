from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

MYSQL_URL = "jdbc:mysql://localhost:3306/support?serverTimezone=UTC"
MYSQL_USER = "root"
MYSQL_PASSWORD = "admin"   # mets ton vrai mdp

data = [(1, "test")]
df = spark.createDataFrame(data, ["id", "txt"])

df.write.format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "test_spark") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()

print("OK écrit dans MySQL")