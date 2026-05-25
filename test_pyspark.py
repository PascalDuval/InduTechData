from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestPySpark")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .getOrCreate()
)

# Créer un DataFrame simple
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Afficher le DataFrame
df.show()

# Fermer la session Spark  
spark.stop()