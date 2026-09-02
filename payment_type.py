from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PaymentTypeAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/dados/nyc_taxi_trip_2024_p1_sample.csv", header=True)

# Questão 1 - Corridas por tipo de pagamento
df.groupBy("payment_type").count().orderBy("count", ascending=False).show()

# Questão 2 - Receita total por tipo de pagamento
df.groupBy("payment_type").sum("total_amount").show()

# Questão 3 - Tarifa média por tipo de pagamento
df.groupBy("payment_type").avg("fare_amount").show()

spark.stop()