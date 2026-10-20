from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("PaymentTypeAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/dados/nyc_tripdata_2024_sample_1M.csv",
                    header=True, inferSchema=True)

# Questões 1, 2 e 3 - Corridas, receita total e tarifa média por tipo de pagamento
resultado = df.groupBy("payment_type").agg(
    F.count("*").alias("qtd_corridas"),
    F.sum("total_amount").alias("receita_total"),
    F.avg("fare_amount").alias("tarifa_media"),
).orderBy("qtd_corridas", ascending=False)


# Salva o resultado no HDFS do cluster (1 arquivo CSV, sobrescrevendo execuções anteriores)
resultado.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("hdfs://namenode:9000/resultados/payment_type")

spark.stop()
