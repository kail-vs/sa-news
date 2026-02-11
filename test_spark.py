from datapipeline.utils.spark_session import get_spark_session

spark = get_spark_session("Test")

df = spark.range(5)
df.show()
