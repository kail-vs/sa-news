from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="SA-News"):

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        .config("spark.local.dir", "C:/spark-temp/local")
        .config("spark.sql.warehouse.dir", "C:/spark-temp/warehouse")

        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")

        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark
