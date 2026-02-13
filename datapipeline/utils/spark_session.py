from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys


def get_spark_session(app_name="SA-News"):

    python_exec = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        .config("spark.local.dir", "C:/spark-temp/local")
        .config("spark.sql.warehouse.dir", "C:/spark-temp/warehouse")

        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")

        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.LocalFileSystem"
        )
        .config(
            "spark.hadoop.fs.file.impl.disable.cache",
            "true"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark