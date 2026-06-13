import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "pipeline", local: bool = False) -> SparkSession:
    master = "local[*]" if local else os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.sql.adaptive.enabled", os.getenv("ENABLE_AQE", "true"))
        .config("spark.sql.autoBroadcastJoinThreshold", "10m" if os.getenv("ENABLE_BROADCAST_JOINS", "true") == "true" else "-1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "200")
    )

    packages = [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
    builder = builder.config("spark.jars.packages", ",".join(packages))

    return builder.getOrCreate()
